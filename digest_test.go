// Copyright 2020 Gregory Petrosyan <gregory.petrosyan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bdigest_test

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"pgregory.net/bdigest"
	"pgregory.net/rapid"
)

var (
	generatorNames = []string{"uniform", "normal", "log_normal", "pareto"}
	generators     = map[string]func(*rapid.T, float64, float64) generator{
		"uniform":    newUniformGen,
		"normal":     newNormalGen,
		"log_normal": newLogNormalGen,
		"pareto":     newParetoGen,
	}
)

type generator interface {
	Seed(int64)
	Gen() float64
}

type uniformGen struct {
	*rand.Rand
	min float64
	max float64
}

func newUniformGen(t *rapid.T, min float64, max float64) generator {
	minR := rapid.Float64Range(min, max).Draw(t, "min param").(float64)
	maxR := rapid.Float64Range(minR, max).Draw(t, "max param").(float64)

	return &uniformGen{
		Rand: rand.New(rand.NewSource(0)),
		min:  minR,
		max:  maxR,
	}
}

type normalGen struct {
	*rand.Rand
	stddev float64
	mean   float64
}

func newNormalGen(t *rapid.T, min float64, max float64) generator {
	mean := rapid.Float64Range(min, max).Draw(t, "mean param").(float64)
	stddev := rapid.Float64Range(0, (max-min)/2).Draw(t, "stddev param").(float64)

	return &normalGen{
		Rand:   rand.New(rand.NewSource(0)),
		mean:   mean,
		stddev: stddev,
	}
}

type logNormalGen struct {
	*rand.Rand
	mu    float64
	sigma float64
}

func newLogNormalGen(t *rapid.T, min float64, max float64) generator {
	mean := rapid.Float64Range(min, max).Draw(t, "mean param").(float64)
	stddev := rapid.Float64Range(0, (max-min)/2).Draw(t, "stddev param").(float64)

	return &logNormalGen{
		Rand:  rand.New(rand.NewSource(0)),
		mu:    math.Log(mean),
		sigma: math.Log(stddev),
	}
}

type paretoGen struct {
	*rand.Rand
	min   float64
	index float64
}

func newParetoGen(t *rapid.T, min float64, max float64) generator {
	m := rapid.Float64Range(min, max).Draw(t, "min param").(float64)
	index := rapid.Float64Range(0.1, 10).Draw(t, "index param").(float64)

	return &paretoGen{
		Rand:  rand.New(rand.NewSource(0)),
		min:   m,
		index: index,
	}
}

func (g *uniformGen) Gen() float64   { return g.min + g.Float64()*(g.max-g.min) }
func (g *normalGen) Gen() float64    { return g.mean + g.NormFloat64()*g.stddev }
func (g *logNormalGen) Gen() float64 { return math.Exp(g.mu + g.NormFloat64()*g.sigma) }
func (g *paretoGen) Gen() float64    { return g.min * math.Exp(g.ExpFloat64()/g.index) }

type digest interface {
	Sum() float64
	Count() uint64
	Merge(digest)
	Add(float64)
	Quantile(float64) float64
}

type approxDigest struct {
	*bdigest.Digest
}

func (d *approxDigest) Merge(v digest) {
	err := d.Digest.Merge(v.(*approxDigest).Digest)
	if err != nil {
		panic(err)
	}
}

type perfectDigest struct {
	min    float64
	max    float64
	values []float64
	sorted bool
}

func (d *perfectDigest) Sum() float64 {
	if !d.sorted {
		sort.Float64s(d.values)
		d.sorted = true
	}

	s := 0.0
	for _, v := range d.values {
		s += v
	}
	return s
}

func (d *perfectDigest) Count() uint64 {
	return uint64(len(d.values))
}

func (d *perfectDigest) Merge(v digest) {
	p := v.(*perfectDigest)
	if d.sorted && p.sorted {
		values := make([]float64, len(d.values)+len(p.values))

		i, j, k := 0, 0, 0
		for i < len(d.values) && j < len(p.values) {
			if d.values[i] <= p.values[j] {
				values[k] = d.values[i]
				i++
			} else {
				values[k] = p.values[j]
				j++
			}
			k++
		}
		if i < len(d.values) {
			copy(values[k:], d.values[i:])
		}
		if j < len(p.values) {
			copy(values[k:], p.values[j:])
		}

		d.values = values
	} else {
		d.values = append(d.values, p.values...)
		d.sorted = false
	}
}

func (d *perfectDigest) Add(v float64) {
	if v < d.min {
		v = d.min
	} else if v > d.max {
		v = d.max
	}

	d.values = append(d.values, v)
	d.sorted = false
}

func (d *perfectDigest) Quantile(q float64) float64 {
	if d.Count() == 0 {
		return math.NaN()
	}

	if !d.sorted {
		sort.Float64s(d.values)
		d.sorted = true
	}

	return d.values[int(q*float64(d.Count()-1))]
}

func TestDigest(t *testing.T) {
	t.Parallel()

	rapid.Check(t, rapid.Run(&digestMachine{}))
}

type digestPair struct {
	d digest
	r digest
}

type digestMachine struct {
	min     float64
	max     float64
	err     float64
	digests []digestPair
}

func (m *digestMachine) Init(t *rapid.T) {
	minVal := 1e-10
	maxVal := 1e20
	minErr := 1e-5
	if testing.Short() {
		minVal = 1e-5
		maxVal = 1e10
		minErr = 1e-2
	}

	m.min = rapid.Float64Range(minVal, 1-1e-10).Draw(t, "digest min").(float64)
	m.max = rapid.Float64Range(1+1e-10, maxVal).Draw(t, "digest max").(float64)
	m.err = rapid.Float64Range(minErr, 1-1e-5).Draw(t, "relative error").(float64)
}

func (m *digestMachine) Check(_ *rapid.T) {}

func (m *digestMachine) AddDigest(t *rapid.T) {
	maxSize := 100000
	if testing.Short() {
		maxSize = 1000
	}

	gen := rapid.SampledFrom(generatorNames).Draw(t, "generator").(string)
	seed := rapid.Int64().Draw(t, "seed").(int64)
	size := rapid.IntRange(0, maxSize).Draw(t, "size").(int)

	d := &approxDigest{bdigest.NewDigest(m.min, m.max, m.err)}
	r := &perfectDigest{min: m.min, max: m.max, values: make([]float64, 0, size)}
	t.Logf("using %v/%v for %v:", gen, size, d.Digest)

	g := generators[gen](t, m.min, m.max)
	g.Seed(seed)
	for i := 0; i < size; i++ {
		f := math.Abs(g.Gen())
		t.Logf("adding %v", f)
		d.Add(f)
		r.Add(f)
	}

	q := rapid.Float64Range(0, 1).Draw(t, "quantile").(float64)
	checkDigest(t, d, r, q, m.err)

	m.digests = append(m.digests, digestPair{d, r})
}

func (m *digestMachine) MergeDigests(t *rapid.T) {
	if len(m.digests) < 1 {
		return
	}

	to := rapid.SampledFrom(m.digests).Draw(t, "digest to").(digestPair)
	from := rapid.SampledFrom(m.digests).Draw(t, "digest from").(digestPair)

	to.d.Merge(from.d)
	to.r.Merge(from.r)

	q := rapid.Float64Range(0, 1).Draw(t, "quantile").(float64)
	checkDigest(t, to.d, to.r, q, m.err)
}

func checkDigest(t *rapid.T, d digest, r digest, q float64, err float64) {
	t.Helper()

	ds := d.Sum()
	rs := r.Sum()
	if math.Abs(ds-rs)/rs > 1e-6 {
		t.Errorf("sum is %v instead of %v", ds, rs)
	}

	dc := d.Count()
	rc := r.Count()
	if dc != rc {
		t.Errorf("count is %v instead of %v", dc, rc)
	}

	dq := d.Quantile(q)
	rq := r.Quantile(q)
	re := math.Abs(dq-rq) / rq
	maxErr := err
	if q == 0 || q == 1 {
		maxErr = 0
	}
	if re > maxErr && (re-maxErr)/maxErr > 1e-9 {
		t.Errorf("q%v error is %v%% instead of max %v%% (%v instead of %v)", q, re*100, maxErr*100, dq, rq)
	}
}
