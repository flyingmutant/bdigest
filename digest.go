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

package bdigest

import (
	"fmt"
	"math"
)

// TODO: track max non-empty indices for faster quantiles and merging
// TODO: expose size information (number of buckets)

// Digest tracks distribution of values using histograms
// with exponentially sized buckets.
type Digest struct {
	params

	min float64
	max float64
	sum float64
	c   float64

	neg    []uint64
	pos    []uint64
	numNeg uint64
	numPos uint64
}

type params struct {
	minVal  float64
	maxVal  float64
	alpha   float64
	gamma   float64
	gammaLn float64
}

func (p *params) bucketKey(x float64) int {
	logGammaX := math.Log(x) / p.gammaLn
	return int(math.Ceil(logGammaX))
}

func (p *params) quantile(k int) float64 {
	powGammaK := math.Exp(float64(k) * p.gammaLn)
	return 2 * powGammaK / (p.gamma + 1)
}

// NewDefaultDigest returns digest suitable for values
// between 1e-6 and 1e6, which covers the range
// between 1 microsecond and 11.5 days (measured in seconds) or
// 1 byte and 1 terabyte / 0.9 tebibytes (measured in megabytes).
func NewDefaultDigest(err float64) *Digest {
	return NewDigest(1e-6, 1e6, err)
}

// NewDigest returns digest suitable for calculating quantiles
// of values between min ∈ (0, 1) and max ∈ (1, math.MaxFloat64)
// with maximum relative error err ∈ (0, 1).
//
// For the given range, size of digest is inversely proportional
// to the relative error. That is, digest with 2% relative error
// is twice as small as digest with 1% relative error.
func NewDigest(min float64, max float64, err float64) *Digest {
	if math.IsNaN(min) || min <= 0 || min >= 1 {
		panic("min must be in (0, 1)")
	}
	if math.IsNaN(max) || max <= 1 || max >= math.MaxFloat64 {
		panic("max must be in (1, math.MaxFloat64)")
	}
	if math.IsNaN(err) || err <= 0 || err >= 1 {
		panic("err must be in (0, 1)")
	}

	p := params{
		minVal:  min,
		maxVal:  max,
		alpha:   err,
		gamma:   1 + 2*err/(1-err),
		gammaLn: math.Log1p(2 * err / (1 - err)),
	}

	return &Digest{
		params: p,
		min:    math.Inf(1),
		max:    math.Inf(-1),
		neg:    make([]uint64, -p.bucketKey(p.minVal)+1),
		pos:    make([]uint64, p.bucketKey(p.maxVal)+1),
	}
}

func (d *Digest) String() string {
	return fmt.Sprintf("Digest(err=%v%%, min=%v, max=%v)", d.alpha*100, d.minVal, d.maxVal)
}

// Sum returns the sum of added values.
func (d *Digest) Sum() float64 {
	return d.sum
}

// Count returns the number of added values.
func (d *Digest) Count() uint64 {
	return d.numPos + d.numNeg
}

// Merge merges the content of v into the digest.
// Merge preserves relative error guarantees of Quantile.
//
// Merge returns an error if digests have different parameters.
func (d *Digest) Merge(v *Digest) error {
	if v.params != d.params {
		return fmt.Errorf("can not merge b-digest with params %+v into one with %+v", v.params, d.params)
	}

	d.addKahan(v.sum)
	if v.min < d.min {
		d.min = v.min
	}
	if v.max > d.max {
		d.max = v.max
	}

	for i, n := range v.neg {
		d.neg[i] += n
	}
	for i, n := range v.pos {
		d.pos[i] += n
	}
	d.numNeg += v.numNeg
	d.numPos += v.numPos

	return nil
}

// Add adds non-negative value v to the digest.
// If v is outside of the digest range, v is set to the
// minimum/maximum value of the range.
//
// Add panics if v is negative or NaN.
func (d *Digest) Add(v float64) {
	if math.IsNaN(v) || v < 0 {
		panic("v must be non-negative")
	}

	if v < d.minVal {
		v = d.minVal
	} else if v > d.maxVal {
		v = d.maxVal
	}

	if v < d.min {
		d.min = v
	}
	if v > d.max {
		d.max = v
	}
	d.addKahan(v)

	k := d.bucketKey(v)
	if k >= 1 {
		d.pos[k]++
		d.numPos++
	} else {
		d.neg[-k]++
		d.numNeg++
	}
}

// Quantile returns the q-quantile of added values.
// Minimum (0-quantile) and maximum (1-quantile) are exact,
// other quantiles have maximum relative error of err.
//
// Quantile panics if q is outside [0, 1].
// Quantile returns NaN for empty digest.
func (d *Digest) Quantile(q float64) float64 {
	if math.IsNaN(q) || q < 0 || q > 1 {
		panic("q must be in [0, 1]")
	}

	if d.Count() == 0 {
		return math.NaN()
	}

	if q == 0 {
		return d.min
	} else if q == 1 {
		return d.max
	}

	rank := uint64(1 + q*float64(d.Count()-1))
	if rank <= d.numNeg {
		i := rankIndexRev(rank, d.neg)
		return d.quantile(-i)
	} else {
		i := rankIndex(rank-d.numNeg, d.pos)
		return d.quantile(i)
	}
}

func (d *Digest) addKahan(v float64) {
	y := v - d.c
	t := d.sum + y
	d.c = (t - d.sum) - y
	d.sum = t
}

func rankIndexRev(rank uint64, buckets []uint64) int {
	n := uint64(0)
	for i := len(buckets) - 1; i >= 0; i-- {
		n += buckets[i]
		if n >= rank {
			return i
		}
	}
	return 0
}

func rankIndex(rank uint64, buckets []uint64) int {
	n := uint64(0)
	for i, b := range buckets {
		n += b
		if n >= rank {
			return i
		}
	}
	return len(buckets) - 1
}
