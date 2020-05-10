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

// Package bdigest provides tools for fast and memory-efficient estimation
// of quantiles with guaranteed relative error and full mergeability.
package bdigest

import (
	"fmt"
	"math"
	"sort"
)

// TODO: track max non-empty indices for faster quantiles and merging?
// TODO: expose size information (number of buckets)

// Digest tracks distribution of values using histograms
// with exponentially sized buckets.
type Digest struct {
	params

	min float64
	max float64
	sum float64
	c   float64

	buckets []uint64
	offset  int
	count   uint64
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
		params:  p,
		min:     math.Inf(1),
		max:     math.Inf(-1),
		buckets: make([]uint64, 1-p.bucketKey(p.minVal)+p.bucketKey(p.maxVal)),
		offset:  1 - p.bucketKey(p.minVal),
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
	return d.count
}

// Merge merges the content of v into the digest.
// Merge preserves relative error guarantees of Quantile.
//
// Merge returns an error if digests have different parameters.
func (d *Digest) Merge(v *Digest) error {
	if v.params != d.params {
		return fmt.Errorf("can not merge b-digest with params %+v into one with %+v", v.params, d.params)
	}

	if v.min < d.min {
		d.min = v.min
	}
	if v.max > d.max {
		d.max = v.max
	}
	d.addKahan(v.sum)

	for i, n := range v.buckets {
		d.buckets[i] += n
	}
	d.count += v.count

	return nil
}

// AddXFast adds non-negative value v to the digest.
// If v is outside of the digest range, v is set to the
// minimum/maximum value of the range.
//
// AddXFast panics if v is negative or NaN.
func AddXFast(d *Digest, v float64) {
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
	//n := len(d.buckets)
	for i := k + d.offset - 1; i < len(d.buckets); i++ {
		d.buckets[i]++
	}
	//d.buckets[k+d.offset-1]++
	d.count++
}

// AddXFast adds non-negative value v to the digest.
// If v is outside of the digest range, v is set to the
// minimum/maximum value of the range.
//
// AddXFast panics if v is negative or NaN.
func AddXSlow(d *Digest, v float64) {
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
	n := len(d.buckets)
	for i := k + d.offset - 1; i < n; i++ {
		d.buckets[i]++
	}
	//d.buckets[k+d.offset-1]++
	d.count++
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

	if d.count == 0 {
		return math.NaN()
	}

	if q == 0 {
		return d.min
	} else if q == 1 {
		return d.max
	}

	rank := uint64(1 + q*float64(d.count-1))
	i := rankIndex(rank, d.buckets)
	return d.quantile(i - d.offset + 1)
}

func (d *Digest) addKahan(v float64) {
	y := v - d.c
	t := d.sum + y
	d.c = (t - d.sum) - y
	d.sum = t
}

func rankIndex(rank uint64, buckets []uint64) int {
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, len(buckets)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		if buckets[h] < rank {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i
	return sort.Search(len(buckets), func(i int) bool {
		return buckets[i] >= rank
	})
	//n := uint64(0)
	//for i, b := range buckets {
	//	n += b
	//	if n >= rank {
	//		return i
	//	}
	//}
	//return len(buckets) - 1
}
