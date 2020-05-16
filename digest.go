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
	"encoding/binary"
	"fmt"
	"math"
)

const (
	headerSize = 8 /* alpha */ + 2*4 /* len(neg), len(pos) */
)

// Digest tracks distribution of values using histograms
// with exponentially sized buckets.
type Digest struct {
	alpha   float64
	gamma   float64
	gammaLn float64
	neg     []uint64
	pos     []uint64
	numNeg  uint64
	numPos  uint64
}

// NewDigest returns digest suitable for calculating quantiles
// of finite positive values with maximum relative error err ∈ (0, 1).
//
// Size of digest is proportional to the logarithm of minimum
// and maximum of the values added.
// Size of digest is inversely proportional to the relative error.
// That is, digest with 2% relative error is twice as small
// as digest with 1% relative error.
func NewDigest(err float64) *Digest {
	if math.IsNaN(err) || err <= 0 || err >= 1 {
		panic("err must be in (0, 1)")
	}

	return &Digest{
		alpha:   err,
		gamma:   1 + 2*err/(1-err),
		gammaLn: math.Log1p(2 * err / (1 - err)),
	}
}

// Reset resets digest to the initial empty state.
func (d *Digest) Reset() {
	d.neg = d.neg[:0]
	d.pos = d.pos[:0]
	d.numNeg = 0
	d.numPos = 0
}

func (d *Digest) String() string {
	return fmt.Sprintf("Digest(err=%v%%)", d.alpha*100)
}

// Size returns the number of histogram buckets.
func (d *Digest) Size() int {
	return len(d.neg) + len(d.pos)
}

// Count returns the number of added values.
func (d *Digest) Count() uint64 {
	return d.numNeg + d.numPos
}

// Merge merges the content of v into the digest.
// Merge preserves relative error guarantees of Quantile.
//
// Merge returns an error if digests have different relative errors.
func (d *Digest) Merge(v *Digest) error {
	if v.alpha != d.alpha {
		return fmt.Errorf("can not merge digest with relative error %v%% into one with %v%%", v.alpha*100, d.alpha*100)
	}

	d.neg = grow(d.neg, len(v.neg)-1)
	for i, n := range v.neg {
		d.neg[i] += n
	}
	d.pos = grow(d.pos, len(v.pos)-1)
	for i, n := range v.pos {
		d.pos[i] += n
	}
	d.numNeg += v.numNeg
	d.numPos += v.numPos

	return nil
}

// Add adds finite positive value v to the digest.
//
// Add panics if v is outside (0, math.MaxFloat64).
func (d *Digest) Add(v float64) {
	if math.IsNaN(v) || v <= 0 || v >= math.MaxFloat64 {
		panic("v must be in (0, math.MaxFloat64)")
	}

	k := d.bucketKey(v)
	if k < 1 {
		d.neg = grow(d.neg, -k)
		d.neg[-k]++
		d.numNeg++
	} else {
		d.pos = grow(d.pos, k-1)
		d.pos[k-1]++
		d.numPos++
	}
}

// Quantile returns the q-quantile of added values
// with a maximum relative error of err.
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

	rank := uint64(1 + q*float64(d.Count()-1))
	if rank <= d.numNeg {
		i := rankIndexRev(rank, d.neg)
		return d.quantile(-i)
	} else {
		i := rankIndex(rank-d.numNeg, d.pos)
		return d.quantile(i + 1)
	}
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (d *Digest) MarshalBinary() ([]byte, error) {
	size := headerSize + len(d.neg)*8 + len(d.pos)*8
	buf := make([]byte, size)
	i := 0

	binary.LittleEndian.PutUint64(buf[i:], math.Float64bits(d.alpha))
	i += 8
	binary.LittleEndian.PutUint32(buf[i:], uint32(len(d.neg)))
	i += 4
	binary.LittleEndian.PutUint32(buf[i:], uint32(len(d.pos)))
	i += 4
	for _, b := range d.neg {
		binary.LittleEndian.PutUint64(buf[i:], b)
		i += 8
	}
	for _, b := range d.pos {
		binary.LittleEndian.PutUint64(buf[i:], b)
		i += 8
	}

	return buf, nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (d *Digest) UnmarshalBinary(data []byte) error {
	if len(data) < headerSize {
		return fmt.Errorf("not enough data to read header: %v bytes instead of minimum %v", len(data), headerSize)
	}

	i := 0
	alpha := math.Float64frombits(binary.LittleEndian.Uint64(data[i:]))
	i += 8
	if math.IsNaN(alpha) || alpha <= 0 || alpha >= 1 {
		return fmt.Errorf("invalid relative error %v", alpha)
	}
	lenNeg := binary.LittleEndian.Uint32(data[i:])
	i += 4
	lenPos := binary.LittleEndian.Uint32(data[i:])
	i += 4

	if uint32(len(data[i:])) != (lenNeg+lenPos)*8 {
		return fmt.Errorf("wrong histograms data size: %v bytes instead of %v", len(data[i:]), (lenNeg+lenPos)*8)
	}
	var neg []uint64
	numNeg := uint64(0)
	if lenNeg > 0 {
		neg = make([]uint64, lenNeg)
		for j := range neg {
			v := binary.LittleEndian.Uint64(data[i:])
			numNeg += v
			neg[j] = v
			i += 8
		}
	}
	var pos []uint64
	numPos := uint64(0)
	if lenPos > 0 {
		pos = make([]uint64, lenPos)
		for j := range pos {
			v := binary.LittleEndian.Uint64(data[i:])
			numPos += v
			pos[j] = v
			i += 8
		}
	}

	*d = Digest{
		alpha:   alpha,
		gamma:   1 + 2*alpha/(1-alpha),
		gammaLn: math.Log1p(2 * alpha / (1 - alpha)),
		neg:     neg,
		pos:     pos,
		numNeg:  numNeg,
		numPos:  numPos,
	}

	return nil
}

func (d *Digest) bucketKey(x float64) int {
	logGammaX := math.Log(x) / d.gammaLn
	return int(math.Ceil(logGammaX))
}

func (d *Digest) quantile(k int) float64 {
	powGammaK := math.Exp(float64(k) * d.gammaLn)
	return 2 * powGammaK / (d.gamma + 1)
}

func grow(buckets []uint64, ix int) []uint64 {
	n := ix + 1 - len(buckets)
	if n <= 0 {
		return buckets
	}

	return append(buckets, make([]uint64, n)...)
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
