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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"pgregory.net/bdigest"
)

var (
	errors    = []float64{0.001}
	quantiles = []float64{0, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999, 1}
)

func BenchmarkNewDefaultDigest(b *testing.B) {
	for _, err := range errors {
		b.Run(fmt.Sprintf("%v", err), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bdigest.NewDefaultDigest(err)
			}
		})
	}
}

func BenchmarkDigest_Add(b *testing.B) {
	for _, err := range errors {
		b.Run(fmt.Sprintf("%v", err), func(b *testing.B) {
			r := rand.New(rand.NewSource(0))
			values := make([]float64, b.N)
			for i := 0; i < b.N; i++ {
				values[i] = math.Exp(r.NormFloat64())
			}
			d := bdigest.NewDefaultDigest(err)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bdigest.AddX(d, values[i])
			}
		})
	}
}

func BenchmarkDigest_Quantile(b *testing.B) {
	for _, err := range errors {
		b.Run(fmt.Sprintf("%v", err), func(b *testing.B) {
			r := rand.New(rand.NewSource(0))
			d := bdigest.NewDefaultDigest(err)
			for i := 0; i < 100000; i++ {
				bdigest.AddX(d, math.Exp(r.NormFloat64()))
			}

			for _, q := range quantiles {
				b.Run(fmt.Sprintf("q%v", q), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						d.Quantile(q)
					}
				})
			}
		})
	}
}

func BenchmarkDigest_Merge(b *testing.B) {
	for _, err := range errors {
		b.Run(fmt.Sprintf("%v", err), func(b *testing.B) {
			r := rand.New(rand.NewSource(0))
			d1 := bdigest.NewDefaultDigest(err)
			d2 := bdigest.NewDefaultDigest(err)
			for i := 0; i < 100000; i++ {
				bdigest.AddX(d1, math.Exp(r.NormFloat64()))
				bdigest.AddX(d2, math.Exp(r.NormFloat64()))
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_ = d1.Merge(d2)
			}
		})
	}
}
