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

	"pgregory.net/bdigest"
)

func ExampleNewDefaultDigest() {
	r := rand.New(rand.NewSource(0))
	d := bdigest.NewDefaultDigest(0.01)

	for i := 0; i < 100000; i++ {
		v := math.Exp(r.NormFloat64())
		bdigest.AddXFast(d, v)
	}

	for _, q := range []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999, 1} {
		fmt.Println(d.Quantile(q))
	}
	// Output:
	// 0.016513257104242528
	// 0.2808056914403475
	// 0.5116715635730887
	// 1.01
	// 1.993661701417351
	// 3.6327611266266073
	// 5.207005866310038
	// 10.278225915562174
	// 21.978242872649467
	// 42.5242714134434
	// 266.08310784183726
}
