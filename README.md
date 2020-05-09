# b-digest [![GoDoc][godoc-img]][godoc] [![CI][ci-img]][ci]

b-digest is a library for fast and memory-efficient estimation
of quantiles with guaranteed relative error and full mergeability.

```go
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
		d.Add(v)
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
```

[godoc-img]: https://godoc.org/pgregory.net/bdigest?status.svg
[godoc]: https://godoc.org/pgregory.net/bdigest
[ci-img]: https://github.com/flyingmutant/bdigest/workflows/CI/badge.svg
[ci]: https://github.com/flyingmutant/bdigest/actions
