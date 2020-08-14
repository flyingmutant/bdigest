# b-digest [![PkgGoDev][godev-img]][godev] [![CI][ci-img]][ci]

B-digest is a Go library for fast and memory-efficient estimation
of quantiles with guaranteed relative error and full mergeability.

```go
package bdigest_test

import (
	"fmt"
	"math"
	"math/rand"

	"pgregory.net/bdigest"
)

func ExampleNewDigest() {
	r := rand.New(rand.NewSource(0))
	d := bdigest.NewDigest(0.05)

	for i := 0; i < 100000; i++ {
		v := math.Exp(r.NormFloat64())
		d.Add(v)
	}

	fmt.Printf("%v buckets\n", d.Size())
	for _, q := range []float64{0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999, 1} {
		fmt.Printf("%v\tq%v\n", d.Quantile(q), q)
	}

	// Output:
	// 98 buckets
	// 0.015690260723105844	q0
	// 0.2858480802952493	q0.1
	// 0.5211100423907477	q0.25
	// 1.05	q0.5
	// 1.9141830301785612	q0.75
	// 3.489615879070075	q0.9
	// 5.2076333497857386	q0.95
	// 10.493014090054524	q0.99
	// 21.142683691165157	q0.999
	// 42.601017193748824	q0.9999
	// 258.10858921508054	q1
}
```

## License

B-digest is licensed under the [Apache License Version 2.0](./LICENSE).

[godev-img]: https://pkg.go.dev/badge/pgregory.net/bdigest
[godev]: https://pkg.go.dev/pgregory.net/bdigest
[ci-img]: https://github.com/flyingmutant/bdigest/workflows/CI/badge.svg
[ci]: https://github.com/flyingmutant/bdigest/actions
