package forest

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/magic-tps/pc4-grupo3/internal/dataset"
)

const csvPath = "../../data/DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"

// Datos sintéticos: la clase depende del distrito (categórico) y del mes (ordinal).
func synthetic(n int) (*dataset.Matrix, [F]int) {
	rng := rand.New(rand.NewSource(7))
	m := &dataset.Matrix{}
	for i := 0; i < n; i++ {
		d, z, f, mo, y := uint8(rng.Intn(10)), uint8(rng.Intn(5)), uint8(rng.Intn(4)), uint8(rng.Intn(12)), uint8(rng.Intn(8))
		c := uint8(1)
		if d%3 == 0 {
			c = 0
		} else if mo >= 6 {
			c = 2
		}
		if rng.Float64() < 0.05 {
			c = uint8(rng.Intn(K))
		}
		for j, v := range []uint8{d, z, f, mo, y} {
			m.X[j] = append(m.X[j], v)
		}
		m.Y = append(m.Y, c)
		m.Count[c]++
	}
	return m, [F]int{10, 5, 4, 12, 16}
}

func TestLearnsPattern(t *testing.T) {
	m, card := synthetic(20000)
	cfg := DefaultConfig()
	cfg.Mtry = 5
	model := Train(m, card, []int{0, 1, 2, 3}, cfg, 2)
	ok := 0
	for i := 0; i < 2000; i++ {
		x := [F]uint8{m.X[0][i], m.X[1][i], m.X[2][i], m.X[3][i], m.X[4][i]}
		p := model.Predict(&x)
		best := 0
		for k := range p {
			if p[k] > p[best] {
				best = k
			}
		}
		if uint8(best) == m.Y[i] {
			ok++
		}
	}
	if acc := float64(ok) / 2000; acc < 0.9 {
		t.Fatalf("exactitud %.3f en un patrón simple", acc)
	}
}

// El bosque es idéntico con 1 y con N goroutines (cada una escribe solo su árbol).
func TestDeterministicWorkers(t *testing.T) {
	m, card := synthetic(30000)
	ids := []int{0, 1, 2, 3, 4, 5, 6, 7}
	a := Train(m, card, ids, DefaultConfig(), 1)
	b := Train(m, card, ids, DefaultConfig(), 8)
	na, sa := a.Signature()
	nb, sb := b.Signature()
	if na != nb || sa != sb || a.Importance != b.Importance {
		t.Fatalf("bosques distintos: %d/%f vs %d/%f", na, sa, nb, sb)
	}
}

// Prueba con el dataset real: 32 árboles repartidos 11/11/10 en 3 shards,
// evaluados en 2026 contra la línea base de la clase mayoritaria (como la PC3).
func TestRealDataDistributedForest(t *testing.T) {
	if _, err := os.Stat(csvPath); err != nil {
		t.Skip("CSV no disponible")
	}
	tab, err := dataset.Load(csvPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	cat, _ := dataset.BuildCatalog(tab)
	train := func(y int) bool { return y < dataset.TestYear }
	split := [][]int{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, {22, 23, 24, 25, 26, 27, 28, 29, 30, 31}}
	var models []*Model
	start := time.Now()
	for s := 0; s < 3; s++ {
		m := dataset.Expand(tab, cat, s, 3, train)
		t0 := time.Now()
		models = append(models, Train(m, cat.Cardinality(), split[s], DefaultConfig(), 0))
		t.Logf("shard %d: %d denuncias, %d árboles en %v", s+1, m.Len(), len(split[s]), time.Since(t0).Round(time.Millisecond))
	}
	t.Logf("entrenamiento total %v", time.Since(start).Round(time.Millisecond))
	cells := dataset.AggregateYear(tab, cat, dataset.TestYear)
	totals := dataset.ClassTotals(tab, train)
	base := 0
	for k := range totals {
		if totals[k] > totals[base] {
			base = k
		}
	}
	var correct, baseHits, n int64
	for _, c := range cells {
		var p [K]float64
		trees := 0
		for _, mdl := range models {
			pp := mdl.Predict(&c.X)
			for k := range p {
				p[k] += pp[k] * float64(len(mdl.Trees))
			}
			trees += len(mdl.Trees)
		}
		best := 0
		for k := range p {
			if p[k] > p[best] {
				best = k
			}
		}
		for k, v := range c.Counts {
			n += v
			if k == best {
				correct += v
			}
			if k == base {
				baseHits += v
			}
		}
	}
	acc, baseAcc := float64(correct)/float64(n), float64(baseHits)/float64(n)
	t.Logf("prueba 2026: %d denuncias; exactitud %.2f%% vs línea base %.2f%%", n, 100*acc, 100*baseAcc)
	if acc <= baseAcc {
		t.Fatalf("el bosque no supera la línea base")
	}
}
