package dataset

import (
	"os"
	"testing"
)

const csvPath = "../../data/DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"

func loadOrSkip(t *testing.T, workers int) *Table {
	t.Helper()
	if _, err := os.Stat(csvPath); err != nil {
		t.Skip("CSV no disponible")
	}
	tab, err := Load(csvPath, workers)
	if err != nil {
		t.Fatal(err)
	}
	return tab
}

func TestNormalizeAndClasses(t *testing.T) {
	cases := map[string]int{"Extorsión": 4, "EXTORSIÓN": 4, "  hurto ": 1, "Violencia contra la mujer e integrantes": 0, "Otros": -1, "xyz": -2}
	for in, want := range cases {
		if got := ClassOf(Normalize(in)); got != want {
			t.Errorf("ClassOf(%q) = %d, se esperaba %d", in, got, want)
		}
	}
	if Normalize("Breña") != "BREÑA" {
		t.Errorf("la Ñ debe conservarse")
	}
	if ZoneOf("PROV. CONST. DEL CALLAO", "VENTANILLA") != 4 || ZoneOf("LIMA METROPOLITANA", "LURIGANCHO - CHOSICA") != 2 ||
		ZoneOf("LIMA METROPOLITANA", "PUCUSANA") != 3 {
		t.Errorf("zonas incorrectas")
	}
}

// Los totales deben coincidir con los verificados en la PC3.
func TestTotalsMatchPC3(t *testing.T) {
	tab := loadOrSkip(t, 4)
	s := tab.Stats
	if s.RowsRead != 376715 || s.DenunciasNacional != 7509690 || s.RowsValid != 25849 || s.DenunciasValidas != 1987556 {
		t.Fatalf("totales distintos a la PC3: %+v", s)
	}
	cat, err := BuildCatalog(tab)
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Districts) != 50 {
		t.Fatalf("se esperaban 50 distritos, hay %d", len(cat.Districts))
	}
	tot := ClassTotals(tab, nil)
	if tot[4] != 50677 {
		t.Fatalf("extorsión debe tener 50,677 denuncias en Lima y Callao, tiene %d", tot[4])
	}
	t.Logf("filas=%d válidas=%d denuncias=%d clases=%v carga=%.1f ms", s.RowsRead, s.RowsValid, s.DenunciasValidas, tot, s.LoadMs)
}

// El resultado de la carga concurrente no depende del número de workers.
func TestLoadDeterministic(t *testing.T) {
	a := loadOrSkip(t, 1)
	b := loadOrSkip(t, 8)
	if len(a.Rows) != len(b.Rows) {
		t.Fatal("distinto número de filas")
	}
	for i := range a.Rows {
		if a.Rows[i] != b.Rows[i] {
			t.Fatalf("fila %d distinta con 1 y 8 workers", i)
		}
	}
}

// Los 3 shards son disjuntos, suman el total y cada uno tiene ~1/3 de cada clase y distrito.
func TestShardsStratified(t *testing.T) {
	tab := loadOrSkip(t, 0)
	cat, _ := BuildCatalog(tab)
	full := Expand(tab, cat, 0, 1, nil)
	if full.Len() != 1987556 {
		t.Fatalf("expansión total %d", full.Len())
	}
	var sum int
	for s := 0; s < 3; s++ {
		m := Expand(tab, cat, s, 3, nil)
		sum += m.Len()
		share := float64(m.Len()) / float64(full.Len())
		if share < 0.333 || share > 0.334 {
			t.Errorf("shard %d tiene %.4f del total", s, share)
		}
		for k := 0; k < NumClasses; k++ {
			r := float64(m.Count[k]) / float64(full.Count[k])
			if r < 0.33 || r > 0.337 {
				t.Errorf("shard %d clase %d: %.4f del total de la clase", s, k, r)
			}
		}
		seen := map[uint8]bool{}
		for _, d := range m.X[FDistrict] {
			seen[d] = true
		}
		if len(seen) != 50 {
			t.Errorf("shard %d solo tiene %d distritos", s, len(seen))
		}
		t.Logf("shard %d: %d denuncias (%.2f%%)", s+1, m.Len(), 100*share)
	}
	if sum != full.Len() {
		t.Fatalf("los shards suman %d, se esperaban %d", sum, full.Len())
	}
	// Franja imputada: debe reproducir la distribución a priori de la PC3 (Tabla 18).
	var byClass [NumClasses][NumFranjas]float64
	for i := range full.Y {
		byClass[full.Y[i]][full.X[FFranja][i]]++
	}
	for k := 0; k < NumClasses; k++ {
		n := float64(full.Count[k])
		for f := 0; f < NumFranjas; f++ {
			if d := byClass[k][f]/n - priorFranja[k][f]; k != 5 && (d > 0.003 || d < -0.003) {
				t.Errorf("franja clase %d franja %d se aleja del supuesto: %.4f", k, f, d)
			}
		}
	}
	test := AggregateYear(tab, cat, TestYear)
	var n int64
	for _, c := range test {
		for _, v := range c.Counts {
			n += v
		}
	}
	if n != 164221 {
		t.Fatalf("prueba 2026: %d denuncias, la PC3 reporta 164,221", n)
	}
	t.Logf("prueba 2026: %d denuncias en %d vectores distintos", n, len(test))
}
