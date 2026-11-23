package dataset

import (
	"fmt"
	"sort"
)

// Catalog es el diccionario global de distritos. Todos los nodos lo construyen
// del mismo archivo y lo ordenan por UBIGEO, así que el id de cada distrito es
// idéntico en todo el clúster (el código anterior usaba ids locales por nodo).
type Catalog struct {
	Districts []string            `json:"districts"`
	Ubigeos   []string            `json:"ubigeos"`
	Zones     []int               `json:"zones"`
	Index     map[string]int      `json:"-"`
	Volume    []int64             `json:"volume"` // denuncias clasificables 2018–2026
	Street    []int64             `json:"street"` // denuncias de calle (hurto, robo, extorsión, secuestro)
	ByClass   [][NumClasses]int64 `json:"-"`
}

func BuildCatalog(t *Table) (*Catalog, error) {
	type info struct {
		ubigeo, dept string
	}
	seen := map[string]info{}
	for _, r := range t.Rows {
		if _, ok := seen[r.District]; !ok {
			seen[r.District] = info{r.Ubigeo, r.Dept}
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool { return seen[names[i]].ubigeo < seen[names[j]].ubigeo })
	if len(names) > 64 {
		return nil, fmt.Errorf("se esperaban como máximo 64 distritos, hay %d", len(names))
	}
	c := &Catalog{Index: map[string]int{}}
	for i, n := range names {
		z := ZoneOf(seen[n].dept, n)
		if z < 0 {
			return nil, fmt.Errorf("distrito sin zona: %s", n)
		}
		c.Districts = append(c.Districts, n)
		c.Ubigeos = append(c.Ubigeos, seen[n].ubigeo)
		c.Zones = append(c.Zones, z)
		c.Index[n] = i
	}
	c.Volume = make([]int64, len(names))
	c.Street = make([]int64, len(names))
	c.ByClass = make([][NumClasses]int64, len(names))
	for _, r := range t.Rows {
		id := c.Index[r.District]
		c.Volume[id] += int64(r.Qty)
		c.ByClass[id][r.Class] += int64(r.Qty)
		if StreetCrime[r.Class] {
			c.Street[id] += int64(r.Qty)
		}
	}
	return c, nil
}

// Features construye el vector de entrada; ok=false si el distrito no existe.
func (c *Catalog) Features(district string, franja, mes, anio int) ([NumFeatures]uint8, bool) {
	var x [NumFeatures]uint8
	id, ok := c.Index[Normalize(district)]
	if !ok {
		return x, false
	}
	x[FDistrict] = uint8(id)
	x[FZone] = uint8(c.Zones[id])
	x[FFranja] = uint8(franja)
	x[FMonth] = uint8(mes - 1)
	x[FYear] = uint8(anio - BaseYear)
	return x, true
}

// Cardinality de cada variable (para los histogramas del árbol).
func (c *Catalog) Cardinality() [NumFeatures]int {
	return [NumFeatures]int{len(c.Districts), NumZones, NumFranjas, 12, MaxYearIndex + 1}
}

// Matrix es un conjunto desagregado en formato columnar (1 byte por variable).
type Matrix struct {
	X     [NumFeatures][]uint8
	Y     []uint8
	Count [NumClasses]int64
}

func (m *Matrix) Len() int { return len(m.Y) }

// Expand desagrega las filas (1 fila con cantidad c -> c denuncias) y conserva
// solo las denuncias del shard pedido: la denuncia global k pertenece al shard
// k mod shards. Así cada nodo recibe ~1/3 de CADA celda (distrito, mes, año,
// delito): un sharding estratificado, no por distrito. La franja se imputa con
// la semilla por fila de la PC3, de modo que la unión de los shards es
// exactamente el conjunto de 1,987,556 denuncias de la PC3.
func Expand(t *Table, c *Catalog, shard, shards int, keep func(year int) bool) *Matrix {
	if shards < 1 {
		shards = 1
	}
	m := &Matrix{}
	estimate := int(t.Stats.DenunciasValidas)/shards + 1
	for f := range m.X {
		m.X[f] = make([]uint8, 0, estimate)
	}
	m.Y = make([]uint8, 0, estimate)
	var k int64
	for _, r := range t.Rows {
		fs := newFranjaStream(r.RowNum, r.Class)
		id := c.Index[r.District]
		use := keep == nil || keep(r.Year)
		for j := 0; j < r.Qty; j++ {
			fr := fs.next() // se consume siempre: la franja no depende del shard
			if use && k%int64(shards) == int64(shard) {
				m.X[FDistrict] = append(m.X[FDistrict], uint8(id))
				m.X[FZone] = append(m.X[FZone], uint8(c.Zones[id]))
				m.X[FFranja] = append(m.X[FFranja], fr)
				m.X[FMonth] = append(m.X[FMonth], uint8(r.Month-1))
				m.X[FYear] = append(m.X[FYear], uint8(r.Year-BaseYear))
				m.Y = append(m.Y, uint8(r.Class))
				m.Count[r.Class]++
			}
			k++
		}
	}
	return m
}

// Cell agrupa las denuncias de prueba con el mismo vector de entrada.
type Cell struct {
	X      [NumFeatures]uint8
	Counts [NumClasses]int64
}

// AggregateYear agrupa todas las denuncias de un año por vector de entrada
// (para evaluar el bosque distribuido con ~1,800 consultas en vez de 164,221).
func AggregateYear(t *Table, c *Catalog, year int) []Cell {
	idx := map[[NumFeatures]uint8]int{}
	var cells []Cell
	for _, r := range t.Rows {
		fs := newFranjaStream(r.RowNum, r.Class)
		id := c.Index[r.District]
		for j := 0; j < r.Qty; j++ {
			fr := fs.next()
			if r.Year != year {
				continue
			}
			x := [NumFeatures]uint8{uint8(id), uint8(c.Zones[id]), fr, uint8(r.Month - 1), uint8(r.Year - BaseYear)}
			i, ok := idx[x]
			if !ok {
				i = len(cells)
				idx[x] = i
				cells = append(cells, Cell{X: x})
			}
			cells[i].Counts[r.Class]++
		}
	}
	sort.Slice(cells, func(i, j int) bool {
		a, b := cells[i].X, cells[j].X
		for f := 0; f < NumFeatures; f++ {
			if a[f] != b[f] {
				return a[f] < b[f]
			}
		}
		return false
	})
	return cells
}

// ClassTotals suma denuncias por clase para los años que cumplan keep.
func ClassTotals(t *Table, keep func(year int) bool) [NumClasses]int64 {
	var out [NumClasses]int64
	for _, r := range t.Rows {
		if keep == nil || keep(r.Year) {
			out[r.Class] += int64(r.Qty)
		}
	}
	return out
}
