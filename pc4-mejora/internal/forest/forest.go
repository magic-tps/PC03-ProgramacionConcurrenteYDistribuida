// Package forest implementa árboles CART (Gini) y su entrenamiento paralelo
// con un Worker Pool de goroutines, igual que la PC3, pero sobre el shard de
// cada nodo. Los histogramas por valor hacen que cada nivel del árbol cueste
// O(muestras × mtry) en vez de O(muestras × umbrales).
package forest

import (
	"math/rand"
	"runtime"
	"slices"
	"sort"
	"sync"

	"github.com/magic-tps/pc4-grupo3/internal/dataset"
)

const (
	K = dataset.NumClasses
	F = dataset.NumFeatures
)

// Config son los hiperparámetros (valores por defecto = PC3, Tabla 19).
type Config struct {
	MaxDepth   int     `json:"maxDepth" bson:"maxDepth"`
	MinLeaf    int     `json:"minLeaf" bson:"minLeaf"`
	Mtry       int     `json:"mtry" bson:"mtry"`
	SampleFrac float64 `json:"sampleFrac" bson:"sampleFrac"`
	Seed       int64   `json:"seed" bson:"seed"`
}

func DefaultConfig() Config {
	return Config{MaxDepth: 12, MinLeaf: 50, Mtry: 3, SampleFrac: 0.30, Seed: 42}
}

// Node de un árbol. Para variables categóricas se usa una máscara de bits
// (categorías que van a la izquierda); para ordinales, un umbral v <= Thr.
type Node struct {
	Leaf    bool
	Feature int8
	Thr     uint8
	Mask    uint64
	Left    int32
	Right   int32
	Prob    [K]float32
}

type Tree struct {
	ID    int
	Nodes []Node
}

// Model es el conjunto de árboles que entrena un nodo del clúster.
type Model struct {
	Trees      []*Tree
	Importance [F]float64 // suma de reducciones de impureza (sin normalizar)
}

// Train entrena los árboles con ids globales treeIDs usando un Worker Pool:
// las goroutines toman ids de un canal y escriben solo en su posición del
// slice, por lo que el resultado es idéntico con 1 o con N workers.
func Train(m *dataset.Matrix, card [F]int, treeIDs []int, cfg Config, workers int) *Model {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	trees := make([]*Tree, len(treeIDs))
	imps := make([][F]float64, len(treeIDs))
	jobs := make(chan int, len(treeIDs))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				trees[i], imps[i] = trainTree(m, card, treeIDs[i], cfg)
			}
		}()
	}
	for i := range treeIDs {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	out := &Model{Trees: trees}
	for _, imp := range imps {
		for f := range imp {
			out.Importance[f] += imp[f]
		}
	}
	return out
}

type builder struct {
	m       *dataset.Matrix
	card    [F]int
	cfg     Config
	rng     *rand.Rand
	tree    *Tree
	imp     [F]float64
	scratch []int32
	hist    [F][][K]int64
}

func trainTree(m *dataset.Matrix, card [F]int, id int, cfg Config) (*Tree, [F]float64) {
	rng := rand.New(rand.NewSource(cfg.Seed + int64(id)*7919))
	n := m.Len()
	size := int(cfg.SampleFrac * float64(n))
	if size < 1 {
		size = n
	}
	t := &Tree{ID: id}
	if n == 0 {
		root := Node{Leaf: true, Feature: -1}
		t.Nodes = append(t.Nodes, root)
		return t, [F]float64{}
	}
	idx := make([]int32, size)
	for i := range idx {
		idx[i] = int32(rng.Intn(n)) // bootstrap con reemplazo
	}
	slices.Sort(idx) // acceso secuencial a la matriz (mejora de localidad propuesta en la PC3)
	b := &builder{m: m, card: card, cfg: cfg, rng: rng, tree: t, scratch: make([]int32, size)}
	for f := 0; f < F; f++ {
		b.hist[f] = make([][K]int64, card[f])
	}
	b.build(idx, 0)
	return t, b.imp
}

func sumsq(c *[K]int64) float64 {
	s := 0.0
	for _, v := range c {
		s += float64(v) * float64(v)
	}
	return s
}

type split struct {
	feature int
	score   float64
	thr     uint8
	mask    uint64
}

func (b *builder) build(idx []int32, depth int) int32 {
	pos := int32(len(b.tree.Nodes))
	b.tree.Nodes = append(b.tree.Nodes, Node{Leaf: true, Feature: -1})
	var counts [K]int64
	for _, i := range idx {
		counts[b.m.Y[i]]++
	}
	n := len(idx)
	majority, classes := 0, 0
	for k := 0; k < K; k++ {
		b.tree.Nodes[pos].Prob[k] = float32(float64(counts[k]) / float64(n))
		if counts[k] > counts[majority] {
			majority = k
		}
		if counts[k] > 0 {
			classes++
		}
	}
	if depth >= b.cfg.MaxDepth || n < 2*b.cfg.MinLeaf || classes <= 1 {
		return pos
	}
	parent := sumsq(&counts) / float64(n)
	best := split{feature: -1, score: parent + 1e-9}

	// mtry variables candidatas al azar (subespacios aleatorios)
	feats := [F]int{0, 1, 2, 3, 4}
	for i := 0; i < b.cfg.Mtry && i < F; i++ {
		j := i + b.rng.Intn(F-i)
		feats[i], feats[j] = feats[j], feats[i]
	}
	order := make([]int, 0, 64)
	for _, f := range feats[:min(b.cfg.Mtry, F)] {
		hist := b.hist[f]
		for v := range hist {
			hist[v] = [K]int64{}
		}
		col := b.m.X[f]
		for _, i := range idx {
			hist[col[i]][b.m.Y[i]]++
		}
		tot := func(v int) int64 {
			var s int64
			for _, c := range hist[v] {
				s += c
			}
			return s
		}
		order = order[:0]
		for v := range hist {
			if tot(v) > 0 {
				order = append(order, v)
			}
		}
		if len(order) < 2 {
			continue
		}
		if dataset.Categorical[f] {
			// Heurística de CART: ordenar categorías por la proporción de la clase mayoritaria.
			sort.SliceStable(order, func(a, c int) bool {
				ra := float64(hist[order[a]][majority]) / float64(tot(order[a]))
				rc := float64(hist[order[c]][majority]) / float64(tot(order[c]))
				return ra < rc
			})
		}
		var left [K]int64
		var nL int64
		var mask uint64
		for j := 0; j < len(order)-1; j++ {
			v := order[j]
			for k := 0; k < K; k++ {
				left[k] += hist[v][k]
			}
			nL += tot(v)
			mask |= 1 << uint(v)
			nR := int64(n) - nL
			if nL < int64(b.cfg.MinLeaf) || nR < int64(b.cfg.MinLeaf) {
				continue
			}
			var right [K]int64
			for k := 0; k < K; k++ {
				right[k] = counts[k] - left[k]
			}
			score := sumsq(&left)/float64(nL) + sumsq(&right)/float64(nR)
			if score > best.score {
				best = split{feature: f, score: score, thr: uint8(v), mask: mask}
			}
		}
	}
	if best.feature < 0 {
		return pos
	}
	// Reducción de impureza de Gini = score - parent (importancia de la variable).
	b.imp[best.feature] += best.score - parent

	// Partición estable: conserva el orden (localidad) dentro de cada lado.
	col := b.m.X[best.feature]
	cat := dataset.Categorical[best.feature]
	goesLeft := func(v uint8) bool {
		if cat {
			return best.mask&(1<<uint(v)) != 0
		}
		return v <= best.thr
	}
	scratch := b.scratch[:0]
	w := 0
	for _, i := range idx {
		if goesLeft(col[i]) {
			idx[w] = i
			w++
		} else {
			scratch = append(scratch, i)
		}
	}
	copy(idx[w:], scratch)
	nd := &b.tree.Nodes[pos]
	nd.Leaf = false
	nd.Feature = int8(best.feature)
	nd.Thr = best.thr
	nd.Mask = best.mask
	l := b.build(idx[:w], depth+1)
	r := b.build(idx[w:], depth+1)
	b.tree.Nodes[pos].Left = l // re-indexar: append pudo mover el slice
	b.tree.Nodes[pos].Right = r
	return pos
}

// Predict recorre el árbol y devuelve las probabilidades de la hoja.
func (t *Tree) Predict(x *[F]uint8) *[K]float32 {
	pos := int32(0)
	for {
		nd := &t.Nodes[pos]
		if nd.Leaf {
			return &nd.Prob
		}
		v := x[nd.Feature]
		var left bool
		if dataset.Categorical[nd.Feature] {
			left = v < 64 && nd.Mask&(1<<uint(v)) != 0
		} else {
			left = v <= nd.Thr
		}
		if left {
			pos = nd.Left
		} else {
			pos = nd.Right
		}
	}
}

// Predict promedia las probabilidades de los árboles (votación suave).
func (m *Model) Predict(x *[F]uint8) [K]float64 {
	var out [K]float64
	if len(m.Trees) == 0 {
		return out
	}
	for _, t := range m.Trees {
		p := t.Predict(x)
		for k := 0; k < K; k++ {
			out[k] += float64(p[k])
		}
	}
	for k := range out {
		out[k] /= float64(len(m.Trees))
	}
	return out
}

// Signature resume el modelo (número de nodos y suma ponderada de hojas) para
// comprobar que 1 y N workers producen exactamente el mismo bosque.
func (m *Model) Signature() (int, float64) {
	nodes, sum := 0, 0.0
	for _, t := range m.Trees {
		nodes += len(t.Nodes)
		for i, nd := range t.Nodes {
			if nd.Leaf {
				for k := 0; k < K; k++ {
					sum += float64(nd.Prob[k]) * float64((i%97)+k+1)
				}
			}
		}
	}
	return nodes, sum
}
