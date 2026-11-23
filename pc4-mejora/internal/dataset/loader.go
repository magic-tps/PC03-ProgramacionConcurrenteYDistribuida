package dataset

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Row es una fila válida, dentro del alcance y clasificable (antes de desagregar).
type Row struct {
	RowNum   int64 // número global de fila de datos (0 = primera fila tras la cabecera)
	Year     int
	Month    int
	Dept     string
	District string
	Ubigeo   string
	Class    int
	Qty      int
}

// Stats resume la carga (permite contrastar con la PC3).
type Stats struct {
	RowsRead          int64   `json:"rowsRead" bson:"rowsRead"`
	RowsInvalid       int64   `json:"rowsInvalid" bson:"rowsInvalid"`
	RowsOutOfScope    int64   `json:"rowsOutOfScope" bson:"rowsOutOfScope"`
	RowsOtros         int64   `json:"rowsOtros" bson:"rowsOtros"`
	RowsValid         int64   `json:"rowsValid" bson:"rowsValid"`
	DenunciasNacional int64   `json:"denunciasNacional" bson:"denunciasNacional"`
	DenunciasValidas  int64   `json:"denunciasValidas" bson:"denunciasValidas"`
	Batches           int     `json:"batches" bson:"batches"`
	Workers           int     `json:"workers" bson:"workers"`
	LoadMs            float64 `json:"loadMs" bson:"loadMs"`
}

// Table contiene las filas válidas en el orden del archivo.
type Table struct {
	Rows  []Row
	Stats Stats
}

type rawBatch struct {
	id    int
	start int64
	recs  [][]string
}

type cleanBatch struct {
	id    int
	rows  []Row
	stats Stats
}

const batchSize = 10000

// Load lee el CSV con un pipeline productor–consumidor (patrón de la PC3):
// un lector agrupa filas en lotes que viajan por un canal con búfer hacia un
// Worker Pool que valida y normaliza; el recolector reordena por id de lote
// para que el resultado sea idéntico con 1 o con N workers.
func Load(path string, workers int) (*Table, error) {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	start := time.Now()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	jobs := make(chan rawBatch, 8) // cola FIFO acotada (backpressure)
	results := make(chan cleanBatch, 8)
	readErr := make(chan error, 1)

	// Productor
	go func() {
		defer close(jobs)
		r := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
		r.FieldsPerRecord = -1
		r.LazyQuotes = true
		if _, err := r.Read(); err != nil { // cabecera
			readErr <- fmt.Errorf("cabecera: %w", err)
			return
		}
		var rowNum int64
		id := 0
		batch := rawBatch{id: id, start: rowNum, recs: make([][]string, 0, batchSize)}
		for {
			rec, err := r.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				rec = nil // fila malformada: el worker la contará como inválida
			}
			batch.recs = append(batch.recs, rec)
			rowNum++
			if len(batch.recs) == batchSize {
				jobs <- batch
				id++
				batch = rawBatch{id: id, start: rowNum, recs: make([][]string, 0, batchSize)}
			}
		}
		if len(batch.recs) > 0 {
			jobs <- batch
		}
		readErr <- nil
	}()

	// Worker Pool
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range jobs {
				results <- cleanRows(b)
			}
		}()
	}
	go func() { wg.Wait(); close(results) }()

	// Recolector
	var batches []cleanBatch
	for cb := range results {
		batches = append(batches, cb)
	}
	if err := <-readErr; err != nil {
		return nil, err
	}
	sort.Slice(batches, func(i, j int) bool { return batches[i].id < batches[j].id })
	t := &Table{}
	for _, b := range batches {
		t.Rows = append(t.Rows, b.rows...)
		t.Stats.RowsRead += b.stats.RowsRead
		t.Stats.RowsInvalid += b.stats.RowsInvalid
		t.Stats.RowsOutOfScope += b.stats.RowsOutOfScope
		t.Stats.RowsOtros += b.stats.RowsOtros
		t.Stats.RowsValid += b.stats.RowsValid
		t.Stats.DenunciasNacional += b.stats.DenunciasNacional
		t.Stats.DenunciasValidas += b.stats.DenunciasValidas
	}
	t.Stats.Batches = len(batches)
	t.Stats.Workers = workers
	t.Stats.LoadMs = float64(time.Since(start).Microseconds()) / 1000
	return t, nil
}

func cleanRows(b rawBatch) cleanBatch {
	out := cleanBatch{id: b.id, rows: make([]Row, 0, len(b.recs)/10)}
	maxYear := time.Now().Year() + 1
	for k, rec := range b.recs {
		out.stats.RowsRead++
		if len(rec) != 8 {
			out.stats.RowsInvalid++
			continue
		}
		year, e1 := strconv.Atoi(strings.TrimSpace(rec[0]))
		month, e2 := strconv.Atoi(strings.TrimSpace(rec[1]))
		qty, e3 := strconv.Atoi(strings.TrimSpace(rec[7]))
		dept, prov, dist := Normalize(rec[2]), Normalize(rec[3]), Normalize(rec[4])
		mod := Normalize(rec[6])
		ubigeo := strings.TrimSpace(rec[5])
		if e1 != nil || e2 != nil || e3 != nil || year < 2010 || year > maxYear || month < 1 || month > 12 ||
			qty < 1 || qty > 100000 || dept == "" || prov == "" || dist == "" || ubigeo == "" {
			out.stats.RowsInvalid++
			continue
		}
		class := ClassOf(mod)
		if class == -2 {
			out.stats.RowsInvalid++
			continue
		}
		out.stats.DenunciasNacional += int64(qty)
		if !InScope(dept, prov) {
			out.stats.RowsOutOfScope++
			continue
		}
		if class < 0 {
			out.stats.RowsOtros++
			continue
		}
		if len(ubigeo) == 5 {
			ubigeo = "0" + ubigeo
		}
		out.stats.RowsValid++
		out.stats.DenunciasValidas += int64(qty)
		out.rows = append(out.rows, Row{
			RowNum: b.start + int64(k), Year: year, Month: month, Dept: dept,
			District: dist, Ubigeo: ubigeo, Class: class, Qty: qty,
		})
	}
	return out
}
