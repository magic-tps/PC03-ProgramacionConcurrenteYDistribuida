// Package worker es el nodo ML: se conecta al coordinador por TCP, entrena su
// parte del bosque sobre su shard y responde predicciones.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magic-tps/pc4-grupo3/internal/dataset"
	"github.com/magic-tps/pc4-grupo3/internal/forest"
	"github.com/magic-tps/pc4-grupo3/internal/protocol"
)

type Worker struct {
	ID          string
	Coordinator string
	DataPath    string
	Workers     int // goroutines para cargar y entrenar (0 = NumCPU)
	Logf        func(format string, args ...any)

	loadMu sync.Mutex
	table  *dataset.Table
	cat    *dataset.Catalog

	trainMu sync.Mutex   // un entrenamiento a la vez
	mu      sync.RWMutex // protege el modelo: lecturas concurrentes, escritura exclusiva
	model   *forest.Model
	version int64
	shard   int
	records int64

	training atomic.Bool
	requests atomic.Int64
	started  time.Time
}

func New(id, coordinator, dataPath string, workers int) *Worker {
	return &Worker{ID: id, Coordinator: coordinator, DataPath: dataPath, Workers: workers, Logf: log.Printf, started: time.Now()}
}

// RunForever mantiene la conexión con el coordinador y reintenta si se cae.
// El modelo queda en memoria del proceso, así que una reconexión no lo pierde.
func (w *Worker) RunForever(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := w.Run(ctx)
		if ctx.Err() != nil {
			return
		}
		w.Logf("[%s] conexión con el coordinador perdida (%v); reintento en %s", w.ID, err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// Run atiende una sesión TCP. Cada mensaje se procesa en su propia goroutine;
// las escrituras al socket se serializan con un mutex.
func (w *Worker) Run(ctx context.Context) error {
	conn, err := net.DialTimeout("tcp", w.Coordinator, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	done := make(chan struct{})
	defer close(done)
	go func() { // cierra el socket si se cancela el contexto (sin fugar goroutines)
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	var encMu sync.Mutex
	enc := json.NewEncoder(conn)
	send := func(m protocol.Message) {
		encMu.Lock()
		defer encMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if err := enc.Encode(m); err != nil {
			w.Logf("[%s] error enviando %s: %v", w.ID, m.Type, err)
		}
	}
	send(protocol.Message{Type: protocol.TypeRegister, NodeID: w.ID, Stats: w.stats()})
	w.Logf("[%s] registrado en %s", w.ID, w.Coordinator)

	dec := json.NewDecoder(conn)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		var msg protocol.Message
		if err := dec.Decode(&msg); err != nil {
			return err
		}
		wg.Add(1)
		go func(m protocol.Message) {
			defer wg.Done()
			resp := w.Handle(m)
			resp.ID = m.ID
			resp.NodeID = w.ID
			send(resp)
		}(msg)
	}
}

// Handle procesa un mensaje y devuelve la respuesta (también usado en pruebas).
func (w *Worker) Handle(m protocol.Message) protocol.Message {
	w.requests.Add(1)
	switch m.Type {
	case protocol.TypePing:
		return protocol.Message{Type: protocol.TypePong, Stats: w.stats()}
	case protocol.TypeTrain:
		if m.Train == nil {
			return errMsg(protocol.CodeBadRequest, "TRAIN sin especificación")
		}
		res, err := w.Train(*m.Train)
		if err != nil {
			return errMsg(protocol.CodeInternal, err.Error())
		}
		return protocol.Message{Type: protocol.TypeTrainOK, TrainResult: res}
	case protocol.TypePredict:
		return w.predict(m.Queries)
	default:
		return errMsg(protocol.CodeBadRequest, "tipo de mensaje desconocido: "+m.Type)
	}
}

func errMsg(code, text string) protocol.Message {
	return protocol.Message{Type: protocol.TypeError, Code: code, Error: text}
}

func (w *Worker) load(path string) (*dataset.Table, *dataset.Catalog, float64, error) {
	w.loadMu.Lock()
	defer w.loadMu.Unlock()
	if w.table != nil && (path == "" || path == w.DataPath) {
		return w.table, w.cat, 0, nil
	}
	if path == "" {
		path = w.DataPath
	}
	start := time.Now()
	t, err := dataset.Load(path, w.Workers)
	if err != nil {
		return nil, nil, 0, err
	}
	c, err := dataset.BuildCatalog(t)
	if err != nil {
		return nil, nil, 0, err
	}
	w.table, w.cat, w.DataPath = t, c, path
	return t, c, float64(time.Since(start).Microseconds()) / 1000, nil
}

// Train carga el CSV (una vez por proceso), arma su shard de entrenamiento
// (años < 2026) y entrena sus árboles en paralelo. El modelo nuevo se
// construye fuera del candado y se publica con un único Lock (las
// predicciones en curso nunca ven un modelo a medio construir).
func (w *Worker) Train(spec protocol.TrainSpec) (*protocol.TrainResult, error) {
	if spec.Shards < 1 || spec.Shard < 0 || spec.Shard >= spec.Shards || len(spec.TreeIDs) == 0 {
		return nil, fmt.Errorf("especificación de entrenamiento inválida: shard %d/%d, %d árboles", spec.Shard, spec.Shards, len(spec.TreeIDs))
	}
	w.trainMu.Lock()
	defer w.trainMu.Unlock()
	w.training.Store(true)
	defer w.training.Store(false)

	start := time.Now()
	t, c, loadMs, err := w.load(spec.DataPath)
	if err != nil {
		return nil, err
	}
	t1 := time.Now()
	m := dataset.Expand(t, c, spec.Shard, spec.Shards, func(y int) bool { return y < dataset.TestYear })
	expandMs := float64(time.Since(t1).Microseconds()) / 1000
	workers := spec.Workers
	if workers <= 0 {
		workers = w.Workers
	}
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	t2 := time.Now()
	model := forest.Train(m, c.Cardinality(), spec.TreeIDs, spec.Config, workers)
	trainMs := float64(time.Since(t2).Microseconds()) / 1000
	nodes, sig := model.Signature()

	w.mu.Lock()
	w.model, w.version, w.shard, w.records = model, spec.Version, spec.Shard, int64(m.Len())
	w.mu.Unlock()

	res := &protocol.TrainResult{
		Version: spec.Version, Shard: spec.Shard, Shards: spec.Shards, Trees: len(model.Trees), TreeIDs: spec.TreeIDs,
		TrainRecords: int64(m.Len()), ClassCounts: m.Count, Workers: workers, LoadMs: loadMs, ExpandMs: expandMs,
		TrainMs: trainMs, TotalMs: float64(time.Since(start).Microseconds()) / 1000, TreeNodes: nodes,
		Importance: model.Importance, Signature: sig,
	}
	w.Logf("[%s] entrenado v%d: shard %d/%d, %d denuncias, %d árboles con %d goroutines en %.1f ms",
		w.ID, spec.Version, spec.Shard+1, spec.Shards, m.Len(), len(model.Trees), workers, trainMs)
	return res, nil
}

var errUnknown = errors.New("distrito desconocido")

func (w *Worker) predict(qs []protocol.Query) protocol.Message {
	w.mu.RLock()
	model, version, cat := w.model, w.version, w.cat
	w.mu.RUnlock()
	if model == nil || cat == nil {
		// Sin modelo NO se inventan probabilidades: el coordinador decide (409 / reentrenar).
		return errMsg(protocol.CodeNotTrained, "el nodo no tiene un modelo entrenado")
	}
	out := make([][]float64, len(qs))
	for i, q := range qs {
		if q.Franja < 0 || q.Franja >= dataset.NumFranjas || q.Mes < 1 || q.Mes > 12 ||
			q.Anio < dataset.BaseYear || q.Anio > dataset.BaseYear+dataset.MaxYearIndex {
			return errMsg(protocol.CodeBadRequest, fmt.Sprintf("consulta %d fuera de rango", i))
		}
		x, ok := cat.Features(q.District, q.Franja, q.Mes, q.Anio)
		if !ok {
			return errMsg(protocol.CodeUnknownDistrict, fmt.Sprintf("%v: %s", errUnknown, q.District))
		}
		p := model.Predict(&x)
		out[i] = p[:]
	}
	return protocol.Message{Type: protocol.TypePredictOK, Probs: out, Trees: len(model.Trees), Version: version}
}

func (w *Worker) stats() *protocol.NodeStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	w.mu.RLock()
	defer w.mu.RUnlock()
	trees := 0
	if w.model != nil {
		trees = len(w.model.Trees)
	}
	return &protocol.NodeStats{
		Trained: w.model != nil, Version: w.version, Trees: trees, Shard: w.shard, Records: w.records,
		Goroutines: runtime.NumGoroutine(), HeapMB: float64(ms.HeapAlloc) / (1 << 20), NumCPU: runtime.NumCPU(),
		UptimeS: time.Since(w.started).Seconds(), Requests: w.requests.Load(), Busy: w.training.Load(),
	}
}
