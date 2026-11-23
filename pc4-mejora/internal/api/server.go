// Package api es el coordinador HTTP: expone la API REST, reparte el
// entrenamiento y las predicciones entre los nodos ML y persiste resultados.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magic-tps/pc4-grupo3/internal/cluster"
	"github.com/magic-tps/pc4-grupo3/internal/dataset"
	"github.com/magic-tps/pc4-grupo3/internal/forest"
	"github.com/magic-tps/pc4-grupo3/internal/protocol"
	"github.com/magic-tps/pc4-grupo3/internal/storage"
)

type Config struct {
	CacheTTL        time.Duration
	TrainTimeout    time.Duration
	PredictTimeout  time.Duration
	MetricsInterval time.Duration
	ExpectedNodes   int
	OpenAPISpec     []byte
	Logf            func(format string, args ...any)
}

type Server struct {
	cfg   Config
	coord *cluster.Coordinator
	store storage.Store

	cat         *dataset.Catalog
	loadStats   dataset.Stats
	testCells   []dataset.Cell
	trainTotals [dataset.NumClasses]int64
	started     time.Time

	trainMu sync.Mutex // un entrenamiento a la vez
	mu      sync.RWMutex
	model   *modelState

	retraining sync.Map
	lat        *latencies
	nPredict   atomic.Int64
	nHits      atomic.Int64
	nMiss      atomic.Int64
	nPatrol    atomic.Int64
	nErrors    atomic.Int64

	persistCh chan persistJob
	dropped   atomic.Int64
	stop      chan struct{}
	wg        sync.WaitGroup
}

// modelState describe el bosque vigente. Ready marca los nodos que tienen
// cargada esta versión; solo ellos participan en las predicciones.
type modelState struct {
	Version    int64
	TrainedAt  time.Time
	Config     forest.Config
	TotalTrees int
	Accuracy   float64
	Assign     map[string]protocol.TrainSpec
	Ready      map[string]bool
}

type persistJob struct {
	collection string
	doc        any
}

var (
	errNotTrained      = errors.New("modelo no entrenado")
	errNoReadyNodes    = errors.New("ningún nodo ML disponible con el modelo vigente")
	errUnknownDistrict = errors.New("distrito desconocido")
)

// New crea el servidor a partir del dataset ya cargado por el coordinador.
func New(cfg Config, coord *cluster.Coordinator, store storage.Store, t *dataset.Table, cat *dataset.Catalog) *Server {
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 10 * time.Minute
	}
	if cfg.TrainTimeout <= 0 {
		cfg.TrainTimeout = 2 * time.Minute
	}
	if cfg.PredictTimeout <= 0 {
		cfg.PredictTimeout = 3 * time.Second
	}
	if cfg.ExpectedNodes <= 0 {
		cfg.ExpectedNodes = 3
	}
	s := &Server{
		cfg: cfg, coord: coord, store: store, cat: cat, loadStats: t.Stats, started: time.Now(),
		testCells:   dataset.AggregateYear(t, cat, dataset.TestYear),
		trainTotals: dataset.ClassTotals(t, func(y int) bool { return y < dataset.TestYear }),
		lat:         newLatencies(),
		persistCh:   make(chan persistJob, 1024),
		stop:        make(chan struct{}),
	}
	coord.OnRegister = s.onRegister
	s.wg.Add(1)
	go s.persistLoop()
	if cfg.MetricsInterval > 0 {
		s.wg.Add(1)
		go s.metricsLoop()
	}
	return s
}

// Close detiene las goroutines de fondo y vacía la cola de persistencia.
func (s *Server) Close() {
	close(s.stop)
	close(s.persistCh)
	s.wg.Wait()
}

// persistLoop escribe en MongoDB de forma asíncrona: la latencia de la API no
// depende de la base de datos (un solo consumidor, cola con búfer).
func (s *Server) persistLoop() {
	defer s.wg.Done()
	for job := range s.persistCh {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.store.Insert(ctx, job.collection, job.doc); err != nil {
			s.cfg.Logf("persistencia %s: %v", job.collection, err)
		}
		cancel()
	}
}

func (s *Server) persist(collection string, doc any) {
	select {
	case s.persistCh <- persistJob{collection, doc}:
	default:
		s.dropped.Add(1) // cola llena: no se bloquea la petición
	}
}

func (s *Server) metricsLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.cfg.MetricsInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			m := s.metrics(ctx)
			cancel()
			if b, err := json.Marshal(m); err == nil {
				s.store.CacheSet(context.Background(), "pc4:metrics:latest", b, 2*s.cfg.MetricsInterval)
			}
			s.persist(storage.CollMetrics, m)
		}
	}
}

// Handler registra las rutas (patrones con método de Go 1.22: un método no
// permitido responde 405 automáticamente).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	mux.HandleFunc("POST /api/v1/train", s.handleTrain)
	mux.HandleFunc("POST /api/v1/predict", s.handlePredict)
	mux.HandleFunc("GET /api/v1/patrullaje", s.handlePatrol)
	mux.HandleFunc("GET /api/v1/districts", s.handleDistricts)
	mux.HandleFunc("GET /api/v1/metrics", s.handleMetrics)
	mux.HandleFunc("GET /swagger.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		_, _ = w.Write(s.cfg.OpenAPISpec)
	})
	mux.HandleFunc("GET /swagger", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(swaggerHTML))
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger", http.StatusFound)
	})
	return s.middleware(mux)
}

const swaggerHTML = `<!doctype html><html lang="es"><head><meta charset="utf-8"><title>API PC4 · Grupo 3</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head>
<body><div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url:'/swagger.yaml',dom_id:'#swagger-ui',displayRequestDuration:true})</script>
</body></html>`

// recorder fija X-Response-Time-Ms ANTES de enviar las cabeceras (en la versión
// anterior se asignaba después de escribir la respuesta y nunca llegaba).
type recorder struct {
	http.ResponseWriter
	start  time.Time
	status int
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
		r.Header().Set("X-Response-Time-Ms", strconv.FormatFloat(msSince(r.start), 'f', 3, 64))
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &recorder{ResponseWriter: w, start: time.Now()}
		next.ServeHTTP(rec, r)
		ms := msSince(rec.start)
		if rec.status >= 400 {
			s.nErrors.Add(1)
		}
		s.cfg.Logf("%s %s -> %d (%.3f ms)", r.Method, r.URL.RequestURI(), rec.status, ms)
	})
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg, hint string) {
	writeJSON(w, status, ErrorResponse{Error: msg, Code: code, Hint: hint})
}

// ---------------------------------------------------------------- modelo

func (s *Server) current() *modelState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.model
}

func (s *Server) modelInfo() ModelInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	mi := ModelInfo{NodesReady: []string{}}
	if s.model == nil {
		return mi
	}
	st := s.model
	t := st.TrainedAt
	cfg := st.Config
	mi.Trained, mi.Version, mi.TrainedAt, mi.TotalTrees, mi.Accuracy, mi.Config = true, st.Version, &t, st.TotalTrees, st.Accuracy, &cfg
	for id, ok := range st.Ready {
		if ok {
			mi.NodesReady = append(mi.NodesReady, id)
		}
	}
	sort.Strings(mi.NodesReady)
	return mi
}

// onRegister: si un nodo vuelve (p. ej. tras reiniciar su contenedor) y no
// tiene la versión vigente, se le reenvía su parte del entrenamiento.
func (s *Server) onRegister(id string, stats protocol.NodeStats) {
	s.mu.Lock()
	st := s.model
	if st == nil {
		s.mu.Unlock()
		return
	}
	_, assigned := st.Assign[id]
	ready := assigned && stats.Trained && stats.Version == st.Version
	st.Ready[id] = ready
	s.mu.Unlock()
	if assigned && !ready {
		s.retrainNode(id, st)
	}
}

func (s *Server) retrainNode(id string, st *modelState) {
	if _, busy := s.retraining.LoadOrStore(id, true); busy {
		return
	}
	defer s.retraining.Delete(id)
	spec := st.Assign[id]
	s.cfg.Logf("reentrenando %s (versión %d, shard %d/%d, %d árboles)", id, st.Version, spec.Shard+1, spec.Shards, len(spec.TreeIDs))
	resp, err := s.coord.Call(id, protocol.Message{Type: protocol.TypeTrain, Train: &spec}, s.cfg.TrainTimeout)
	if err != nil || resp.Type != protocol.TypeTrainOK || resp.TrainResult == nil || resp.TrainResult.Version != st.Version {
		s.cfg.Logf("reentrenamiento de %s fallido: %v %s", id, err, resp.Error)
		return
	}
	s.mu.Lock()
	if s.model == st {
		st.Ready[id] = true
	}
	s.mu.Unlock()
	s.cfg.Logf("%s listo de nuevo con la versión %d", id, st.Version)
}

// clusterPredict reparte el lote de consultas a los nodos listos y combina
// sus probabilidades ponderando por número de árboles: el resultado es el
// promedio sobre TODOS los árboles del bosque distribuido.
func (s *Server) clusterPredict(st *modelState, qs []protocol.Query, timeout time.Duration) ([][dataset.NumClasses]float64, int, int, error) {
	s.mu.RLock()
	targets := make([]string, 0, len(st.Ready))
	for id, ok := range st.Ready {
		if ok {
			targets = append(targets, id)
		}
	}
	s.mu.RUnlock()
	if len(targets) == 0 {
		return nil, 0, 0, errNoReadyNodes
	}
	results, err := s.coord.Broadcast(targets, func(string) protocol.Message {
		return protocol.Message{Type: protocol.TypePredict, Queries: qs}
	}, timeout)
	if err != nil {
		return nil, 0, 0, errNoReadyNodes
	}
	sum := make([][dataset.NumClasses]float64, len(qs))
	used, trees := 0, 0
	for _, r := range results {
		if r.Err != nil {
			s.cfg.Logf("nodo %s no respondió: %v", r.NodeID, r.Err)
			continue
		}
		m := r.Msg
		if m.Type == protocol.TypeError {
			switch m.Code {
			case protocol.CodeUnknownDistrict:
				return nil, 0, 0, errUnknownDistrict
			case protocol.CodeNotTrained:
				s.markNotReady(st, r.NodeID)
			}
			s.cfg.Logf("nodo %s: %s (%s)", r.NodeID, m.Error, m.Code)
			continue
		}
		if m.Version != st.Version || len(m.Probs) != len(qs) || m.Trees == 0 {
			s.markNotReady(st, r.NodeID)
			continue
		}
		w := float64(m.Trees)
		for i, p := range m.Probs {
			for k := 0; k < dataset.NumClasses && k < len(p); k++ {
				sum[i][k] += w * p[k]
			}
		}
		used++
		trees += m.Trees
	}
	if used == 0 {
		return nil, 0, 0, errNoReadyNodes
	}
	for i := range sum {
		for k := range sum[i] {
			sum[i][k] /= float64(trees)
		}
	}
	return sum, used, trees, nil
}

func (s *Server) markNotReady(st *modelState, id string) {
	s.mu.Lock()
	if s.model == st {
		st.Ready[id] = false
	}
	s.mu.Unlock()
	go s.retrainNode(id, st)
}

// ---------------------------------------------------------------- handlers

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ids := s.coord.NodeIDs()
	h := HealthResponse{
		Status: "ok", Time: time.Now().UTC(), UptimeS: time.Since(s.started).Seconds(),
		NodesExpected: s.cfg.ExpectedNodes, NodesConnected: len(ids), Nodes: ids,
		Model: s.modelInfo(), Storage: s.store.Status(r.Context()),
	}
	if len(ids) < s.cfg.ExpectedNodes {
		h.Status = "degradado"
	}
	writeJSON(w, http.StatusOK, h)
}

func (s *Server) handleDistricts(w http.ResponseWriter, r *http.Request) {
	out := make([]DistrictInfo, len(s.cat.Districts))
	for i, d := range s.cat.Districts {
		out[i] = DistrictInfo{District: d, Ubigeo: s.cat.Ubigeos[i], Zone: dataset.ZoneNames[s.cat.Zones[i]], Denuncias: s.cat.Volume[i], Street: s.cat.Street[i]}
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(out), "districts": out})
}

func (s *Server) handleTrain(w http.ResponseWriter, r *http.Request) {
	if !s.trainMu.TryLock() {
		writeErr(w, http.StatusConflict, "TRAINING_IN_PROGRESS", "ya hay un entrenamiento en curso", "espere a que termine")
		return
	}
	defer s.trainMu.Unlock()

	var req TrainRequest
	if r.ContentLength != 0 {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "JSON inválido: "+err.Error(), "")
			return
		}
	}
	cfg := forest.DefaultConfig()
	trees, workers := 32, 0
	if msg := applyTrain(&req, &cfg, &trees, &workers); msg != "" {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", msg, "")
		return
	}
	ids := s.coord.NodeIDs()
	if len(ids) == 0 {
		writeErr(w, http.StatusServiceUnavailable, "NO_NODES", cluster.ErrNoNodes.Error(), "levante los servicios ml-node-*")
		return
	}
	if trees < len(ids) {
		trees = len(ids)
	}
	prev := s.current()
	version := int64(1)
	if prev != nil {
		version = prev.Version + 1
	}
	// Reparto de árboles: bloques contiguos de ids globales (32 -> 11, 11, 10).
	specs := map[string]protocol.TrainSpec{}
	next := 0
	for i, id := range ids {
		n := trees / len(ids)
		if i < trees%len(ids) {
			n++
		}
		tids := make([]int, n)
		for j := range tids {
			tids[j] = next
			next++
		}
		specs[id] = protocol.TrainSpec{Version: version, Shard: i, Shards: len(ids), TreeIDs: tids, Config: cfg, Workers: workers}
	}
	start := time.Now()
	results, err := s.coord.Broadcast(ids, func(id string) protocol.Message {
		sp := specs[id]
		return protocol.Message{Type: protocol.TypeTrain, Train: &sp}
	}, s.cfg.TrainTimeout)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "NO_NODES", err.Error(), "")
		return
	}
	job := TrainingJob{
		JobID: fmt.Sprintf("job-%d-v%d", time.Now().Unix(), version), Version: version, CreatedAt: time.Now().UTC(),
		DurationMs: msSince(start), NodeCount: len(ids), TotalTrees: trees, Config: cfg, Dataset: s.loadStats,
	}
	var imp [dataset.NumFeatures]float64
	for _, res := range results {
		if res.Err != nil || res.Msg.Type != protocol.TypeTrainOK || res.Msg.TrainResult == nil {
			msg := res.Msg.Error
			if res.Err != nil {
				msg = res.Err.Error()
			}
			writeErr(w, http.StatusBadGateway, "NODE_TRAIN_FAILED", fmt.Sprintf("el nodo %s no pudo entrenar: %s", res.NodeID, msg), "revise los logs del nodo")
			return
		}
		tr := *res.Msg.TrainResult
		job.Nodes = append(job.Nodes, NodeTrain{NodeID: res.NodeID, RoundTripMs: res.Ms, Result: tr})
		job.TrainRecords += tr.TrainRecords
		for f := range imp {
			imp[f] += tr.Importance[f]
		}
	}
	st := &modelState{Version: version, TrainedAt: time.Now().UTC(), Config: cfg, TotalTrees: trees, Assign: specs, Ready: map[string]bool{}}
	for _, id := range ids {
		st.Ready[id] = true
	}
	ev, err := s.evaluate(st, imp)
	if err != nil {
		job.EvalError = err.Error()
	} else {
		job.Evaluation = ev
		st.Accuracy = ev.Accuracy
	}
	job.Status = "completed"
	s.mu.Lock()
	s.model = st
	s.mu.Unlock()
	if b, err := json.Marshal(s.modelInfo()); err == nil {
		s.store.CacheSet(r.Context(), "pc4:model:current", b, 0)
	}
	s.persist(storage.CollTrainingJobs, job)
	writeJSON(w, http.StatusOK, job)
}

func applyTrain(req *TrainRequest, cfg *forest.Config, trees, workers *int) string {
	if req.Trees != nil {
		if *req.Trees < 3 || *req.Trees > 256 {
			return "trees debe estar entre 3 y 256"
		}
		*trees = *req.Trees
	}
	if req.MaxDepth != nil {
		if *req.MaxDepth < 1 || *req.MaxDepth > 20 {
			return "maxDepth debe estar entre 1 y 20"
		}
		cfg.MaxDepth = *req.MaxDepth
	}
	if req.MinLeaf != nil {
		if *req.MinLeaf < 1 || *req.MinLeaf > 100000 {
			return "minLeaf debe estar entre 1 y 100000"
		}
		cfg.MinLeaf = *req.MinLeaf
	}
	if req.Mtry != nil {
		if *req.Mtry < 1 || *req.Mtry > dataset.NumFeatures {
			return "mtry debe estar entre 1 y 5"
		}
		cfg.Mtry = *req.Mtry
	}
	if req.SampleFrac != nil {
		if *req.SampleFrac <= 0 || *req.SampleFrac > 1 {
			return "sampleFrac debe estar en (0, 1]"
		}
		cfg.SampleFrac = *req.SampleFrac
	}
	if req.Workers != nil {
		if *req.Workers < 0 || *req.Workers > 64 {
			return "workers debe estar entre 0 y 64 (0 = todos los núcleos del nodo)"
		}
		*workers = *req.Workers
	}
	return ""
}

// evaluate mide el bosque distribuido con la misma partición temporal de la
// PC3 (entrenamiento 2018–2025, prueba 2026). Las 164,221 denuncias de prueba
// se agrupan en vectores distintos y se consultan en un solo lote.
func (s *Server) evaluate(st *modelState, imp [dataset.NumFeatures]float64) (*Evaluation, error) {
	start := time.Now()
	qs := make([]protocol.Query, len(s.testCells))
	for i, c := range s.testCells {
		qs[i] = protocol.Query{District: s.cat.Districts[c.X[dataset.FDistrict]], Franja: int(c.X[dataset.FFranja]),
			Mes: int(c.X[dataset.FMonth]) + 1, Anio: int(c.X[dataset.FYear]) + dataset.BaseYear}
	}
	probs, used, trees, err := s.clusterPredict(st, qs, 60*time.Second)
	if err != nil {
		return nil, err
	}
	ev := &Evaluation{TestYear: dataset.TestYear, Queries: len(qs), NodesUsed: used, TreesUsed: trees}
	for i, c := range s.testCells {
		pred := argmax(probs[i][:], nil)
		for k := 0; k < dataset.NumClasses; k++ {
			ev.Confusion[k][pred] += c.Counts[k]
			ev.TestRecords += c.Counts[k]
		}
	}
	base := argmax(nil, s.trainTotals[:])
	ev.BaselineClass = dataset.ClassNames[base]
	var correct, baseHits int64
	for k := 0; k < dataset.NumClasses; k++ {
		var real, predicted int64
		for j := 0; j < dataset.NumClasses; j++ {
			real += ev.Confusion[k][j]
			predicted += ev.Confusion[j][k]
		}
		correct += ev.Confusion[k][k]
		if k == base {
			baseHits = real
		}
		cm := ClassMetric{Class: dataset.ClassNames[k], Real: real, Predicted: predicted}
		if predicted > 0 {
			cm.Precision = float64(ev.Confusion[k][k]) / float64(predicted)
		}
		if real > 0 {
			cm.Recall = float64(ev.Confusion[k][k]) / float64(real)
		}
		if cm.Precision+cm.Recall > 0 {
			cm.F1 = 2 * cm.Precision * cm.Recall / (cm.Precision + cm.Recall)
		}
		ev.MacroRecall += cm.Recall / dataset.NumClasses
		ev.MacroF1 += cm.F1 / dataset.NumClasses
		ev.PerClass = append(ev.PerClass, cm)
	}
	if ev.TestRecords > 0 {
		ev.Accuracy = float64(correct) / float64(ev.TestRecords)
		ev.Baseline = float64(baseHits) / float64(ev.TestRecords)
	}
	total := 0.0
	for _, v := range imp {
		total += v
	}
	for f, v := range imp {
		share := 0.0
		if total > 0 {
			share = v / total
		}
		ev.Importance = append(ev.Importance, FeatureImportance{Feature: dataset.FeatureNames[f], Importance: share})
	}
	ev.EvalMs = msSince(start)
	return ev, nil
}

func argmax(p []float64, c []int64) int {
	best := 0
	if p != nil {
		for i := 1; i < len(p); i++ {
			if p[i] > p[best] {
				best = i
			}
		}
		return best
	}
	for i := 1; i < len(c); i++ {
		if c[i] > c[best] {
			best = i
		}
	}
	return best
}

func topStreet(p *[dataset.NumClasses]float64) int {
	best := -1
	for k := 0; k < dataset.NumClasses; k++ {
		if dataset.StreetCrime[k] && (best < 0 || p[k] > p[best]) {
			best = k
		}
	}
	return best
}

func (s *Server) handlePredict(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req PredictRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "JSON inválido: "+err.Error(), `{"district":"SAN JUAN DE LURIGANCHO","franja":2,"mes":10,"anio":2026}`)
		return
	}
	if req.District == nil || req.Franja == nil || req.Mes == nil || req.Anio == nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "district, franja, mes y anio son obligatorios", "")
		return
	}
	franja, mes, anio := *req.Franja, *req.Mes, *req.Anio
	if franja < 0 || franja > 3 {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "franja debe estar entre 0 y 3 (0 madrugada, 1 mañana, 2 tarde, 3 noche)", "")
		return
	}
	if mes < 1 || mes > 12 {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "mes debe estar entre 1 y 12", "")
		return
	}
	if anio < dataset.BaseYear || anio > dataset.BaseYear+dataset.MaxYearIndex {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", fmt.Sprintf("anio debe estar entre %d y %d", dataset.BaseYear, dataset.BaseYear+dataset.MaxYearIndex), "")
		return
	}
	district := dataset.Normalize(*req.District)
	id, ok := s.cat.Index[district]
	if !ok {
		writeErr(w, http.StatusNotFound, "UNKNOWN_DISTRICT", "distrito no encontrado en Lima Metropolitana y Callao: "+*req.District, "consulte GET /api/v1/districts")
		return
	}
	st := s.current()
	if st == nil {
		writeErr(w, http.StatusConflict, "MODEL_NOT_TRAINED", "el modelo aún no ha sido entrenado", "ejecute POST /api/v1/train")
		return
	}
	s.nPredict.Add(1)
	key := fmt.Sprintf("pc4:pred:v%d:%s:%d:%d:%d", st.Version, district, franja, mes, anio)
	if b, ok := s.store.CacheGet(r.Context(), key); ok {
		var resp PredictResponse
		if json.Unmarshal(b, &resp) == nil {
			s.nHits.Add(1)
			resp.FromCache = true
			resp.LatencyMs = msSince(start)
			s.lat.add("predict_cache", resp.LatencyMs)
			s.persist(storage.CollPredictions, predictionDoc(&resp))
			writeJSON(w, http.StatusOK, resp)
			return
		}
	}
	s.nMiss.Add(1)
	t0 := time.Now()
	probs, used, trees, err := s.clusterPredict(st, []protocol.Query{{District: district, Franja: franja, Mes: mes, Anio: anio}}, s.cfg.PredictTimeout)
	clusterMs := msSince(t0)
	if err != nil {
		code, status := "CLUSTER_UNAVAILABLE", http.StatusServiceUnavailable
		if errors.Is(err, errUnknownDistrict) {
			code, status = "UNKNOWN_DISTRICT", http.StatusNotFound
		}
		writeErr(w, status, code, err.Error(), "")
		return
	}
	p := probs[0]
	resp := PredictResponse{
		District: district, Ubigeo: s.cat.Ubigeos[id], Zone: dataset.ZoneNames[s.cat.Zones[id]], Franja: franja,
		FranjaLabel: dataset.FranjaNames[franja], Mes: mes, Anio: anio, ModelVersion: st.Version,
		NodesUsed: used, TreesUsed: trees, ClusterMs: clusterMs,
	}
	for k := 0; k < dataset.NumClasses; k++ {
		resp.Probabilities = append(resp.Probabilities, ClassProb{Class: dataset.ClassNames[k], Probability: p[k]})
	}
	top := argmax(p[:], nil)
	street := topStreet(&p)
	resp.TopClass, resp.TopProbability = dataset.ClassNames[top], p[top]
	resp.TopStreetCrime, resp.TopStreetProbability = dataset.ClassNames[street], p[street]
	if b, err := json.Marshal(resp); err == nil {
		s.store.CacheSet(r.Context(), key, b, s.cfg.CacheTTL)
	}
	resp.LatencyMs = msSince(start)
	s.lat.add("predict_cluster", resp.LatencyMs)
	s.persist(storage.CollPredictions, predictionDoc(&resp))
	writeJSON(w, http.StatusOK, resp)
}

func predictionDoc(p *PredictResponse) PredictionDoc {
	return PredictionDoc{
		CreatedAt: time.Now().UTC(), District: p.District, Zone: p.Zone, Franja: p.Franja, Mes: p.Mes, Anio: p.Anio,
		Probabilities: p.Probabilities, TopClass: p.TopClass, TopProbability: p.TopProbability,
		TopStreetCrime: p.TopStreetCrime, ModelVersion: p.ModelVersion, NodesUsed: p.NodesUsed,
		TreesUsed: p.TreesUsed, FromCache: p.FromCache, LatencyMs: p.LatencyMs,
	}
}

// handlePatrol responde la pregunta de impacto: para cada distrito y franja,
// el delito de calle más probable (matriz de patrullaje de la PC3, ahora
// calculada por el clúster y guardada en Redis como recomendación precalculada).
func (s *Server) handlePatrol(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	q := r.URL.Query()
	mes, err1 := strconv.Atoi(q.Get("mes"))
	anio, err2 := strconv.Atoi(q.Get("anio"))
	if err1 != nil || err2 != nil || mes < 1 || mes > 12 || anio < dataset.BaseYear || anio > dataset.BaseYear+dataset.MaxYearIndex {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "parámetros mes (1-12) y anio (2018-2033) obligatorios", "/api/v1/patrullaje?mes=10&anio=2026&top=10")
		return
	}
	top := len(s.cat.Districts)
	if t := q.Get("top"); t != "" {
		n, err := strconv.Atoi(t)
		if err != nil || n < 1 {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "top debe ser un entero positivo", "")
			return
		}
		top = min(n, top)
	}
	st := s.current()
	if st == nil {
		writeErr(w, http.StatusConflict, "MODEL_NOT_TRAINED", "el modelo aún no ha sido entrenado", "ejecute POST /api/v1/train")
		return
	}
	s.nPatrol.Add(1)
	key := fmt.Sprintf("pc4:patrol:v%d:%d:%d", st.Version, mes, anio)
	var resp PatrolResponse
	if b, ok := s.store.CacheGet(r.Context(), key); ok && json.Unmarshal(b, &resp) == nil {
		resp.FromCache = true
	} else {
		qs := make([]protocol.Query, 0, len(s.cat.Districts)*dataset.NumFranjas)
		for _, d := range s.cat.Districts {
			for f := 0; f < dataset.NumFranjas; f++ {
				qs = append(qs, protocol.Query{District: d, Franja: f, Mes: mes, Anio: anio})
			}
		}
		t0 := time.Now()
		probs, used, trees, err := s.clusterPredict(st, qs, 10*time.Second)
		if err != nil {
			writeErr(w, http.StatusServiceUnavailable, "CLUSTER_UNAVAILABLE", err.Error(), "")
			return
		}
		resp = PatrolResponse{Mes: mes, Anio: anio, ModelVersion: st.Version, NodesUsed: used, TreesUsed: trees,
			Queries: len(qs), Districts: len(s.cat.Districts), ClusterMs: msSince(t0), CreatedAt: time.Now().UTC(),
			Note: "La franja horaria es imputada (el CSV no trae la hora del hecho); el distrito, el mes y el año sí son datos reales."}
		for i, d := range s.cat.Districts {
			row := PatrolRow{District: d, Zone: dataset.ZoneNames[s.cat.Zones[i]], HistoricalStreet: s.cat.Street[i]}
			for f := 0; f < dataset.NumFranjas; f++ {
				p := probs[i*dataset.NumFranjas+f]
				sc, tc := topStreet(&p), argmax(p[:], nil)
				pf := PatrolFranja{Franja: f, Label: dataset.FranjaNames[f], StreetCrime: dataset.ClassNames[sc], Probability: p[sc],
					TopClass: dataset.ClassNames[tc], TopProbability: p[tc]}
				row.Franjas = append(row.Franjas, pf)
				if pf.Probability > row.PriorityProb {
					row.PriorityProb, row.PriorityFranja, row.PriorityCrime = pf.Probability, pf.Label, pf.StreetCrime
				}
			}
			resp.Rows = append(resp.Rows, row)
		}
		sort.SliceStable(resp.Rows, func(a, b int) bool { return resp.Rows[a].HistoricalStreet > resp.Rows[b].HistoricalStreet })
		for i := range resp.Rows {
			resp.Rows[i].Rank = i + 1
		}
		if b, err := json.Marshal(resp); err == nil {
			s.store.CacheSet(r.Context(), key, b, s.cfg.CacheTTL)
		}
		s.persist(storage.CollPatrol, resp)
	}
	resp.LatencyMs = msSince(start)
	if resp.FromCache {
		s.lat.add("patrol_cache", resp.LatencyMs)
	} else {
		s.lat.add("patrol_cluster", resp.LatencyMs)
	}
	resp.Rows = resp.Rows[:top]
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.metrics(r.Context()))
}

func (s *Server) metrics(ctx context.Context) MetricsResponse {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	hits, miss := s.nHits.Load(), s.nMiss.Load()
	m := MetricsResponse{
		Timestamp: time.Now().UTC(), UptimeS: time.Since(s.started).Seconds(),
		Coordinator: map[string]any{"goroutines": runtime.NumGoroutine(), "heapMB": float64(ms.HeapAlloc) / (1 << 20),
			"numCPU": runtime.NumCPU(), "persistQueue": len(s.persistCh), "persistDropped": s.dropped.Load()},
		Nodes: s.coord.Nodes(), Model: s.modelInfo(),
		Requests: map[string]int64{"predict": s.nPredict.Load(), "predictCacheHits": hits, "predictClusterCalls": miss,
			"patrullaje": s.nPatrol.Load(), "httpErrors": s.nErrors.Load()},
		Latency: s.lat.summary(), Storage: s.store.Status(ctx),
	}
	if hits+miss > 0 {
		m.CacheHitPct = 100 * float64(hits) / float64(hits+miss)
	}
	return m
}

// ---------------------------------------------------------------- latencias

type latencies struct {
	mu   sync.Mutex
	data map[string][]float64
}

func newLatencies() *latencies { return &latencies{data: map[string][]float64{}} }

func (l *latencies) add(name string, ms float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v := append(l.data[name], ms)
	if len(v) > 2000 {
		v = v[len(v)-2000:]
	}
	l.data[name] = v
}

func (l *latencies) summary() map[string]LatencySummary {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]LatencySummary{}
	for name, v := range l.data {
		if len(v) == 0 {
			continue
		}
		c := append([]float64(nil), v...)
		sort.Float64s(c)
		sum, under := 0.0, 0
		for _, x := range c {
			sum += x
			if x < 100 {
				under++
			}
		}
		pct := func(p float64) float64 { return c[int(p*float64(len(c)-1)+0.5)] }
		out[name] = LatencySummary{Count: len(c), AvgMs: sum / float64(len(c)), P50Ms: pct(0.50), P95Ms: pct(0.95),
			P99Ms: pct(0.99), MaxMs: c[len(c)-1], Under100: 100 * float64(under) / float64(len(c))}
	}
	return out
}
