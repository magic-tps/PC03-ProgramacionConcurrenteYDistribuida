package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/magic-tps/pc4-grupo3/internal/cluster"
	"github.com/magic-tps/pc4-grupo3/internal/dataset"
	"github.com/magic-tps/pc4-grupo3/internal/forest"
	"github.com/magic-tps/pc4-grupo3/internal/storage"
	"github.com/magic-tps/pc4-grupo3/internal/worker"
)

const csvPath = "../../data/DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"

type env struct {
	t      *testing.T
	http   *httptest.Server
	srv    *Server
	coord  *cluster.Coordinator
	store  *storage.Memory
	cancel map[string]context.CancelFunc
	addr   string
}

func setup(t *testing.T) *env {
	if _, err := os.Stat(csvPath); err != nil {
		t.Skip("CSV no disponible")
	}
	tab, err := dataset.Load(csvPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	cat, _ := dataset.BuildCatalog(tab)
	coord := cluster.New()
	coord.Logf = func(string, ...any) {}
	a, err := coord.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	store := storage.NewMemory()
	srv := New(Config{OpenAPISpec: []byte("openapi: 3.0.3"), Logf: func(string, ...any) {}, PredictTimeout: 5 * time.Second}, coord, store, tab, cat)
	e := &env{t: t, srv: srv, coord: coord, store: store, cancel: map[string]context.CancelFunc{}, addr: a.String()}
	e.http = httptest.NewServer(srv.Handler())
	for i := 1; i <= 3; i++ {
		e.startNode(fmt.Sprintf("ml-node-%d", i))
	}
	e.waitNodes(3)
	t.Cleanup(func() {
		for _, c := range e.cancel {
			c()
		}
		e.http.Close()
		srv.Close()
		coord.Close()
	})
	return e
}

// startNode arranca un nodo nuevo (proceso "limpio", sin modelo).
func (e *env) startNode(id string) {
	ctx, cancel := context.WithCancel(context.Background())
	w := worker.New(id, e.addr, csvPath, 0)
	w.Logf = func(string, ...any) {}
	go w.RunForever(ctx)
	e.cancel[id] = cancel
}

func (e *env) waitNodes(n int) {
	deadline := time.Now().Add(5 * time.Second)
	for len(e.coord.NodeIDs()) != n {
		if time.Now().After(deadline) {
			e.t.Fatalf("se esperaban %d nodos, hay %d", n, len(e.coord.NodeIDs()))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *env) do(method, path string, body any, out any) (int, float64) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.http.URL+path, r)
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	ms := float64(time.Since(start).Microseconds()) / 1000
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			e.t.Fatalf("%s %s: respuesta no JSON (%d): %s", method, path, resp.StatusCode, b)
		}
	}
	if resp.Header.Get("X-Response-Time-Ms") == "" {
		e.t.Errorf("%s %s sin cabecera X-Response-Time-Ms", method, path)
	}
	return resp.StatusCode, ms
}

func q(d string, f, m, y int) map[string]any {
	return map[string]any{"district": d, "franja": f, "mes": m, "anio": y}
}

func TestEndToEnd(t *testing.T) {
	e := setup(t)

	// 1. Antes de entrenar no se inventan probabilidades.
	var er ErrorResponse
	if code, _ := e.do("POST", "/api/v1/predict", q("SAN JUAN DE LURIGANCHO", 2, 10, 2026), &er); code != 409 || er.Code != "MODEL_NOT_TRAINED" {
		t.Fatalf("antes de entrenar: %d %+v", code, er)
	}

	// 2. Entrenamiento distribuido.
	var job TrainingJob
	code, ms := e.do("POST", "/api/v1/train", map[string]any{}, &job)
	if code != 200 || job.NodeCount != 3 || job.TotalTrees != 32 || job.TrainRecords != 1823335 {
		t.Fatalf("train: %d %+v", code, job)
	}
	ev := job.Evaluation
	if ev == nil || ev.TestRecords != 164221 || ev.Accuracy <= ev.Baseline || ev.TreesUsed != 32 {
		t.Fatalf("evaluación: %+v (%s)", ev, job.EvalError)
	}
	t.Logf("train %.0f ms; exactitud %.2f%% vs base %.2f%%; árboles por nodo: %d/%d/%d", ms, 100*ev.Accuracy, 100*ev.Baseline,
		job.Nodes[0].Result.Trees, job.Nodes[1].Result.Trees, job.Nodes[2].Result.Trees)

	// 3. Predicción del clúster y luego desde caché.
	var p1, p2 PredictResponse
	if code, _ := e.do("POST", "/api/v1/predict", q("san juan de lurigancho", 2, 10, 2026), &p1); code != 200 || p1.FromCache || p1.NodesUsed != 3 || p1.TreesUsed != 32 {
		t.Fatalf("predict: %d %+v", code, p1)
	}
	sum := 0.0
	for _, c := range p1.Probabilities {
		sum += c.Probability
	}
	if sum < 0.999 || sum > 1.001 || p1.Probabilities[4].Probability <= 0 {
		t.Fatalf("probabilidades inválidas o extorsión en 0: %+v", p1.Probabilities)
	}
	if code, _ := e.do("POST", "/api/v1/predict", q("SAN JUAN DE LURIGANCHO", 2, 10, 2026), &p2); code != 200 || !p2.FromCache || p2.TopClass != p1.TopClass {
		t.Fatalf("caché: %d %+v", code, p2)
	}
	t.Logf("SJL tarde 10/2026: %s %.3f (calle: %s %.3f); clúster %.3f ms, caché %.3f ms", p1.TopClass, p1.TopProbability, p1.TopStreetCrime, p1.TopStreetProbability, p1.LatencyMs, p2.LatencyMs)

	// 4. El mes y el año SÍ cambian la predicción (antes se ignoraban).
	var p3 PredictResponse
	e.do("POST", "/api/v1/predict", q("SAN JUAN DE LURIGANCHO", 2, 7, 2020), &p3)
	if p3.Probabilities[0].Probability == p1.Probabilities[0].Probability {
		t.Fatalf("mes/año no influyen en la predicción")
	}

	// 5. Validaciones.
	cases := []struct {
		body any
		code int
	}{
		{q("DISTRITO INVENTADO", 1, 1, 2026), 404},
		{q("MIRAFLORES", 7, 1, 2026), 400},
		{q("MIRAFLORES", 1, 13, 2026), 400},
		{q("MIRAFLORES", 1, 1, 1990), 400},
		{map[string]any{"district": "MIRAFLORES"}, 400},
		{map[string]any{"district": "MIRAFLORES", "franja": 1, "mes": 1, "anio": 2026, "extra": 1}, 400},
	}
	for _, c := range cases {
		if code, _ := e.do("POST", "/api/v1/predict", c.body, &er); code != c.code {
			t.Errorf("%v: se esperaba %d, se obtuvo %d", c.body, c.code, code)
		}
	}
	if code, _ := e.do("GET", "/api/v1/predict", nil, nil); code != 405 {
		t.Errorf("GET /predict debe ser 405, fue %d", code)
	}

	// 6. Matriz de patrullaje (pregunta de impacto social).
	var pr, pr2 PatrolResponse
	if code, _ := e.do("GET", "/api/v1/patrullaje?mes=10&anio=2026&top=10", nil, &pr); code != 200 || len(pr.Rows) != 10 || pr.Queries != 200 || pr.FromCache {
		t.Fatalf("patrullaje: %d %+v", code, pr)
	}
	e.do("GET", "/api/v1/patrullaje?mes=10&anio=2026&top=10", nil, &pr2)
	if !pr2.FromCache {
		t.Fatal("la matriz debe salir de caché la segunda vez")
	}
	t.Logf("patrullaje #1 %s: prioridad %s %s %.3f (clúster %.1f ms)", pr.Rows[0].District, pr.Rows[0].PriorityFranja, pr.Rows[0].PriorityCrime, pr.Rows[0].PriorityProb, pr.LatencyMs)

	// 7. Latencia: 40 predicciones distintas (sin caché) por debajo de 100 ms.
	worst := 0.0
	for i := 0; i < 40; i++ {
		var p PredictResponse
		d := []string{"LIMA", "COMAS", "ATE", "CALLAO", "MIRAFLORES"}[i%5]
		e.do("POST", "/api/v1/predict", q(d, i%4, 1+i%12, 2025), &p)
		if p.FromCache {
			continue
		}
		if p.LatencyMs > worst {
			worst = p.LatencyMs
		}
	}
	if worst >= 100 {
		t.Fatalf("latencia del clúster %.1f ms >= 100 ms", worst)
	}
	t.Logf("peor latencia del clúster en 40 consultas: %.3f ms", worst)

	// 8. Tolerancia a fallos: cae un nodo, se sigue respondiendo con 2.
	e.cancel["ml-node-2"]()
	e.waitNodes(2)
	var p4 PredictResponse
	if code, _ := e.do("POST", "/api/v1/predict", q("COMAS", 0, 3, 2026), &p4); code != 200 || p4.NodesUsed != 2 || p4.TreesUsed != 21 {
		t.Fatalf("con un nodo caído: %d %+v", code, p4)
	}
	// 9. Vuelve como proceso nuevo (sin modelo): el coordinador lo reentrena solo.
	e.startNode("ml-node-2")
	e.waitNodes(3)
	deadline := time.Now().Add(20 * time.Second)
	for len(e.srv.modelInfo().NodesReady) != 3 {
		if time.Now().After(deadline) {
			t.Fatal("el nodo reincorporado no fue reentrenado")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var p5 PredictResponse
	if code, _ := e.do("POST", "/api/v1/predict", q("COMAS", 1, 3, 2026), &p5); code != 200 || p5.NodesUsed != 3 || p5.TreesUsed != 32 {
		t.Fatalf("tras reincorporar: %d %+v", code, p5)
	}

	// 10. Métricas y persistencia.
	var m MetricsResponse
	e.do("GET", "/api/v1/metrics", nil, &m)
	if len(m.Nodes) != 3 || m.Latency["predict_cluster"].Count == 0 || m.Requests["predictCacheHits"] == 0 {
		t.Fatalf("métricas incompletas: %+v", m)
	}
	time.Sleep(100 * time.Millisecond)
	st := e.store.Status(context.Background())
	if st.Collections[storage.CollTrainingJobs] != 1 || st.Collections[storage.CollPredictions] < 40 || st.Collections[storage.CollPatrol] != 1 {
		t.Fatalf("persistencia: %+v", st.Collections)
	}
	var h HealthResponse
	e.do("GET", "/api/v1/health", nil, &h)
	if h.Status != "ok" || !h.Model.Trained || len(h.Model.NodesReady) != 3 {
		t.Fatalf("health: %+v", h)
	}
}

// El bosque distribuido debe dar EXACTAMENTE las mismas probabilidades que
// los mismos 32 árboles entrenados en un solo proceso (equivalencia).
func TestDistributedEqualsLocal(t *testing.T) {
	e := setup(t)
	var job TrainingJob
	e.do("POST", "/api/v1/train", nil, &job)
	tab, _ := dataset.Load(csvPath, 0)
	cat, _ := dataset.BuildCatalog(tab)
	var local []*forest.Model
	for _, n := range job.Nodes {
		m := dataset.Expand(tab, cat, n.Result.Shard, n.Result.Shards, func(y int) bool { return y < dataset.TestYear })
		local = append(local, forest.Train(m, cat.Cardinality(), n.Result.TreeIDs, forest.DefaultConfig(), 1))
	}
	for _, d := range []string{"SAN JUAN DE LURIGANCHO", "LIMA", "VENTANILLA", "BREÑA"} {
		var p PredictResponse
		e.do("POST", "/api/v1/predict", q(d, 3, 9, 2026), &p)
		x, _ := cat.Features(d, 3, 9, 2026)
		var want [6]float64
		for _, m := range local {
			pp := m.Predict(&x)
			for k := range want {
				want[k] += pp[k] * float64(len(m.Trees)) / 32
			}
		}
		for k := range want {
			if diff := want[k] - p.Probabilities[k].Probability; diff > 1e-9 || diff < -1e-9 {
				t.Fatalf("%s clase %d: distribuido %.12f vs local %.12f", d, k, p.Probabilities[k].Probability, want[k])
			}
		}
	}
}
