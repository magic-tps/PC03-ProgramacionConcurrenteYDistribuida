package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/magic-tps/pc03/pc4/internal/cluster"
	"github.com/magic-tps/pc03/pc4/internal/storage"
)

type predictRequest struct {
	District string `json:"district"`
	Franja   int    `json:"franja"`
	Mes      int    `json:"mes"`
	Anio     int    `json:"anio"`
}

type predictResponse struct {
	Predictions [6]float64 `json:"probabilities"`
	TopClass    string     `json:"topClass"`
	TopProb     float64    `json:"topProbability"`
	LatencyMs   float64    `json:"latencyMs"`
	FromCache   bool       `json:"fromCache"`
	NodesUsed   int        `json:"nodesUsed"`
}

var classes = []string{"Violencia mujer/IGF", "Hurto", "Robo", "Estafa", "Extorsión", "Secuestro"}

func main() {
	httpAddress := flag.String("http", ":8080", "dirección HTTP")
	tcpAddress := flag.String("tcp", ":9090", "dirección TCP interna")
	dataPath := flag.String("csv", "DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv", "dataset")
	flag.Parse()

	ctx := context.Background()
	store := storage.Open(ctx)
	defer store.Close()
	coordinator := cluster.NewCoordinator()
	if err := coordinator.Listen(*tcpAddress); err != nil {
		log.Fatalf("TCP: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/swagger.yaml", http.StripPrefix("/", http.FileServer(http.Dir("pc4/openapi"))))
	mux.HandleFunc("/swagger", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<!doctype html><html><head><link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css"></head><body><div id="swagger-ui"></div><script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script><script>SwaggerUIBundle({url:'/swagger.yaml',dom_id:'#swagger-ui'})</script></body></html>`)
	})
	mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "nodes": coordinator.Nodes(), "storage": store.Status(r.Context())})
	})
	mux.HandleFunc("/api/v1/train", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())
		start := time.Now()
		responses, err := coordinator.Send(cluster.Request{Type: "TRAIN", JobID: jobID, DataPath: *dataPath}, 120*time.Second)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
			return
		}
		var rows int64
		for _, response := range responses {
			rows += response.Rows
			if response.Error != "" {
				writeJSON(w, http.StatusBadGateway, map[string]any{"error": response.Error, "node": response.NodeID})
				return
			}
		}
		duration := float64(time.Since(start).Microseconds()) / 1000
		store.SaveJob(r.Context(), jobID, rows, duration, len(responses))
		writeJSON(w, http.StatusAccepted, map[string]any{"jobId": jobID, "rows": rows, "nodes": len(responses), "durationMs": duration, "status": "completed"})
	})
	mux.HandleFunc("/api/v1/predict", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		start := time.Now()
		var in predictRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "JSON inválido"})
			return
		}
		in.District = strings.ToUpper(strings.Join(strings.Fields(in.District), " "))
		if in.District == "" || in.Franja < 0 || in.Franja > 3 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "district es obligatorio y franja debe estar entre 0 y 3"})
			return
		}
		if prob, ok := store.GetPrediction(r.Context(), in.District, in.Franja, in.Mes, in.Anio); ok {
			top, topProb := argmax(prob)
			elapsed := float64(time.Since(start).Microseconds()) / 1000
			writeJSON(w, http.StatusOK, predictResponse{Predictions: six(prob), TopClass: classes[top], TopProb: topProb, LatencyMs: elapsed, FromCache: true, NodesUsed: len(coordinator.Nodes())})
			return
		}
		responses, err := coordinator.Send(cluster.Request{Type: "PREDICT", District: in.District, Franja: in.Franja, Mes: in.Mes, Anio: in.Anio}, 3*time.Second)
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
			return
		}
		average := [6]float64{}
		valid := 0
		for _, response := range responses {
			if len(response.Prob) == 6 {
				valid++
				for i := range average {
					average[i] += response.Prob[i]
				}
			}
		}
		if valid == 0 {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": "los nodos no devolvieron probabilidades"})
			return
		}
		for i := range average {
			average[i] /= float64(valid)
		}
		top, topProb := argmax(average[:])
		elapsed := float64(time.Since(start).Microseconds()) / 1000
		store.SavePrediction(r.Context(), in.District, in.Franja, in.Mes, in.Anio, average[:], elapsed)
		writeJSON(w, http.StatusOK, predictResponse{Predictions: average, TopClass: classes[top], TopProb: topProb, LatencyMs: elapsed, FromCache: false, NodesUsed: valid})
	})
	mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		nodes := coordinator.Nodes()
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
		writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes, "storage": store.Status(r.Context()), "timestamp": time.Now().UTC()})
	})

	handler := latencyHeader(mux)
	log.Printf("API HTTP en %s; TCP coordinador en %s", *httpAddress, *tcpAddress)
	server := &http.Server{Addr: *httpAddress, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	_ = os.Stdout
}

func latencyHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		ms := float64(time.Since(start).Microseconds()) / 1000
		w.Header().Set("X-Response-Time-Ms", fmt.Sprintf("%.3f", ms))
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func argmax(prob []float64) (int, float64) {
	best := 0
	for i := 1; i < len(prob); i++ {
		if prob[i] > prob[best] {
			best = i
		}
	}
	return best, prob[best]
}

func six(prob []float64) [6]float64 {
	var out [6]float64
	copy(out[:], prob)
	return out
}
