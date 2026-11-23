// Coordinador: API REST (HTTP :8080) + servidor TCP del clúster (:9090).
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/magic-tps/pc4-grupo3/internal/api"
	"github.com/magic-tps/pc4-grupo3/internal/cluster"
	"github.com/magic-tps/pc4-grupo3/internal/dataset"
	"github.com/magic-tps/pc4-grupo3/internal/storage"
	"github.com/magic-tps/pc4-grupo3/internal/storage/dbstore"
	"github.com/magic-tps/pc4-grupo3/openapi"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func duration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return def
}

func main() {
	httpAddr := flag.String("http", env("HTTP_ADDR", ":8080"), "dirección HTTP de la API")
	tcpAddr := flag.String("tcp", env("TCP_ADDR", ":9090"), "dirección TCP para los nodos")
	data := flag.String("data", env("DATA_PATH", "data/DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"), "CSV de denuncias")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// 1. Dataset: catálogo de distritos y conjunto de prueba (2026) para evaluar el bosque.
	t, err := dataset.Load(*data, 0)
	if err != nil {
		log.Fatalf("no se pudo leer el dataset %s: %v", *data, err)
	}
	cat, err := dataset.BuildCatalog(t)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("dataset: %d filas leídas, %d válidas, %d denuncias clasificables, %d distritos (%.1f ms, %d workers)",
		t.Stats.RowsRead, t.Stats.RowsValid, t.Stats.DenunciasValidas, len(cat.Districts), t.Stats.LoadMs, t.Stats.Workers)

	// 2. Persistencia: MongoDB + Redis (o memoria si no están configurados).
	var store storage.Store
	if os.Getenv("MONGO_URI") == "" && os.Getenv("REDIS_ADDR") == "" {
		log.Printf("MONGO_URI y REDIS_ADDR vacíos: persistencia en memoria")
		store = storage.NewMemory()
	} else {
		store = dbstore.Open(dbstore.Config{MongoURI: os.Getenv("MONGO_URI"), MongoDB: env("MONGO_DB", "pc4_denuncias"),
			RedisAddr: os.Getenv("REDIS_ADDR")}, log.Printf)
	}
	defer store.Close()

	// 3. Clúster TCP (topología estrella).
	coord := cluster.New()
	addr, err := coord.Listen(*tcpAddr)
	if err != nil {
		log.Fatalf("TCP: %v", err)
	}
	log.Printf("servidor TCP del clúster en %s", addr)
	defer coord.Close()

	expected, _ := strconv.Atoi(env("EXPECTED_NODES", "3"))
	srv := api.New(api.Config{
		CacheTTL: duration("CACHE_TTL", 10*time.Minute), TrainTimeout: duration("TRAIN_TIMEOUT", 2*time.Minute),
		PredictTimeout: duration("PREDICT_TIMEOUT", 3*time.Second), MetricsInterval: duration("METRICS_INTERVAL", 30*time.Second),
		ExpectedNodes: expected, OpenAPISpec: openapi.Spec, Logf: log.Printf,
	}, coord, store, t, cat)
	defer srv.Close()

	stop := make(chan struct{})
	coord.StartHeartbeat(duration("HEARTBEAT", 3*time.Second), stop)
	defer close(stop)

	httpSrv := &http.Server{Addr: *httpAddr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 3 * time.Minute, IdleTimeout: 2 * time.Minute}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		sh, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = httpSrv.Shutdown(sh)
	}()
	log.Printf("API REST en %s (Swagger UI en /swagger)", *httpAddr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	log.Printf("coordinador detenido")
}
