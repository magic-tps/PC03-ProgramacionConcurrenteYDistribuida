// Nodo ML del clúster: entrena su parte del Random Forest distribuido.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/magic-tps/pc4-grupo3/internal/worker"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	id := flag.String("id", env("NODE_ID", "ml-node-1"), "identificador del nodo")
	coord := flag.String("coordinator", env("COORDINATOR_ADDR", "127.0.0.1:9090"), "dirección TCP del coordinador")
	data := flag.String("data", env("DATA_PATH", "data/DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"), "CSV de denuncias")
	defWorkers, _ := strconv.Atoi(env("WORKERS", "0"))
	workers := flag.Int("workers", defWorkers, "goroutines para cargar y entrenar (0 = todos los núcleos)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("nodo %s iniciando; coordinador %s; dataset %s", *id, *coord, *data)
	worker.New(*id, *coord, *data, *workers).RunForever(ctx)
	log.Printf("nodo %s detenido", *id)
}
