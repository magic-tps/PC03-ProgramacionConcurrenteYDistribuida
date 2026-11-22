package main

import (
	"flag"
	"log"
	"strconv"
	"time"

	pcnode "github.com/magic-tps/pc03/pc4/internal/node"
)

func main() {
	id := flag.String("id", "ml-node-1", "identificador del nodo")
	coordinator := flag.String("coordinator", "127.0.0.1:9090", "dirección TCP del coordinador")
	csvPath := flag.String("csv", "../../DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv", "dataset")
	flag.Parse()
	workerNo := 0
	if len(*id) > 0 {
		if n, err := strconv.Atoi((*id)[len(*id)-1:]); err == nil {
			workerNo = (n - 1) % 3
		}
	}
	server := pcnode.NewServer(*id, *coordinator, *csvPath, workerNo)
	log.Printf("nodo %s conectado a %s", *id, *coordinator)
	for {
		if err := server.Run(); err != nil {
			log.Printf("conexión perdida: %v; reintentando...", err)
			time.Sleep(time.Second)
		}
	}
}
