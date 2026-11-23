package worker

import (
	"os"
	"testing"

	"github.com/magic-tps/pc4-grupo3/internal/forest"
	"github.com/magic-tps/pc4-grupo3/internal/protocol"
)

const csvPath = "../../data/DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"

// Sin modelo, el nodo responde NOT_TRAINED en lugar de inventar probabilidades.
func TestNotTrainedAndUnknownDistrict(t *testing.T) {
	w := New("n1", "", csvPath, 0)
	q := []protocol.Query{{District: "LIMA", Franja: 1, Mes: 1, Anio: 2026}}
	if r := w.Handle(protocol.Message{Type: protocol.TypePredict, Queries: q}); r.Code != protocol.CodeNotTrained {
		t.Fatalf("se esperaba NOT_TRAINED, se obtuvo %+v", r)
	}
	if _, err := os.Stat(csvPath); err != nil {
		t.Skip("CSV no disponible")
	}
	spec := protocol.TrainSpec{Version: 1, Shard: 0, Shards: 3, TreeIDs: []int{0, 1, 2}, Config: forest.DefaultConfig()}
	if r := w.Handle(protocol.Message{Type: protocol.TypeTrain, Train: &spec}); r.Type != protocol.TypeTrainOK || r.TrainResult.Trees != 3 {
		t.Fatalf("entrenamiento: %+v", r)
	}
	r := w.Handle(protocol.Message{Type: protocol.TypePredict, Queries: q})
	if r.Type != protocol.TypePredictOK || len(r.Probs) != 1 || r.Version != 1 {
		t.Fatalf("predicción: %+v", r)
	}
	bad := []protocol.Query{{District: "NARNIA", Franja: 1, Mes: 1, Anio: 2026}}
	if r := w.Handle(protocol.Message{Type: protocol.TypePredict, Queries: bad}); r.Code != protocol.CodeUnknownDistrict {
		t.Fatalf("distrito inexistente: %+v", r)
	}
	if p := w.Handle(protocol.Message{Type: protocol.TypePing}); p.Stats == nil || !p.Stats.Trained || p.Stats.Trees != 3 {
		t.Fatalf("stats: %+v", p.Stats)
	}
}
