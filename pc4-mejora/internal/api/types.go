package api

import (
	"time"

	"github.com/magic-tps/pc4-grupo3/internal/cluster"
	"github.com/magic-tps/pc4-grupo3/internal/dataset"
	"github.com/magic-tps/pc4-grupo3/internal/forest"
	"github.com/magic-tps/pc4-grupo3/internal/protocol"
	"github.com/magic-tps/pc4-grupo3/internal/storage"
)

// ErrorResponse es el cuerpo de todos los errores de la API.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
	Hint  string `json:"hint,omitempty"`
}

// TrainRequest (todos los campos son opcionales; por defecto = PC3).
type TrainRequest struct {
	Trees      *int     `json:"trees,omitempty"`
	MaxDepth   *int     `json:"maxDepth,omitempty"`
	MinLeaf    *int     `json:"minLeaf,omitempty"`
	Mtry       *int     `json:"mtry,omitempty"`
	SampleFrac *float64 `json:"sampleFrac,omitempty"`
	Workers    *int     `json:"workers,omitempty"`
}

type NodeTrain struct {
	NodeID      string               `json:"nodeId" bson:"nodeId"`
	RoundTripMs float64              `json:"roundTripMs" bson:"roundTripMs"`
	Result      protocol.TrainResult `json:"result" bson:"result"`
}

type ClassMetric struct {
	Class     string  `json:"class" bson:"class"`
	Real      int64   `json:"real" bson:"real"`
	Predicted int64   `json:"predicted" bson:"predicted"`
	Precision float64 `json:"precision" bson:"precision"`
	Recall    float64 `json:"recall" bson:"recall"`
	F1        float64 `json:"f1" bson:"f1"`
}

type FeatureImportance struct {
	Feature    string  `json:"feature" bson:"feature"`
	Importance float64 `json:"importance" bson:"importance"`
}

type Evaluation struct {
	TestYear      int                 `json:"testYear" bson:"testYear"`
	TestRecords   int64               `json:"testRecords" bson:"testRecords"`
	Queries       int                 `json:"distinctQueries" bson:"distinctQueries"`
	Accuracy      float64             `json:"accuracy" bson:"accuracy"`
	Baseline      float64             `json:"baselineAccuracy" bson:"baselineAccuracy"`
	BaselineClass string              `json:"baselineClass" bson:"baselineClass"`
	MacroRecall   float64             `json:"macroRecall" bson:"macroRecall"`
	MacroF1       float64             `json:"macroF1" bson:"macroF1"`
	PerClass      []ClassMetric       `json:"perClass" bson:"perClass"`
	Confusion     [6][6]int64         `json:"confusion" bson:"confusion"`
	Importance    []FeatureImportance `json:"featureImportance" bson:"featureImportance"`
	NodesUsed     int                 `json:"nodesUsed" bson:"nodesUsed"`
	TreesUsed     int                 `json:"treesUsed" bson:"treesUsed"`
	EvalMs        float64             `json:"evalMs" bson:"evalMs"`
}

// TrainingJob se devuelve en POST /train y se guarda en training_jobs.
type TrainingJob struct {
	JobID        string        `json:"jobId" bson:"jobId"`
	Version      int64         `json:"version" bson:"version"`
	Status       string        `json:"status" bson:"status"`
	CreatedAt    time.Time     `json:"createdAt" bson:"createdAt"`
	DurationMs   float64       `json:"durationMs" bson:"durationMs"`
	Nodes        []NodeTrain   `json:"nodes" bson:"nodes"`
	NodeCount    int           `json:"nodeCount" bson:"nodeCount"`
	TotalTrees   int           `json:"totalTrees" bson:"totalTrees"`
	TrainRecords int64         `json:"trainRecords" bson:"trainRecords"`
	Config       forest.Config `json:"config" bson:"config"`
	Dataset      dataset.Stats `json:"dataset" bson:"dataset"`
	Evaluation   *Evaluation   `json:"evaluation,omitempty" bson:"evaluation,omitempty"`
	EvalError    string        `json:"evaluationError,omitempty" bson:"evaluationError,omitempty"`
}

type PredictRequest struct {
	District *string `json:"district"`
	Franja   *int    `json:"franja"`
	Mes      *int    `json:"mes"`
	Anio     *int    `json:"anio"`
}

type ClassProb struct {
	Class       string  `json:"class" bson:"class"`
	Probability float64 `json:"probability" bson:"probability"`
}

type PredictResponse struct {
	District             string      `json:"district"`
	Ubigeo               string      `json:"ubigeo"`
	Zone                 string      `json:"zone"`
	Franja               int         `json:"franja"`
	FranjaLabel          string      `json:"franjaLabel"`
	Mes                  int         `json:"mes"`
	Anio                 int         `json:"anio"`
	Probabilities        []ClassProb `json:"probabilities"`
	TopClass             string      `json:"topClass"`
	TopProbability       float64     `json:"topProbability"`
	TopStreetCrime       string      `json:"topStreetCrime"`
	TopStreetProbability float64     `json:"topStreetProbability"`
	ModelVersion         int64       `json:"modelVersion"`
	NodesUsed            int         `json:"nodesUsed"`
	TreesUsed            int         `json:"treesUsed"`
	FromCache            bool        `json:"fromCache"`
	ClusterMs            float64     `json:"clusterMs"`
	LatencyMs            float64     `json:"latencyMs"`
}

// PredictionDoc es el documento de la colección predictions.
type PredictionDoc struct {
	CreatedAt      time.Time   `bson:"createdAt"`
	District       string      `bson:"district"`
	Zone           string      `bson:"zone"`
	Franja         int         `bson:"franja"`
	Mes            int         `bson:"mes"`
	Anio           int         `bson:"anio"`
	Probabilities  []ClassProb `bson:"probabilities"`
	TopClass       string      `bson:"topClass"`
	TopProbability float64     `bson:"topProbability"`
	TopStreetCrime string      `bson:"topStreetCrime"`
	ModelVersion   int64       `bson:"modelVersion"`
	NodesUsed      int         `bson:"nodesUsed"`
	TreesUsed      int         `bson:"treesUsed"`
	FromCache      bool        `bson:"fromCache"`
	LatencyMs      float64     `bson:"latencyMs"`
}

type PatrolFranja struct {
	Franja         int     `json:"franja" bson:"franja"`
	Label          string  `json:"label" bson:"label"`
	StreetCrime    string  `json:"streetCrime" bson:"streetCrime"`
	Probability    float64 `json:"probability" bson:"probability"`
	TopClass       string  `json:"topClass" bson:"topClass"`
	TopProbability float64 `json:"topProbability" bson:"topProbability"`
}

type PatrolRow struct {
	Rank             int            `json:"rank" bson:"rank"`
	District         string         `json:"district" bson:"district"`
	Zone             string         `json:"zone" bson:"zone"`
	HistoricalStreet int64          `json:"historicalStreetCrimes" bson:"historicalStreetCrimes"`
	PriorityFranja   string         `json:"priorityFranja" bson:"priorityFranja"`
	PriorityCrime    string         `json:"priorityCrime" bson:"priorityCrime"`
	PriorityProb     float64        `json:"priorityProbability" bson:"priorityProbability"`
	Franjas          []PatrolFranja `json:"franjas" bson:"franjas"`
}

type PatrolResponse struct {
	Mes          int         `json:"mes" bson:"mes"`
	Anio         int         `json:"anio" bson:"anio"`
	ModelVersion int64       `json:"modelVersion" bson:"modelVersion"`
	NodesUsed    int         `json:"nodesUsed" bson:"nodesUsed"`
	TreesUsed    int         `json:"treesUsed" bson:"treesUsed"`
	Queries      int         `json:"queries" bson:"queries"`
	Districts    int         `json:"districts" bson:"districts"`
	FromCache    bool        `json:"fromCache" bson:"fromCache"`
	ClusterMs    float64     `json:"clusterMs" bson:"clusterMs"`
	LatencyMs    float64     `json:"latencyMs" bson:"latencyMs"`
	Note         string      `json:"note" bson:"note"`
	Rows         []PatrolRow `json:"rows" bson:"rows"`
	CreatedAt    time.Time   `json:"createdAt" bson:"createdAt"`
}

type DistrictInfo struct {
	District  string `json:"district"`
	Ubigeo    string `json:"ubigeo"`
	Zone      string `json:"zone"`
	Denuncias int64  `json:"denuncias"`
	Street    int64  `json:"denunciasCalle"`
}

type ModelInfo struct {
	Trained    bool           `json:"trained" bson:"trained"`
	Version    int64          `json:"version,omitempty" bson:"version,omitempty"`
	TrainedAt  *time.Time     `json:"trainedAt,omitempty" bson:"trainedAt,omitempty"`
	TotalTrees int            `json:"totalTrees,omitempty" bson:"totalTrees,omitempty"`
	NodesReady []string       `json:"nodesReady" bson:"nodesReady"`
	Accuracy   float64        `json:"accuracy,omitempty" bson:"accuracy,omitempty"`
	Config     *forest.Config `json:"config,omitempty" bson:"config,omitempty"`
}

type HealthResponse struct {
	Status         string         `json:"status"`
	Time           time.Time      `json:"time"`
	UptimeS        float64        `json:"uptimeS"`
	NodesExpected  int            `json:"nodesExpected"`
	NodesConnected int            `json:"nodesConnected"`
	Nodes          []string       `json:"nodes"`
	Model          ModelInfo      `json:"model"`
	Storage        storage.Status `json:"storage"`
}

type LatencySummary struct {
	Count    int     `json:"count" bson:"count"`
	AvgMs    float64 `json:"avgMs" bson:"avgMs"`
	P50Ms    float64 `json:"p50Ms" bson:"p50Ms"`
	P95Ms    float64 `json:"p95Ms" bson:"p95Ms"`
	P99Ms    float64 `json:"p99Ms" bson:"p99Ms"`
	MaxMs    float64 `json:"maxMs" bson:"maxMs"`
	Under100 float64 `json:"pctUnder100ms" bson:"pctUnder100ms"`
}

type MetricsResponse struct {
	Timestamp   time.Time                 `json:"timestamp" bson:"timestamp"`
	UptimeS     float64                   `json:"uptimeS" bson:"uptimeS"`
	Coordinator map[string]any            `json:"coordinator" bson:"coordinator"`
	Nodes       []cluster.NodeInfo        `json:"nodes" bson:"nodes"`
	Model       ModelInfo                 `json:"model" bson:"model"`
	Requests    map[string]int64          `json:"requests" bson:"requests"`
	CacheHitPct float64                   `json:"cacheHitPct" bson:"cacheHitPct"`
	Latency     map[string]LatencySummary `json:"latency" bson:"latency"`
	Storage     storage.Status            `json:"storage" bson:"storage"`
}
