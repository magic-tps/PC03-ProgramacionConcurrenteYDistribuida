package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Store struct {
	mongo       *mongo.Client
	db          *mongo.Database
	redis       *redis.Client
	mu          sync.RWMutex
	predictions []bson.M
	jobs        []bson.M
}

type Status struct {
	Mongo       string `json:"mongo"`
	Redis       string `json:"redis"`
	Mode        string `json:"mode"`
	Predictions int    `json:"predictionsPersisted"`
	Jobs        int    `json:"jobsPersisted"`
}

func Open(ctx context.Context) *Store {
	s := &Store{}
	mongoURI := env("MONGO_URI", "")
	if mongoURI != "" {
		client, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
		if err == nil {
			if err := client.Ping(ctx, nil); err == nil {
				s.mongo = client
				s.db = client.Database(env("MONGO_DB", "pc4_denuncias"))
			}
		}
	}
	redisURI := env("REDIS_ADDR", "")
	if redisURI != "" {
		s.redis = redis.NewClient(&redis.Options{Addr: redisURI, Password: env("REDIS_PASSWORD", ""), DB: 0})
		if err := s.redis.Ping(ctx).Err(); err != nil {
			s.redis = nil
		}
	}
	return s
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (s *Store) Status(ctx context.Context) Status {
	status := Status{Mongo: "disconnected", Redis: "disconnected", Mode: "memory-fallback"}
	if s.mongo != nil {
		status.Mongo = "connected"
	}
	if s.redis != nil {
		status.Redis = "connected"
	}
	if s.mongo != nil || s.redis != nil {
		status.Mode = "persisted"
	}
	s.mu.RLock()
	status.Predictions = len(s.predictions)
	status.Jobs = len(s.jobs)
	s.mu.RUnlock()
	return status
}

func keyForPrediction(district string, franja, mes, anio int) string {
	sum := md5.Sum([]byte(fmt.Sprintf("%s|%d|%d|%d", district, franja, mes, anio)))
	return "pc4:predict:" + hex.EncodeToString(sum[:])
}

func (s *Store) GetPrediction(ctx context.Context, district string, franja, mes, anio int) ([]float64, bool) {
	key := keyForPrediction(district, franja, mes, anio)
	if s.redis != nil {
		v, err := s.redis.Get(ctx, key).Result()
		if err == nil {
			var prob []float64
			if json.Unmarshal([]byte(v), &prob) == nil {
				return prob, true
			}
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := len(s.predictions) - 1; i >= 0; i-- {
		doc := s.predictions[i]
		if doc["district"] == district && doc["franja"] == franja {
			if prob, ok := doc["probabilities"].([]float64); ok {
				return prob, len(prob) == 6
			}
		}
	}
	return nil, false
}

func (s *Store) SavePrediction(ctx context.Context, district string, franja, mes, anio int, prob []float64, latency float64) {
	key := keyForPrediction(district, franja, mes, anio)
	raw, _ := json.Marshal(prob)
	if s.redis != nil {
		_ = s.redis.Set(ctx, key, raw, 30*time.Second).Err()
	}
	doc := bson.M{
		"district": district, "franja": franja, "month": mes, "year": anio,
		"probabilities": prob, "latencyMs": latency, "createdAt": time.Now().UTC(),
	}
	if s.db != nil {
		_, _ = s.db.Collection("predictions").InsertOne(ctx, doc)
	}
	s.mu.Lock()
	s.predictions = append(s.predictions, doc)
	if len(s.predictions) > 200 {
		s.predictions = s.predictions[len(s.predictions)-200:]
	}
	s.mu.Unlock()
}

func (s *Store) SaveJob(ctx context.Context, jobID string, rows int64, duration float64, nodes int) {
	doc := bson.M{"jobId": jobID, "rows": rows, "durationMs": duration, "nodes": nodes, "createdAt": time.Now().UTC()}
	if s.db != nil {
		_, _ = s.db.Collection("training_jobs").InsertOne(ctx, doc)
	}
	s.mu.Lock()
	s.jobs = append(s.jobs, doc)
	s.mu.Unlock()
}

func (s *Store) Close() {
	if s.mongo != nil {
		_ = s.mongo.Disconnect(context.Background())
	}
	if s.redis != nil {
		_ = s.redis.Close()
	}
}
