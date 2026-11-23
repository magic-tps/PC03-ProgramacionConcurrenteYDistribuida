// Package storage define la persistencia del coordinador: caché (Redis) y
// documentos (MongoDB). Memory es la implementación en memoria usada en las
// pruebas y como respaldo explícito cuando no hay bases de datos.
package storage

import (
	"context"
	"sync"
	"time"
)

// Colecciones de MongoDB.
const (
	CollTrainingJobs = "training_jobs"
	CollPredictions  = "predictions"
	CollPatrol       = "patrol_recommendations"
	CollMetrics      = "cluster_metrics"
)

var Collections = []string{CollTrainingJobs, CollPredictions, CollPatrol, CollMetrics}

type Store interface {
	CacheGet(ctx context.Context, key string) ([]byte, bool)
	CacheSet(ctx context.Context, key string, value []byte, ttl time.Duration)
	Insert(ctx context.Context, collection string, doc any) error
	Status(ctx context.Context) Status
	Close()
}

type Status struct {
	Mode        string           `json:"mode" bson:"mode"`
	Mongo       string           `json:"mongo" bson:"mongo"`
	Redis       string           `json:"redis" bson:"redis"`
	Database    string           `json:"database,omitempty" bson:"database,omitempty"`
	Collections map[string]int64 `json:"collections" bson:"collections"`
	CacheKeys   int64            `json:"cacheKeys" bson:"cacheKeys"`
}

// Memory: caché con expiración y documentos en memoria.
type Memory struct {
	mu    sync.Mutex
	cache map[string]memEntry
	docs  map[string]int64
	last  map[string]any
}

type memEntry struct {
	value   []byte
	expires time.Time
}

func NewMemory() *Memory {
	return &Memory{cache: map[string]memEntry{}, docs: map[string]int64{}, last: map[string]any{}}
}

func (m *Memory) CacheGet(_ context.Context, key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.cache[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(e.expires) { // a diferencia del código anterior, el respaldo SÍ expira
		delete(m.cache, key)
		return nil, false
	}
	return e.value, true
}

func (m *Memory) CacheSet(_ context.Context, key string, value []byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.cache) > 5000 { // evita crecer sin límite
		now := time.Now()
		for k, e := range m.cache {
			if now.After(e.expires) {
				delete(m.cache, k)
			}
		}
	}
	if ttl <= 0 { // igual que Redis: TTL 0 = sin expiración
		ttl = 100 * 365 * 24 * time.Hour
	}
	m.cache[key] = memEntry{value: value, expires: time.Now().Add(ttl)}
}

func (m *Memory) Insert(_ context.Context, collection string, doc any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.docs[collection]++
	m.last[collection] = doc
	return nil
}

func (m *Memory) Last(collection string) any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last[collection]
}

func (m *Memory) Status(_ context.Context) Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := Status{Mode: "memoria (sin MongoDB/Redis)", Mongo: "no configurado", Redis: "no configurado", Collections: map[string]int64{}}
	for _, c := range Collections {
		st.Collections[c] = m.docs[c]
	}
	now := time.Now()
	for _, e := range m.cache {
		if now.Before(e.expires) {
			st.CacheKeys++
		}
	}
	return st
}

func (m *Memory) Close() {}
