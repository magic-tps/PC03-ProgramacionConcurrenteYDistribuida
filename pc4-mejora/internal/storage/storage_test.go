package storage

import (
	"context"
	"testing"
	"time"
)

// El respaldo en memoria respeta el TTL (antes la caché en memoria no expiraba nunca).
func TestMemoryTTL(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	m.CacheSet(ctx, "k", []byte("v"), 50*time.Millisecond)
	if _, ok := m.CacheGet(ctx, "k"); !ok {
		t.Fatal("la clave debe existir")
	}
	time.Sleep(80 * time.Millisecond)
	if _, ok := m.CacheGet(ctx, "k"); ok {
		t.Fatal("la clave debe expirar")
	}
	m.CacheSet(ctx, "fija", []byte("v"), 0)
	if _, ok := m.CacheGet(ctx, "fija"); !ok {
		t.Fatal("TTL 0 = sin expiración")
	}
	_ = m.Insert(ctx, CollPredictions, map[string]int{"a": 1})
	if m.Status(ctx).Collections[CollPredictions] != 1 {
		t.Fatal("conteo de documentos")
	}
}
