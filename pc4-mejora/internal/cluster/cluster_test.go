package cluster

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/magic-tps/pc4-grupo3/internal/protocol"
)

// fakeNode responde PING con PONG; si slowFirst, la primera respuesta llega tarde.
func fakeNode(t *testing.T, addr, id string, slowFirst time.Duration) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	enc, dec := json.NewEncoder(conn), json.NewDecoder(conn)
	var mu sync.Mutex
	_ = enc.Encode(protocol.Message{Type: protocol.TypeRegister, NodeID: id})
	var n atomic.Int64
	go func() {
		for {
			var m protocol.Message
			if dec.Decode(&m) != nil {
				return
			}
			go func(m protocol.Message) {
				if n.Add(1) == 1 && slowFirst > 0 {
					time.Sleep(slowFirst)
				}
				mu.Lock()
				_ = enc.Encode(protocol.Message{Type: protocol.TypePong, ID: m.ID, NodeID: id, Version: n.Load()})
				mu.Unlock()
			}(m)
		}
	}()
	return conn
}

func start(t *testing.T) (*Coordinator, string) {
	c := New()
	c.Logf = t.Logf
	a, err := c.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, a.String()
}

func waitNodes(t *testing.T, c *Coordinator, n int) {
	deadline := time.Now().Add(3 * time.Second)
	for len(c.NodeIDs()) != n {
		if time.Now().After(deadline) {
			t.Fatalf("se esperaban %d nodos, hay %d", n, len(c.NodeIDs()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Una respuesta tardía no se entrega a la petición siguiente: se correlaciona
// por ID y se descarta. Además, el timeout NO desconecta al nodo.
func TestLateResponseIsDiscarded(t *testing.T) {
	c, addr := start(t)
	fakeNode(t, addr, "n1", 300*time.Millisecond)
	waitNodes(t, c, 1)

	_, err := c.Call("n1", protocol.Message{Type: protocol.TypePing}, 100*time.Millisecond)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("se esperaba timeout, se obtuvo %v", err)
	}
	if len(c.NodeIDs()) != 1 {
		t.Fatal("el timeout no debe desconectar al nodo")
	}
	// Segunda petición mientras la primera respuesta aún está en camino.
	resp, err := c.Call("n1", protocol.Message{Type: protocol.TypePing}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Version != 2 {
		t.Fatalf("la segunda petición recibió la respuesta de la primera (version=%d)", resp.Version)
	}
	time.Sleep(400 * time.Millisecond) // llega la respuesta tardía
	if got := c.Nodes()[0].LateDropped; got != 1 {
		t.Fatalf("se esperaba 1 respuesta tardía descartada, hubo %d", got)
	}
}

// Ejecutar con -race: latidos, llamadas y lecturas de métricas concurrentes.
func TestConcurrentCallsNoRace(t *testing.T) {
	c, addr := start(t)
	for _, id := range []string{"n1", "n2", "n3"} {
		fakeNode(t, addr, id, 0)
	}
	waitNodes(t, c, 3)
	stop := make(chan struct{})
	c.StartHeartbeat(5*time.Millisecond, stop)
	defer close(stop)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				res, err := c.Broadcast(nil, func(string) protocol.Message { return protocol.Message{Type: protocol.TypePing} }, time.Second)
				if err != nil || len(res) != 3 {
					t.Errorf("broadcast: %v (%d)", err, len(res))
					return
				}
				for _, r := range res {
					if r.Err != nil {
						t.Errorf("%s: %v", r.NodeID, r.Err)
					}
				}
				_ = c.Nodes()
			}
		}()
	}
	wg.Wait()
}

// Un nodo que se reconecta con el mismo ID reemplaza a la conexión anterior
// y la limpieza de la conexión vieja no borra al nodo nuevo.
func TestReconnectReplaces(t *testing.T) {
	c, addr := start(t)
	old := fakeNode(t, addr, "n1", 0)
	waitNodes(t, c, 1)
	fakeNode(t, addr, "n1", 0)
	time.Sleep(100 * time.Millisecond)
	old.Close()
	time.Sleep(100 * time.Millisecond)
	if len(c.NodeIDs()) != 1 {
		t.Fatalf("el nodo reconectado debe seguir registrado")
	}
	if _, err := c.Call("n1", protocol.Message{Type: protocol.TypePing}, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestNodeDisconnectFailsPending(t *testing.T) {
	c, addr := start(t)
	conn := fakeNode(t, addr, "n1", 2*time.Second)
	waitNodes(t, c, 1)
	go func() { time.Sleep(100 * time.Millisecond); conn.Close() }()
	_, err := c.Call("n1", protocol.Message{Type: protocol.TypePing}, 5*time.Second)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("se esperaba ErrClosed, se obtuvo %v", err)
	}
}
