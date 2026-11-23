// Package cluster es el lado coordinador del clúster: acepta nodos por TCP
// (topología estrella), correlaciona peticiones/respuestas por ID y reparte
// trabajo en paralelo (fan-out con goroutines, fan-in con canal + WaitGroup).
package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/magic-tps/pc4-grupo3/internal/protocol"
)

var (
	ErrNoNodes = errors.New("no hay nodos ML conectados")
	ErrTimeout = errors.New("tiempo de espera agotado esperando al nodo")
	ErrClosed  = errors.New("conexión con el nodo cerrada")
)

// NodeConn representa un nodo registrado.
type NodeConn struct {
	ID   string
	Addr string

	conn   net.Conn
	encMu  sync.Mutex // serializa escrituras al socket
	enc    *json.Encoder
	closed chan struct{}
	once   sync.Once

	mu        sync.Mutex // protege los campos de abajo (antes LastSeen se escribía sin candado: carrera de datos)
	lastSeen  time.Time
	connected time.Time
	stats     protocol.NodeStats
	pending   map[string]chan protocol.Message
	late      int64
}

// NodeInfo es una copia segura para exponer en la API.
type NodeInfo struct {
	ID          string             `json:"id" bson:"id"`
	Address     string             `json:"address" bson:"address"`
	ConnectedAt time.Time          `json:"connectedAt" bson:"connectedAt"`
	LastSeen    time.Time          `json:"lastSeen" bson:"lastSeen"`
	LateDropped int64              `json:"lateResponsesDropped" bson:"lateResponsesDropped"`
	Stats       protocol.NodeStats `json:"stats" bson:"stats"`
}

type Coordinator struct {
	mu    sync.RWMutex
	nodes map[string]*NodeConn
	seq   atomic.Uint64
	ln    net.Listener

	// OnRegister se invoca (en otra goroutine) cuando un nodo se registra.
	OnRegister func(id string, stats protocol.NodeStats)
	Logf       func(format string, args ...any)
}

func New() *Coordinator {
	return &Coordinator{nodes: map[string]*NodeConn{}, Logf: log.Printf}
}

// Listen abre el servidor TCP y atiende cada conexión en su propia goroutine.
func (c *Coordinator) Listen(addr string) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	c.ln = ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}
			go c.handle(conn)
		}
	}()
	return ln.Addr(), nil
}

func (c *Coordinator) Close() {
	if c.ln != nil {
		c.ln.Close()
	}
	c.mu.Lock()
	for _, n := range c.nodes {
		n.close()
	}
	c.mu.Unlock()
}

func (n *NodeConn) close() {
	n.once.Do(func() {
		close(n.closed)
		n.conn.Close()
	})
}

func (c *Coordinator) handle(conn net.Conn) {
	dec := json.NewDecoder(conn)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var reg protocol.Message
	if err := dec.Decode(&reg); err != nil || reg.Type != protocol.TypeRegister || reg.NodeID == "" {
		conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{}) // sin plazo de lectura: los timeouts son por petición
	now := time.Now()
	n := &NodeConn{
		ID: reg.NodeID, Addr: conn.RemoteAddr().String(), conn: conn, enc: json.NewEncoder(conn),
		closed: make(chan struct{}), lastSeen: now, connected: now, pending: map[string]chan protocol.Message{},
	}
	if reg.Stats != nil {
		n.stats = *reg.Stats
	}
	c.mu.Lock()
	if old, ok := c.nodes[n.ID]; ok {
		old.close() // un nodo que vuelve con el mismo ID reemplaza la conexión vieja
	}
	c.nodes[n.ID] = n
	c.mu.Unlock()
	c.Logf("nodo %s registrado desde %s", n.ID, n.Addr)
	if c.OnRegister != nil {
		go c.OnRegister(n.ID, n.stats)
	}

	defer func() {
		n.close()
		c.mu.Lock()
		if c.nodes[n.ID] == n { // no borrar a un reemplazo más nuevo
			delete(c.nodes, n.ID)
		}
		c.mu.Unlock()
		n.mu.Lock()
		for id, ch := range n.pending {
			close(ch) // despierta a quien esperaba: recibirá ErrClosed
			delete(n.pending, id)
		}
		n.mu.Unlock()
		c.Logf("nodo %s desconectado", n.ID)
	}()

	for {
		var msg protocol.Message
		if err := dec.Decode(&msg); err != nil {
			return
		}
		n.mu.Lock()
		n.lastSeen = time.Now()
		if msg.Stats != nil {
			n.stats = *msg.Stats
		}
		ch, ok := n.pending[msg.ID]
		if ok {
			delete(n.pending, msg.ID)
		} else {
			n.late++
		}
		n.mu.Unlock()
		if ok {
			ch <- msg // búfer 1: nunca bloquea
		} else {
			c.Logf("respuesta tardía o desconocida de %s (id=%s, %s) descartada", n.ID, msg.ID, msg.Type)
		}
	}
}

// Call envía un mensaje a un nodo y espera SU respuesta (misma ID). Si vence
// el plazo, la petición se da por perdida pero la conexión sigue abierta; una
// respuesta que llegue después se descarta porque su ID ya no está pendiente.
func (c *Coordinator) Call(nodeID string, msg protocol.Message, timeout time.Duration) (protocol.Message, error) {
	c.mu.RLock()
	n, ok := c.nodes[nodeID]
	c.mu.RUnlock()
	if !ok {
		return protocol.Message{}, fmt.Errorf("nodo %s no conectado", nodeID)
	}
	return n.call(c.nextID(), msg, timeout)
}

func (c *Coordinator) nextID() string {
	return "r" + strconv.FormatUint(c.seq.Add(1), 10)
}

func (n *NodeConn) call(id string, msg protocol.Message, timeout time.Duration) (protocol.Message, error) {
	msg.ID = id
	ch := make(chan protocol.Message, 1)
	n.mu.Lock()
	n.pending[id] = ch
	n.mu.Unlock()
	cleanup := func() {
		n.mu.Lock()
		delete(n.pending, id)
		n.mu.Unlock()
	}

	n.encMu.Lock()
	_ = n.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)) // solo escritura
	err := n.enc.Encode(msg)
	n.encMu.Unlock()
	if err != nil {
		cleanup()
		n.close()
		return protocol.Message{}, fmt.Errorf("enviando a %s: %w", n.ID, err)
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case resp, ok := <-ch:
		if !ok {
			return protocol.Message{}, ErrClosed
		}
		return resp, nil
	case <-timer.C:
		cleanup()
		return protocol.Message{}, ErrTimeout
	case <-n.closed:
		cleanup()
		return protocol.Message{}, ErrClosed
	}
}

// Result de una llamada a un nodo dentro de un Broadcast.
type Result struct {
	NodeID string
	Msg    protocol.Message
	Err    error
	Ms     float64
}

// Broadcast hace fan-out a los nodos indicados (todos si ids == nil): una
// goroutine por nodo, resultados por un canal con búfer = número de nodos y
// un WaitGroup que lo cierra. El orden del resultado es por ID de nodo.
func (c *Coordinator) Broadcast(ids []string, build func(nodeID string) protocol.Message, timeout time.Duration) ([]Result, error) {
	c.mu.RLock()
	targets := make([]*NodeConn, 0, len(c.nodes))
	if ids == nil {
		for _, n := range c.nodes {
			targets = append(targets, n)
		}
	} else {
		for _, id := range ids {
			if n, ok := c.nodes[id]; ok {
				targets = append(targets, n)
			}
		}
	}
	c.mu.RUnlock()
	if len(targets) == 0 {
		return nil, ErrNoNodes
	}
	out := make(chan Result, len(targets))
	var wg sync.WaitGroup
	for _, n := range targets {
		wg.Add(1)
		go func(n *NodeConn) {
			defer wg.Done()
			start := time.Now()
			msg, err := n.call(c.nextID(), build(n.ID), timeout)
			out <- Result{NodeID: n.ID, Msg: msg, Err: err, Ms: float64(time.Since(start).Microseconds()) / 1000}
		}(n)
	}
	go func() { wg.Wait(); close(out) }()
	results := make([]Result, 0, len(targets))
	for r := range out {
		results = append(results, r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].NodeID < results[j].NodeID })
	return results, nil
}

// Nodes devuelve una copia de la información de cada nodo, ordenada por ID.
func (c *Coordinator) Nodes() []NodeInfo {
	c.mu.RLock()
	list := make([]*NodeConn, 0, len(c.nodes))
	for _, n := range c.nodes {
		list = append(list, n)
	}
	c.mu.RUnlock()
	out := make([]NodeInfo, 0, len(list))
	for _, n := range list {
		n.mu.Lock()
		out = append(out, NodeInfo{ID: n.ID, Address: n.Addr, ConnectedAt: n.connected, LastSeen: n.lastSeen, LateDropped: n.late, Stats: n.stats})
		n.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// NodeIDs devuelve los IDs conectados, ordenados.
func (c *Coordinator) NodeIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// StartHeartbeat envía PING periódicos; las respuestas actualizan las métricas
// de cada nodo. Termina cuando se cierra stop.
func (c *Coordinator) StartHeartbeat(interval time.Duration, stop <-chan struct{}) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				_, _ = c.Broadcast(nil, func(string) protocol.Message { return protocol.Message{Type: protocol.TypePing} }, interval)
			}
		}
	}()
}
