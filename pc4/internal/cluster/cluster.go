package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type Request struct {
	Type     string `json:"type"`
	JobID    string `json:"jobId,omitempty"`
	District string `json:"district,omitempty"`
	Franja   int    `json:"franja,omitempty"`
	Mes      int    `json:"mes,omitempty"`
	Anio     int    `json:"anio,omitempty"`
	DataPath string `json:"dataPath,omitempty"`
	Worker   int    `json:"worker,omitempty"`
}

type Response struct {
	NodeID string    `json:"nodeId"`
	JobID  string    `json:"jobId,omitempty"`
	Type   string    `json:"type"`
	Error  string    `json:"error,omitempty"`
	Rows   int64     `json:"rows,omitempty"`
	TimeMs float64   `json:"timeMs,omitempty"`
	Prob   []float64 `json:"prob,omitempty"`
}

type Node struct {
	ID        string
	Address   string
	Conn      net.Conn
	Encoder   *json.Encoder
	Decoder   *json.Decoder
	LastSeen  time.Time
	Responses chan Response
	mu        sync.Mutex
}

type Coordinator struct {
	mu    sync.Mutex
	nodes map[string]*Node
}

func NewCoordinator() *Coordinator {
	return &Coordinator{nodes: make(map[string]*Node)}
}

func (c *Coordinator) Listen(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	go func() {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go c.handle(conn)
		}
	}()
	return nil
}

func (c *Coordinator) handle(conn net.Conn) {
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	var reg struct {
		Type string `json:"type"`
		ID   string `json:"nodeId"`
	}
	if err := decoder.Decode(&reg); err != nil {
		conn.Close()
		return
	}
	if reg.Type != "REGISTER" || reg.ID == "" {
		conn.Close()
		return
	}
	node := &Node{ID: reg.ID, Address: conn.RemoteAddr().String(), Conn: conn, Encoder: encoder, Decoder: decoder, LastSeen: time.Now(), Responses: make(chan Response, 16)}
	c.mu.Lock()
	c.nodes[node.ID] = node
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.nodes, node.ID)
		c.mu.Unlock()
		conn.Close()
	}()

	for {
		var response Response
		if err := decoder.Decode(&response); err != nil {
			return
		}
		node.LastSeen = time.Now()
		select {
		case node.Responses <- response:
		default:
		}
	}
}

func (c *Coordinator) Nodes() []NodeSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]NodeSnapshot, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, NodeSnapshot{ID: n.ID, Address: n.Address, LastSeen: n.LastSeen})
	}
	return out
}

type NodeSnapshot struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	LastSeen time.Time `json:"lastSeen"`
}

func (c *Coordinator) Send(req Request, timeout time.Duration) ([]Response, error) {
	c.mu.Lock()
	nodes := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()
	if len(nodes) == 0 {
		return nil, errors.New("no hay nodos ML conectados")
	}

	responses := make([]Response, len(nodes))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for i, node := range nodes {
		wg.Add(1)
		go func(i int, node *Node) {
			defer wg.Done()
			node.mu.Lock()
			defer node.mu.Unlock()
			_ = node.Conn.SetDeadline(time.Now().Add(timeout))
			if err := node.Encoder.Encode(req); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			var response Response
			select {
			case response = <-node.Responses:
			case <-time.After(timeout):
				mu.Lock()
				if firstErr == nil {
					firstErr = errors.New("timeout esperando respuesta del nodo")
				}
				mu.Unlock()
				return
			}
			_ = node.Conn.SetDeadline(time.Time{})
			responses[i] = response
		}(i, node)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, fmt.Errorf("fallo TCP con un nodo: %w", firstErr)
	}
	return responses, nil
}
