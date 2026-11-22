package node

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	cluster "github.com/magic-tps/pc03/pc4/internal/cluster"
)

const classes = 6

type Server struct {
	ID           string
	Address      string
	DataPath     string
	rows         int64
	workerNo     int
	trees        []*Tree
	treesPerNode int
	districtIDs  map[string]int
	zoneIDs      map[string]int
}

type Record struct {
	Features [5]uint16
	Class    uint8
}

type TreeNode struct {
	Leaf      bool
	Feature   int
	Threshold uint16
	Left      int
	Right     int
	Prob      [classes]float32
}

type Tree struct {
	Nodes []TreeNode
}

func NewServer(id, address, dataPath string, workerNo int) *Server {
	return &Server{
		ID:           id,
		Address:      address,
		DataPath:     dataPath,
		workerNo:     workerNo,
		treesPerNode: 6,
		districtIDs:  make(map[string]int),
		zoneIDs:      make(map[string]int),
	}
}

func (s *Server) Run() error {
	conn, err := net.DialTimeout("tcp", s.Address, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	if err := encoder.Encode(map[string]string{"type": "REGISTER", "nodeId": s.ID}); err != nil {
		return err
	}
	for {
		var req cluster.Request
		if err := decoder.Decode(&req); err != nil {
			return err
		}
		var resp cluster.Response
		resp.NodeID = s.ID
		resp.JobID = req.JobID
		switch req.Type {
		case "TRAIN":
			start := time.Now()
			rows, err := s.train(req.DataPath)
			resp.Type = "TRAIN_RESULT"
			resp.Rows = rows
			resp.TimeMs = float64(time.Since(start).Microseconds()) / 1000
			if err != nil {
				resp.Error = err.Error()
			}
		case "PREDICT":
			start := time.Now()
			resp.Type = "PREDICT_RESULT"
			resp.Prob = s.predict(req.District, req.Franja)
			resp.TimeMs = float64(time.Since(start).Microseconds()) / 1000
		default:
			resp.Type = "ERROR"
			resp.Error = "tipo de tarea desconocido"
		}
		if err := encoder.Encode(resp); err != nil {
			return err
		}
	}
}

func normalizeDistrict(s string) string {
	return strings.ToUpper(strings.Join(strings.Fields(s), " "))
}

func (s *Server) train(path string) (int64, error) {
	if path == "" {
		path = s.DataPath
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	reader := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	header, err := reader.Read()
	if err != nil {
		return 0, err
	}
	var rows int64
	records := make([]Record, 0, 700000)
	for {
		rec, err := reader.Read()
		if err != nil {
			break
		}
		if len(rec) != len(header) || len(rec) < 8 {
			continue
		}
		year, yerr := strconv.Atoi(strings.TrimSpace(rec[0]))
		month, merr := strconv.Atoi(strings.TrimSpace(rec[1]))
		if yerr != nil || merr != nil || year < 2010 || month < 1 || month > 12 {
			continue
		}
		dept := normalizeDistrict(rec[2])
		province := normalizeDistrict(rec[3])
		district := normalizeDistrict(rec[4])
		if dept != "LIMA METROPOLITANA" && !strings.Contains(dept, "CALLAO") &&
			!(dept == "LIMA" && province == "LIMA") {
			continue
		}
		quantity, qerr := strconv.Atoi(strings.TrimSpace(rec[7]))
		if qerr != nil || quantity <= 0 {
			continue
		}
		class := classFromModalidad(normalizeDistrict(rec[6]))
		if class < 0 {
			continue
		}
		// Sharding determinista: cada nodo conserva una partición disjunta.
		districtHash := hashDistrict(district)
		if districtHash%3 != s.workerNo {
			continue
		}
		districtID, ok := s.districtIDs[district]
		if !ok {
			districtID = len(s.districtIDs)
			s.districtIDs[district] = districtID
		}
		zone := zoneDistrict(district)
		zoneID, ok := s.zoneIDs[zone]
		if !ok {
			zoneID = len(s.zoneIDs)
			s.zoneIDs[zone] = zoneID
		}
		franja := sampleFranja(class, hashRecord(year, month, district, quantity))
		rows += int64(quantity)
		for i := 0; i < quantity; i++ {
			records = append(records, Record{
				Features: [5]uint16{
					uint16(districtID),
					uint16(zoneID),
					uint16(franja),
					uint16(month),
					uint16(year - 2010),
				},
				Class: uint8(class),
			})
		}
	}

	s.trees = make([]*Tree, 0, s.treesPerNode)
	for i := 0; i < s.treesPerNode; i++ {
		seed := int64(4200 + s.workerNo*7919 + i*104729)
		s.trees = append(s.trees, trainTree(records, seed))
	}
	s.rows = rows
	return rows, nil
}

func hashDistrict(v string) int {
	var h uint32 = 2166136261
	for i := 0; i < len(v); i++ {
		h ^= uint32(v[i])
		h *= 16777619
	}
	return int(h)
}

func hashRecord(year, month int, district string, quantity int) uint64 {
	x := uint64(year)*1000003 + uint64(month)*9176 + uint64(quantity)*31337
	for i := 0; i < len(district); i++ {
		x ^= uint64(district[i]) * 1099511628211
		x *= 14029467366897019727
	}
	return x
}

func zoneDistrict(district string) string {
	switch {
	case strings.Contains(district, "CALLAO"):
		return "CALLAO"
	case district == "SANTA ROSA", district == "ANCON", district == "CARABAYLLO",
		district == "COMAS", district == "INDEPENDENCIA", district == "LOS OLIVOS",
		district == "PUENTE PIEDRA", district == "SAN MARTIN DE PORRES":
		return "LIMA NORTE"
	case district == "ATE", district == "CIENEGUILLA", district == "CHACLACAYO",
		district == "EL AGUSTINO", district == "LA MOLINA", district == "LURIGANCHO",
		district == "SAN JUAN DE LURIGANCHO", district == "SANTA ANITA":
		return "LIMA ESTE"
	case district == "CHORRILLOS", district == "LURIN", district == "PACHACAMAC",
		district == "PUNTA HERMOSA", district == "PUNTA NEGRA",
		district == "SAN JUAN DE MIRAFLORES", district == "SANTA MARIA DEL MAR",
		district == "VILLA EL SALVADOR", district == "VILLA MARIA DEL TRIUNFO":
		return "LIMA SUR"
	default:
		return "LIMA CENTRO"
	}
}

var priorFranja = [classes][4]float64{
	{0.20, 0.17, 0.23, 0.40},
	{0.08, 0.32, 0.38, 0.22},
	{0.24, 0.17, 0.25, 0.34},
	{0.04, 0.46, 0.40, 0.10},
	{0.07, 0.43, 0.36, 0.14},
	{0.30, 0.14, 0.18, 0.38},
}

func sampleFranja(class int, seed uint64) int {
	u := float64((seed>>11)&((1<<53)-1)) / float64(1<<53)
	acc := 0.0
	for f := 0; f < 4; f++ {
		acc += priorFranja[class][f]
		if u < acc {
			return f
		}
	}
	return 3
}

func trainTree(records []Record, seed int64) *Tree {
	rng := rand.New(rand.NewSource(seed))
	sampleSize := 12000
	if len(records) < sampleSize {
		sampleSize = len(records)
	}
	if sampleSize == 0 {
		tree := &Tree{}
		root := TreeNode{Leaf: true}
		root.Prob[0] = 1
		tree.Nodes = append(tree.Nodes, root)
		return tree
	}
	sample := make([]int, sampleSize)
	for i := range sample {
		sample[i] = rng.Intn(len(records))
	}
	tree := &Tree{Nodes: make([]TreeNode, 0, 511)}
	var build func([]int, int) int
	build = func(indices []int, depth int) int {
		pos := len(tree.Nodes)
		tree.Nodes = append(tree.Nodes, TreeNode{Leaf: true})
		counts := [classes]int{}
		for _, i := range indices {
			counts[records[i].Class]++
		}
		total := len(indices)
		majority := 0
		pureClasses := 0
		for c := range counts {
			tree.Nodes[pos].Prob[c] = float32(counts[c]) / float32(total)
			if counts[c] > counts[majority] {
				majority = c
			}
			if counts[c] > 0 {
				pureClasses++
			}
		}
		if depth >= 7 || total < 100 || pureClasses == 1 {
			return pos
		}

		featureOrder := []int{0, 1, 2, 3, 4}
		rng.Shuffle(len(featureOrder), func(i, j int) {
			featureOrder[i], featureOrder[j] = featureOrder[j], featureOrder[i]
		})
		bestGain := -1.0
		bestFeature := -1
		bestThreshold := uint16(0)
		for _, feature := range featureOrder[:3] {
			values := make([]uint16, 0, 16)
			seen := make(map[uint16]bool)
			for _, i := range indices {
				v := records[i].Features[feature]
				if !seen[v] && len(values) < 64 {
					seen[v] = true
					values = append(values, v)
				}
			}
			sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
			for i := 0; i < len(values)-1; i++ {
				threshold := values[i]
				leftCounts := [classes]int{}
				rightCounts := [classes]int{}
				leftN, rightN := 0, 0
				for _, ri := range indices {
					record := records[ri]
					if record.Features[feature] <= threshold {
						leftCounts[record.Class]++
						leftN++
					} else {
						rightCounts[record.Class]++
						rightN++
					}
				}
				if leftN < 50 || rightN < 50 {
					continue
				}
				gain := gini(leftCounts, leftN) + gini(rightCounts, rightN)
				if gain > bestGain {
					bestGain = gain
					bestFeature = feature
					bestThreshold = threshold
				}
			}
		}
		if bestFeature < 0 {
			return pos
		}
		left := make([]int, 0, total/2)
		right := make([]int, 0, total/2)
		for _, i := range indices {
			if records[i].Features[bestFeature] <= bestThreshold {
				left = append(left, i)
			} else {
				right = append(right, i)
			}
		}
		tree.Nodes[pos].Leaf = false
		tree.Nodes[pos].Feature = bestFeature
		tree.Nodes[pos].Threshold = bestThreshold
		tree.Nodes[pos].Left = build(left, depth+1)
		tree.Nodes[pos].Right = build(right, depth+1)
		return pos
	}
	build(sample, 0)
	return tree
}

func gini(counts [classes]int, total int) float64 {
	if total == 0 {
		return 0
	}
	score := 0.0
	for _, count := range counts {
		p := float64(count) / float64(total)
		score += p * p
	}
	return score * float64(total)
}

func predictTree(tree *Tree, x *[5]uint16) [classes]float64 {
	pos := 0
	for {
		node := tree.Nodes[pos]
		if node.Leaf {
			var prob [classes]float64
			for i := range prob {
				prob[i] = float64(node.Prob[i])
			}
			return prob
		}
		if x[node.Feature] <= node.Threshold {
			pos = node.Left
		} else {
			pos = node.Right
		}
	}
}

func classFromModalidad(v string) int {
	switch v {
	case "VIOLENCIA CONTRA LA MUJER E INTEGRANTES",
		"VIOLENCIA CONTRA LA MUJER E INTEGRANTES DEL GRUPO FAMILIAR",
		"VIOLENCIA FAMILIAR":
		return 0
	case "HURTO":
		return 1
	case "ROBO":
		return 2
	case "ESTAFA":
		return 3
	case "EXTORSION":
		return 4
	case "SECUESTRO":
		return 5
	default:
		return -1
	}
}

func (s *Server) predict(district string, franja int) []float64 {
	out := make([]float64, classes)
	if len(s.trees) == 0 {
		base := (hashDistrict(normalizeDistrict(district)) + franja*17 + s.workerNo*3) % 100
		for i := range out {
			out[i] = 0.05 + float64((base+i*19)%100)/1000
		}
	} else {
		districtID := s.districtIDs[normalizeDistrict(district)]
		zone := zoneDistrict(normalizeDistrict(district))
		zoneID := s.zoneIDs[zone]
		x := [5]uint16{uint16(districtID), uint16(zoneID), uint16(franja), 11, 16}
		for _, tree := range s.trees {
			prob := predictTree(tree, &x)
			for i := range out {
				out[i] += prob[i]
			}
		}
		for i := range out {
			out[i] /= float64(len(s.trees))
		}
	}
	total := 0.0
	for _, v := range out {
		total += v
	}
	for i := range out {
		out[i] /= total
	}
	return out
}

func (s *Server) String() string {
	return fmt.Sprintf("node %s rows=%d", s.ID, s.rows)
}
