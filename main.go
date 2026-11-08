// =============================================================================
//  CC65 · Programación Concurrente y Distribuida — UPC
//  Trabajo Final · Entregable 1 (PC3) · Fase 1 (código Go)
//
//  CASO 2: Análisis y Clasificación de Delitos y Faltas Urbanas
//  Fuente : SIDPOL – MININTER / PNP (Portal Nacional de Datos Abiertos)
//  Archivo: "DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv"
//           376,715 filas agregadas · 8 columnas · Σ cantidad = 7,509,690
//
//  PARTE A : Cargador y limpiador concurrente (pipeline + worker pool) que
//            DESAGREGA las filas agrupadas por `cantidad` en denuncias
//            individuales (> 1.85 M registros para Lima Metropolitana + Callao).
//  PARTE B : Prototipo local de Random Forest (Bagging con data parallelism)
//            + probabilidades condicionales P(delito | distrito, franja)
//            + benchmark 1 worker vs N workers (Speedup, eficiencia, Karp-Flatt).
//
//  Solo librería estándar.  Ejecutar:   go run main.go
//  Opciones útiles:                    go run main.go -h
//
//  Primitivas de concurrencia (Semanas 10 y 11) — buscar las etiquetas [S10]/[S11]:
//    [S10-A] Canales con búfer como cola FIFO (desacople productor/consumidores)
//    [S10-B] Canales direccionales: <-chan (solo lectura) / chan<- (solo escritura)
//    [S10-C] Pipeline: lector -> workers -> recolector, con close() ordenado
//    [S11-A] Worker Pool de N = runtime.NumCPU() goroutines + *sync.WaitGroup
//    [S11-B] select bloqueante multi-canal (monitor sin busy waiting)
//    [S11-C] select con default (envío no bloqueante, patrón nonblocking.go)
//    [S11-D] Semáforo con canal con búfer (fusión paralela de lotes)
//    [S11-E] Map-Reduce sin locks (cada goroutine escribe en su propia memoria)
// =============================================================================

package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// -----------------------------------------------------------------------------
// 1. CONSTANTES DEL DOMINIO
// -----------------------------------------------------------------------------

const (
	NumClases   = 6 // clases del clasificador ("Otros" se excluye: no clasificable)
	NumFranjas  = 4
	NumFeatures = 5
	anioBase    = 2010 // el año se guarda como (anio - anioBase) en un uint8
	maxCantidad = 100000

	rutaSintetica = "DATASET_Denuncias_Policiales_SINTETICO.csv"
)

// Nombres de archivo que se buscan (el portal usa espacios; algunas descargas
// los reemplazan por guiones bajos).
var candidatosCSV = []string{
	"DATASET_Denuncias Policiales - Enero 2018 a Septiembre 2026.csv",
	"DATASET_Denuncias_Policiales_-_Enero_2018_a_Septiembre_2026.csv",
}

var cabeceraEsperada = []string{"ANIO", "MES", "DPTO_HECHO_NEW", "PROV_HECHO", "DIST_HECHO", "UBIGEO_HECHO", "P_MODALIDADES", "CANTIDAD"}

// Clases objetivo (índices 0..5).  "Otros" = -1 (no clasificable).
var clasesCortas = [NumClases]string{"Violencia mujer/IGF", "Hurto", "Robo", "Estafa", "Extorsión", "Secuestro"}

// Delitos que la patrulla preventiva puede disuadir en la vía pública.
// (La violencia contra la mujer ocurre mayormente en el hogar: se atiende con
// otras estrategias — CEM, Línea 100 — y no define la ruta de patrullaje.)
var esPatrullable = [NumClases]bool{false, true, true, false, true, true}

// Mapa de modalidad normalizada (MAYÚSCULAS, sin tildes, espacios simples) -> clase.
var mapaModalidades = map[string]int{
	"VIOLENCIA CONTRA LA MUJER E INTEGRANTES":                    0,
	"VIOLENCIA CONTRA LA MUJER E INTEGRANTES DEL GRUPO FAMILIAR": 0,
	"VIOLENCIA FAMILIAR":                                         0,
	"HURTO":                                                      1,
	"ROBO":                                                       2,
	"ESTAFA":                                                     3,
	"EXTORSION":                                                  4,
	"SECUESTRO":                                                  5,
	"OTROS":                                                      -1,
}

// Etiquetas exactamente como vienen en el archivo oficial: si una fila no
// coincide literalmente pero sí tras normalizar, se cuenta como "reparada".
var etiquetasOficiales = map[string]bool{
	"Otros": true, "Violencia contra la mujer e integrantes": true, "Hurto": true,
	"Robo": true, "Estafa": true, "Extorsión": true, "Secuestro": true,
}

var nombresFranjas = [NumFranjas]string{"Madrugada 00-06", "Mañana 06-12", "Tarde 12-18", "Noche 18-24"}
var franjasCortas = [NumFranjas]string{"Madrugada", "Mañana", "Tarde", "Noche"}

// -----------------------------------------------------------------------------
// SUPUESTO DE MODELADO — Franja horaria derivada
// El dataset SIDPOL abierto NO trae la hora del hecho.  Para cada denuncia
// desagregada se IMPUTA una franja con una distribución a priori coherente con
// la modalidad (p.ej. estafas y extorsiones concentradas en horario de oficina,
// robos y violencia doméstica con pico nocturno).  Es un supuesto explícito y
// reproducible (semilla por fila), que debe reemplazarse por la hora real
// cuando se cuente con los microdatos de SIDPOL.
//
//	Madrugada Mañana  Tarde   Noche
//
// -----------------------------------------------------------------------------
var priorFranjas = [NumClases][NumFranjas]float64{
	{0.20, 0.17, 0.23, 0.40}, // Violencia contra la mujer e integrantes
	{0.08, 0.32, 0.38, 0.22}, // Hurto
	{0.24, 0.17, 0.25, 0.34}, // Robo
	{0.04, 0.46, 0.40, 0.10}, // Estafa
	{0.07, 0.43, 0.36, 0.14}, // Extorsión
	{0.30, 0.14, 0.18, 0.38}, // Secuestro
}
var acumFranjas [NumClases][NumFranjas]float64 // distribución acumulada (se llena en init)

// Zonas urbanas de Lima Metropolitana (feature espacial agregada).
var zonasLima = map[string]string{
	"LIMA": "LIMA CENTRO", "BREÑA": "LIMA CENTRO", "LA VICTORIA": "LIMA CENTRO", "RIMAC": "LIMA CENTRO",
	"SAN LUIS": "LIMA CENTRO", "BARRANCO": "LIMA CENTRO", "JESUS MARIA": "LIMA CENTRO", "LINCE": "LIMA CENTRO",
	"MAGDALENA DEL MAR": "LIMA CENTRO", "PUEBLO LIBRE": "LIMA CENTRO", "MIRAFLORES": "LIMA CENTRO",
	"SAN BORJA": "LIMA CENTRO", "SAN ISIDRO": "LIMA CENTRO", "SAN MIGUEL": "LIMA CENTRO",
	"SANTIAGO DE SURCO": "LIMA CENTRO", "SURQUILLO": "LIMA CENTRO",
	"ANCON": "LIMA NORTE", "CARABAYLLO": "LIMA NORTE", "COMAS": "LIMA NORTE", "INDEPENDENCIA": "LIMA NORTE",
	"LOS OLIVOS": "LIMA NORTE", "PUENTE PIEDRA": "LIMA NORTE", "SAN MARTIN DE PORRES": "LIMA NORTE", "SANTA ROSA": "LIMA NORTE",
	"ATE": "LIMA ESTE", "CHACLACAYO": "LIMA ESTE", "CIENEGUILLA": "LIMA ESTE", "EL AGUSTINO": "LIMA ESTE",
	"LURIGANCHO - CHOSICA": "LIMA ESTE", "SAN JUAN DE LURIGANCHO": "LIMA ESTE", "SANTA ANITA": "LIMA ESTE", "LA MOLINA": "LIMA ESTE",
	"CHORRILLOS": "LIMA SUR", "LURIN": "LIMA SUR", "PACHACAMAC": "LIMA SUR", "PUCUSANA": "LIMA SUR",
	"PUNTA HERMOSA": "LIMA SUR", "PUNTA NEGRA": "LIMA SUR", "SAN BARTOLO": "LIMA SUR", "SAN JUAN DE MIRAFLORES": "LIMA SUR",
	"SANTA MARIA DEL MAR": "LIMA SUR", "VILLA EL SALVADOR": "LIMA SUR", "VILLA MARIA DEL TRIUNFO": "LIMA SUR",
}

var sinTildes = strings.NewReplacer("Á", "A", "É", "E", "Í", "I", "Ó", "O", "Ú", "U", "Ü", "U", "\uFEFF", "")

var anioMax = time.Now().Year() + 1

func init() {
	for c := 0; c < NumClases; c++ {
		s := 0.0
		for f := 0; f < NumFranjas; f++ {
			s += priorFranjas[c][f]
			acumFranjas[c][f] = s
		}
		acumFranjas[c][NumFranjas-1] = 1.0
	}
}

// -----------------------------------------------------------------------------
// 2. CONFIGURACIÓN (flags)
// -----------------------------------------------------------------------------

type ConfigRF struct {
	NumArboles int
	ProfMax    int
	MinHoja    int
	Mtry       int
	Fraccion   float64 // tamaño de cada muestra bootstrap / tamaño del train
	Semilla    int64
}

type Config struct {
	RutaCSV   string
	Workers   int
	TamLote   int
	Buffer    int
	Alcance   string // "lima" (Lima Metropolitana + Callao) | "nacional"
	Parseo    string // "lector" | "workers"
	Verbose   bool
	RepsBench int
	RF        ConfigRF
}

func leerFlags() Config {
	var c Config
	flag.StringVar(&c.RutaCSV, "csv", "", "ruta del CSV SIDPOL (por defecto se busca en el directorio actual)")
	flag.IntVar(&c.Workers, "workers", runtime.NumCPU(), "N workers del pool (por defecto runtime.NumCPU())")
	flag.IntVar(&c.TamLote, "lote", 10000, "filas del CSV por lote (chunk)")
	flag.IntVar(&c.Buffer, "buffer", 16, "capacidad de los canales con búfer (cola FIFO de lotes)")
	flag.StringVar(&c.Alcance, "alcance", "lima", "lima = Lima Metropolitana + Callao | nacional = todo el Perú")
	flag.StringVar(&c.Parseo, "parseo", "lector", "dónde se parsea el CSV: lector (encoding/csv en el productor) | workers")
	flag.BoolVar(&c.Verbose, "v", true, "mostrar log de cada lote procesado por los workers")
	flag.IntVar(&c.RepsBench, "reps", 3, "repeticiones del benchmark (se toma el mejor tiempo)")
	flag.IntVar(&c.RF.NumArboles, "arboles", 32, "número de árboles del Random Forest")
	flag.IntVar(&c.RF.ProfMax, "prof", 12, "profundidad máxima de cada árbol")
	flag.IntVar(&c.RF.MinHoja, "minhoja", 50, "mínimo de muestras por hoja")
	flag.IntVar(&c.RF.Mtry, "mtry", 3, "features candidatas por nodo (≈ ceil(sqrt(5)))")
	flag.Float64Var(&c.RF.Fraccion, "muestra", 0.30, "fracción del train en cada muestra bootstrap")
	flag.Int64Var(&c.RF.Semilla, "semilla", 42, "semilla del bosque (reproducible)")
	flag.Parse()
	if c.Workers < 1 {
		c.Workers = 1
	}
	if c.TamLote < 100 {
		c.TamLote = 100
	}
	if c.Buffer < 1 {
		c.Buffer = 1
	}
	if c.RepsBench < 1 {
		c.RepsBench = 1
	}
	c.Alcance = strings.ToLower(c.Alcance)
	if c.Alcance != "nacional" {
		c.Alcance = "lima"
	}
	c.Parseo = strings.ToLower(c.Parseo)
	if c.Parseo != "workers" {
		c.Parseo = "lector"
	}
	if c.RF.Mtry < 1 || c.RF.Mtry > NumFeatures {
		c.RF.Mtry = 3
	}
	return c
}

// -----------------------------------------------------------------------------
// 3. ESTRUCTURAS DEL PIPELINE
// -----------------------------------------------------------------------------

// LoteCrudo: bloque (chunk) de filas tal como salen de encoding/csv.
// Se envían LOTES y no filas sueltas: 38 mensajes por el canal en vez de
// 376,715 (o de 2 M si se desagregara en el lector) -> mínima contención.
type LoteCrudo struct {
	ID         int
	FilaInicio int64      // nº global (1-based) de la primera fila de datos del lote
	Registros  [][]string // modo -parseo=lector : filas ya parseadas por encoding/csv
	Texto      []byte     // modo -parseo=workers: bloque crudo de líneas (lo parsea el worker)
}

const (
	motivoColumnas = iota
	motivoVacio
	motivoFecha
	motivoUbigeo
	motivoModalidad
	motivoCantidad
	motivoCSV
	numMotivos
)

var nombresMotivos = [numMotivos]string{
	"Nº de columnas incorrecto (fila truncada)",
	"Campo obligatorio vacío",
	"Año/mes fuera de rango o no numérico",
	"UBIGEO inválido",
	"Modalidad no reconocida",
	"Cantidad no numérica, <= 0 o atípica",
	"Línea CSV malformada (parser)",
}

type Metricas struct {
	FilasLeidas              int64
	FilasInvalidas           int64
	Motivos                  [numMotivos]int64
	FilasReparadas           int64 // con formato desordenado, recuperadas al normalizar
	DenunciasNacionales      int64 // Σ cantidad de las filas con formato válido
	PorModalidadNacional     [NumClases + 1]int64
	FilasFueraAlcance        int64
	DenunciasFueraAlcance    int64
	FilasNoClasificables     int64 // modalidad "Otros"
	DenunciasNoClasificables int64
	FilasValidas             int64 // en alcance y clasificables
	Desagregados             int64 // denuncias individuales generadas
}

func (m *Metricas) sumar(o *Metricas) {
	m.FilasLeidas += o.FilasLeidas
	m.FilasInvalidas += o.FilasInvalidas
	for i := range m.Motivos {
		m.Motivos[i] += o.Motivos[i]
	}
	m.FilasReparadas += o.FilasReparadas
	m.DenunciasNacionales += o.DenunciasNacionales
	for i := range m.PorModalidadNacional {
		m.PorModalidadNacional[i] += o.PorModalidadNacional[i]
	}
	m.FilasFueraAlcance += o.FilasFueraAlcance
	m.DenunciasFueraAlcance += o.DenunciasFueraAlcance
	m.FilasNoClasificables += o.FilasNoClasificables
	m.DenunciasNoClasificables += o.DenunciasNoClasificables
	m.FilasValidas += o.FilasValidas
	m.Desagregados += o.Desagregados
}

// LoteLimpio: resultado de un worker.  Formato columnar (struct of arrays):
// cada posición = 1 denuncia individual.  Los distritos usan un diccionario
// LOCAL al lote, así los workers no comparten memoria ni necesitan Mutex.
type LoteLimpio struct {
	ID        int
	Worker    int
	Duracion  time.Duration
	Metricas  Metricas
	Ubigeos   []string
	NomDist   []string
	DptoDist  []string
	DistLocal []uint16
	Mes       []uint8
	Anio      []uint8
	Franja    []uint8
	Clase     []uint8
}

type ResultadoLector struct {
	Lotes       int
	Filas       int64
	Malformadas int64
	Duracion    time.Duration
	Err         error
}

const (
	evLoteEmitido = iota
	evLoteProcesado
)

type Evento struct {
	Tipo   int
	Worker int
	Filas  int
	Desag  int64
}

// Dataset final en memoria (columnar).
type Dataset struct {
	N         int
	Dist      []uint16
	Mes       []uint8
	Anio      []uint8
	Franja    []uint8
	Clase     []uint8
	NomDist   []string
	Ubigeo    []string
	DptoDist  []string
	ZonaDist  []uint16
	NomZona   []string
	UltimoMes int // anio*12 + (mes-1) más reciente del dataset
}

// -----------------------------------------------------------------------------
// 4. ETAPA 1 DEL PIPELINE — LECTOR (productor)
// -----------------------------------------------------------------------------

// lector lee el CSV con bufio y agrupa las filas en lotes (chunks).
//
//	[S10-B] salida  chan<- LoteCrudo : canal de SOLO ESCRITURA (el lector no puede leerlo)
//	[S10-B] eventos chan<- Evento    : solo escritura hacia el monitor
//	[S10-B] res     chan<- ResultadoLector
//	[S10-C] Al terminar hace close(salida): es la señal de "no hay más trabajo"
//	        que hace terminar los `for range` de todos los workers.
//
// Dos modos (flag -parseo):
//
//	"lector"  : el lector usa encoding/csv y envía lotes de [][]string ya parseados.
//	"workers" : el lector solo corta el archivo en bloques de N líneas crudas
//	            (bufio, muy barato) y CADA WORKER parsea su bloque con encoding/csv.
//	            Así el parseo —la parte más costosa— sale de la sección serial
//	            (ley de Amdahl).  Válido porque SIDPOL no tiene saltos de línea
//	            dentro de campos entrecomillados (1 línea = 1 registro).
func lector(ruta string, tamLote int, parseoEnWorkers bool, salida chan<- LoteCrudo, eventos chan<- Evento, res chan<- ResultadoLector) {
	ini := time.Now()
	r := ResultadoLector{}
	defer func() {
		close(salida) // [S10-C] cierre ordenado: solo el productor cierra su canal
		r.Duracion = time.Since(ini)
		res <- r
	}()

	f, err := os.Open(ruta)
	if err != nil {
		r.Err = err
		return
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20) // lectura de disco en bloques de 1 MiB

	emitir := func(l LoteCrudo, filas int) {
		salida <- l // [S10-A] si la cola FIFO está llena, el lector espera (backpressure)
		enviarEvento(eventos, Evento{Tipo: evLoteEmitido, Filas: filas})
		r.Lotes++
	}
	var fila int64

	if parseoEnWorkers {
		linea, err := br.ReadString('\n')
		if err != nil && linea == "" {
			r.Err = fmt.Errorf("no se pudo leer la cabecera: %w", err)
			return
		}
		if cab, e := csv.NewReader(strings.NewReader(linea)).Read(); e != nil || !cabeceraValida(cab) {
			fmt.Printf("  ⚠ Cabecera inesperada: %q (se continúa asumiendo el orden estándar)\n", strings.TrimSpace(linea))
		}
		buf := make([]byte, 0, tamLote*96)
		inicio, nLote, id := 0, 0, 0
		for {
			l, err := br.ReadSlice('\n')
			buf = append(buf, l...)
			if err == bufio.ErrBufferFull {
				continue // línea más larga que el búfer: seguir acumulándola
			}
			if err != nil && err != io.EOF {
				r.Err = err
				return
			}
			if len(bytes.TrimSpace(buf[inicio:])) == 0 {
				buf = buf[:inicio] // línea en blanco: encoding/csv también la ignora
			} else {
				if err == io.EOF {
					buf = append(buf, '\n')
				}
				fila++
				nLote++
				inicio = len(buf)
				if nLote == tamLote {
					emitir(LoteCrudo{ID: id, FilaInicio: fila - int64(nLote) + 1, Texto: buf}, nLote)
					id++
					buf = make([]byte, 0, tamLote*96)
					inicio, nLote = 0, 0
				}
			}
			if err == io.EOF {
				break
			}
		}
		if nLote > 0 {
			emitir(LoteCrudo{ID: id, FilaInicio: fila - int64(nLote) + 1, Texto: buf}, nLote)
		}
		r.Filas = fila
		return
	}

	cr := csv.NewReader(br)
	cr.FieldsPerRecord = -1 // no abortar con filas truncadas: las clasifica el worker
	cr.LazyQuotes = true
	cr.ReuseRecord = false // cada registro se conserva dentro de su lote

	cab, err := cr.Read()
	if err != nil {
		r.Err = fmt.Errorf("no se pudo leer la cabecera: %w", err)
		return
	}
	if !cabeceraValida(cab) {
		fmt.Printf("  ⚠ Cabecera inesperada: %v (se continúa asumiendo el orden estándar)\n", cab)
	}

	lote := LoteCrudo{ID: 0, FilaInicio: 1, Registros: make([][]string, 0, tamLote)}
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		fila++
		if err != nil {
			var pe *csv.ParseError
			if !errors.As(err, &pe) {
				r.Err = err
				return
			}
			r.Malformadas++
			rec = nil // se envía vacía para que el worker la cuente como inválida
		}
		lote.Registros = append(lote.Registros, rec)
		if len(lote.Registros) == tamLote {
			emitir(lote, tamLote)
			lote = LoteCrudo{ID: lote.ID + 1, FilaInicio: fila + 1, Registros: make([][]string, 0, tamLote)}
		}
	}
	if len(lote.Registros) > 0 {
		emitir(lote, len(lote.Registros))
	}
	r.Filas = fila
}

// parsearBloque: usado por los workers en modo -parseo=workers.
func parsearBloque(texto []byte) [][]string {
	cr := csv.NewReader(bytes.NewReader(texto))
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	regs := make([][]string, 0, bytes.Count(texto, []byte{'\n'}))
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			rec = nil // línea malformada -> el worker la cuenta como ruido
		}
		regs = append(regs, rec)
	}
	return regs
}

func cabeceraValida(c []string) bool {
	if len(c) != len(cabeceraEsperada) {
		return false
	}
	for i := range c {
		if normalizar(c[i]) != cabeceraEsperada[i] {
			return false
		}
	}
	return true
}

// enviarEvento — [S11-C] envío NO bloqueante con select + default
// (patrón nonblocking.go).  Si el monitor está ocupado y su búfer lleno, el
// evento se descarta: la telemetría nunca frena al pipeline.  Las métricas
// oficiales NO dependen de estos eventos (viajan dentro de cada LoteLimpio).
func enviarEvento(eventos chan<- Evento, ev Evento) {
	select {
	case eventos <- ev:
	default:
	}
}

// -----------------------------------------------------------------------------
// 5. ETAPA 2 DEL PIPELINE — WORKERS (consumidores) · Worker Pool
// -----------------------------------------------------------------------------

// worker — [S11-A] cada worker del pool:
//
//	[S10-B] trabajos   <-chan LoteCrudo  : SOLO LECTURA (no puede enviar ni cerrar)
//	[S10-B] resultados chan<- LoteLimpio : SOLO ESCRITURA
//	wg *sync.WaitGroup : se recibe por PUNTERO (copiarlo rompería la sincronización)
func worker(id int, trabajos <-chan LoteCrudo, resultados chan<- LoteLimpio, eventos chan<- Evento,
	wg *sync.WaitGroup, alcance string, verbose bool) {
	defer wg.Done() // [S11-A] avisa al WaitGroup al salir, pase lo que pase

	// [S10-A] `range` sobre el canal: extrae lotes en orden FIFO y termina solo
	// cuando el lector cierra el canal y la cola queda vacía.
	for lote := range trabajos {
		ini := time.Now()
		ll := procesarLote(lote, alcance)
		filas := int(ll.Metricas.FilasLeidas)
		ll.Worker = id
		ll.Duracion = time.Since(ini)
		if verbose {
			fmt.Printf("  [Worker %02d] Lote #%03d │ filas %s │ válidas %s │ descartadas %s │ desagregadas %s │ %s\n",
				id, lote.ID, padL(miles(ll.Metricas.FilasLeidas), 6), padL(miles(ll.Metricas.FilasValidas), 6),
				padL(miles(ll.Metricas.FilasInvalidas), 5), padL(miles(ll.Metricas.Desagregados), 7), fmtDur(ll.Duracion))
		}
		enviarEvento(eventos, Evento{Tipo: evLoteProcesado, Worker: id, Filas: filas, Desag: ll.Metricas.Desagregados})
		resultados <- ll
	}
}

type filaLimpia struct {
	anio, mes        int
	dpto, prov, dist string
	ubigeo           string
	clase            int
	cantidad         int
}

// procesarLote: limpieza + normalización + filtro + DESAGREGACIÓN.
func procesarLote(lote LoteCrudo, alcance string) LoteLimpio {
	ll := LoteLimpio{ID: lote.ID}
	m := &ll.Metricas
	dic := make(map[string]uint16, 64)
	capEst := len(lote.Registros) * 6
	ll.DistLocal = make([]uint16, 0, capEst)
	ll.Mes = make([]uint8, 0, capEst)
	ll.Anio = make([]uint8, 0, capEst)
	ll.Franja = make([]uint8, 0, capEst)
	ll.Clase = make([]uint8, 0, capEst)

	if lote.Texto != nil {
		lote.Registros = parsearBloque(lote.Texto) // parseo CSV en paralelo (dentro del worker)
	}
	for k, rec := range lote.Registros {
		m.FilasLeidas++
		f, motivo, reparada := limpiarFila(rec)
		if motivo >= 0 {
			m.FilasInvalidas++
			m.Motivos[motivo]++
			continue
		}
		if reparada {
			m.FilasReparadas++
		}
		c := int64(f.cantidad)
		m.DenunciasNacionales += c
		if f.clase < 0 {
			m.PorModalidadNacional[NumClases] += c
		} else {
			m.PorModalidadNacional[f.clase] += c
		}
		if !enAlcance(&f, alcance) {
			m.FilasFueraAlcance++
			m.DenunciasFueraAlcance += c
			continue
		}
		if f.clase < 0 {
			m.FilasNoClasificables++
			m.DenunciasNoClasificables += c
			continue
		}
		m.FilasValidas++

		id, ok := dic[f.ubigeo]
		if !ok {
			id = uint16(len(ll.Ubigeos))
			dic[f.ubigeo] = id
			ll.Ubigeos = append(ll.Ubigeos, f.ubigeo)
			ll.NomDist = append(ll.NomDist, f.dist)
			ll.DptoDist = append(ll.DptoDist, f.dpto)
		}

		// DESAGREGACIÓN: 1 fila con cantidad = c  ->  c denuncias individuales.
		// La semilla depende SOLO del nº global de fila: el resultado es idéntico
		// con 1 o con N workers y sin importar el orden en que se procesen los lotes.
		estado := uint64(lote.FilaInicio+int64(k))*0x9E3779B97F4A7C15 ^ 0x5EED2026
		acum := &acumFranjas[f.clase]
		for j := 0; j < f.cantidad; j++ {
			u := float64(splitmix64(&estado)>>11) * (1.0 / (1 << 53))
			fr := 0
			for fr < NumFranjas-1 && u >= acum[fr] {
				fr++
			}
			ll.DistLocal = append(ll.DistLocal, id)
			ll.Mes = append(ll.Mes, uint8(f.mes))
			ll.Anio = append(ll.Anio, uint8(f.anio-anioBase))
			ll.Franja = append(ll.Franja, uint8(fr))
			ll.Clase = append(ll.Clase, uint8(f.clase))
		}
		m.Desagregados += c
	}
	return ll
}

// limpiarFila valida y normaliza.  Devuelve motivo = -1 si la fila es válida.
func limpiarFila(rec []string) (filaLimpia, int, bool) {
	var f filaLimpia
	if rec == nil {
		return f, motivoCSV, false
	}
	if len(rec) != 8 {
		return f, motivoColumnas, false
	}
	reparada := false
	for _, s := range rec {
		if strings.TrimSpace(s) != s {
			reparada = true
		}
	}
	f.dpto, f.prov, f.dist = normalizar(rec[2]), normalizar(rec[3]), normalizar(rec[4])
	mod := normalizar(rec[6])
	if f.dpto == "" || f.prov == "" || f.dist == "" || mod == "" || strings.TrimSpace(rec[7]) == "" {
		return f, motivoVacio, false
	}
	if rec[4] != strings.ToUpper(rec[4]) || rec[2] != strings.ToUpper(rec[2]) {
		reparada = true
	}
	var err error
	if f.anio, err = strconv.Atoi(strings.TrimSpace(rec[0])); err != nil || f.anio < anioBase || f.anio > anioMax {
		return f, motivoFecha, false
	}
	if f.mes, err = strconv.Atoi(strings.TrimSpace(rec[1])); err != nil || f.mes < 1 || f.mes > 12 {
		return f, motivoFecha, false
	}
	var ok bool
	if f.ubigeo, ok = normalizarUbigeo(rec[5]); !ok {
		return f, motivoUbigeo, false
	}
	if f.clase, ok = mapaModalidades[mod]; !ok {
		return f, motivoModalidad, false
	}
	if !etiquetasOficiales[rec[6]] {
		reparada = true
	}
	if f.cantidad, err = strconv.Atoi(strings.TrimSpace(rec[7])); err != nil || f.cantidad <= 0 || f.cantidad > maxCantidad {
		return f, motivoCantidad, false
	}
	return f, -1, reparada
}

// normalizar: MAYÚSCULAS, sin tildes (se conserva la Ñ), sin BOM y con espacios simples.
func normalizar(s string) string {
	return strings.Join(strings.Fields(sinTildes.Replace(strings.ToUpper(s))), " ")
}

// normalizarUbigeo: 5 dígitos (cero inicial perdido, p.ej. 10202) -> 6 dígitos.
func normalizarUbigeo(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 5 || len(s) > 6 {
		return "", false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return "", false
		}
	}
	if len(s) == 5 {
		s = "0" + s
	}
	return s, true
}

func enAlcance(f *filaLimpia, alcance string) bool {
	if alcance == "nacional" {
		return true
	}
	return f.dpto == "LIMA METROPOLITANA" || strings.Contains(f.dpto, "CALLAO") || (f.dpto == "LIMA" && f.prov == "LIMA")
}

func splitmix64(x *uint64) uint64 {
	*x += 0x9E3779B97F4A7C15
	z := *x
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// -----------------------------------------------------------------------------
// 6. MONITOR NO BLOQUEANTE CON select
// -----------------------------------------------------------------------------

// monitor — [S11-B] goroutine que escucha VARIOS canales con un único select:
//   - eventos (progreso de lector y workers)
//   - ticker.C (reporte periódico)
//   - parar   (fin del pipeline)
//
// El select sin `default` BLOQUEA la goroutine hasta que algún canal esté listo:
// el planificador de Go la duerme, no consume CPU => no hay busy waiting.
// Solo en el drenado final se usa `default` para vaciar lo pendiente y salir.
func monitor(eventos <-chan Evento, parar <-chan struct{}, terminado chan<- struct{},
	enCola func() int, capCola int, silencioso bool) {
	defer close(terminado)
	ini := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var emitidos, procesados int
	var filas, desag int64
	porWorker := map[int]int{}

	reportar := func(etq string) {
		if silencioso {
			return
		}
		seg := time.Since(ini).Seconds()
		fmt.Printf("  [MONITOR·select] %s t=%5.2fs │ lotes emitidos %3d │ procesados %3d │ cola FIFO %2d/%d │ filas %s │ desagregadas %s │ %s filas/s\n",
			etq, seg, emitidos, procesados, enCola(), capCola, padL(miles(filas), 7), padL(miles(desag), 9), padL(miles(int64(float64(filas)/math.Max(seg, 1e-9))), 9))
	}
	actualizar := func(ev Evento) {
		switch ev.Tipo {
		case evLoteEmitido:
			emitidos++
		case evLoteProcesado:
			procesados++
			filas += int64(ev.Filas)
			desag += ev.Desag
			porWorker[ev.Worker]++
		}
	}

	for {
		select {
		case ev := <-eventos:
			actualizar(ev)
		case <-ticker.C:
			reportar("⟳")
		case <-parar:
			for { // drenado no bloqueante de eventos rezagados
				select {
				case ev := <-eventos:
					actualizar(ev)
				default:
					reportar("✔")
					if !silencioso {
						ids := make([]int, 0, len(porWorker))
						for id := range porWorker {
							ids = append(ids, id)
						}
						sort.Ints(ids)
						var b strings.Builder
						for _, id := range ids {
							fmt.Fprintf(&b, " W%02d=%d", id, porWorker[id])
						}
						fmt.Printf("  [MONITOR·select] Reparto de lotes por worker:%s\n", b.String())
					}
					return
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------
// 7. ORQUESTADOR DEL PIPELINE
// -----------------------------------------------------------------------------

type ResultadoPipeline struct {
	DS       *Dataset
	Met      Metricas
	Lector   ResultadoLector
	Duracion time.Duration
	Lotes    int
}

func ejecutarPipeline(ruta string, cfg Config, numWorkers int, verbose bool) (ResultadoPipeline, error) {
	ini := time.Now()

	// [S10-A] CANALES CON BÚFER = COLAS FIFO.
	// `trabajos` puede retener hasta cfg.Buffer lotes: el lector (I/O de disco)
	// sigue leyendo mientras los workers (CPU) procesan; si la cola se llena, el
	// lector se bloquea (backpressure) y la memoria queda acotada.  Los lotes
	// salen en el mismo orden en que entraron (First-In, First-Out).
	trabajos := make(chan LoteCrudo, cfg.Buffer)
	resultados := make(chan LoteLimpio, cfg.Buffer)
	eventos := make(chan Evento, 1024)
	resLector := make(chan ResultadoLector, 1)
	parar := make(chan struct{})
	monitorFin := make(chan struct{})

	go monitor(eventos, parar, monitorFin, func() int { return len(trabajos) }, cap(trabajos), !verbose)

	// [S10-C] Etapa 1: productor
	go lector(ruta, cfg.TamLote, cfg.Parseo == "workers", trabajos, eventos, resLector)

	// [S11-A] Etapa 2: Worker Pool de N goroutines sincronizadas con WaitGroup
	var wg sync.WaitGroup
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, trabajos, resultados, eventos, &wg, cfg.Alcance, verbose)
	}
	// Goroutine "cerradora": cuando TODOS los workers terminan, cierra resultados.
	go func() {
		wg.Wait()
		close(resultados) // [S10-C] solo se cierra cuando ya nadie puede escribir
	}()

	// [S10-C] Etapa 3: recolector (goroutine principal).  Termina cuando se cierra `resultados`.
	lotes := make([]LoteLimpio, 0, 64)
	for ll := range resultados {
		lotes = append(lotes, ll)
	}
	rl := <-resLector
	close(parar)
	<-monitorFin
	if rl.Err != nil {
		return ResultadoPipeline{}, rl.Err
	}

	ds, met := fusionar(lotes, numWorkers)
	return ResultadoPipeline{DS: ds, Met: met, Lector: rl, Duracion: time.Since(ini), Lotes: len(lotes)}, nil
}

// fusionar une los lotes en un Dataset global (orden determinista por ID de lote).
// La remapeo de diccionarios es secuencial (barato: 1 entrada por distrito por lote);
// la copia de columnas (~2 M elementos) se hace en paralelo.
func fusionar(lotes []LoteLimpio, workers int) (*Dataset, Metricas) {
	sort.Slice(lotes, func(i, j int) bool { return lotes[i].ID < lotes[j].ID })
	var met Metricas
	ds := &Dataset{}
	global := map[string]uint16{}
	remaps := make([][]uint16, len(lotes))
	offsets := make([]int, len(lotes)+1)
	for i := range lotes {
		met.sumar(&lotes[i].Metricas)
		rm := make([]uint16, len(lotes[i].Ubigeos))
		for j, u := range lotes[i].Ubigeos {
			g, ok := global[u]
			if !ok {
				g = uint16(len(ds.Ubigeo))
				global[u] = g
				ds.Ubigeo = append(ds.Ubigeo, u)
				ds.NomDist = append(ds.NomDist, lotes[i].NomDist[j])
				ds.DptoDist = append(ds.DptoDist, lotes[i].DptoDist[j])
			}
			rm[j] = g
		}
		remaps[i] = rm
		offsets[i+1] = offsets[i] + len(lotes[i].Clase)
	}
	n := offsets[len(lotes)]
	ds.N = n
	ds.Dist = make([]uint16, n)
	ds.Mes = make([]uint8, n)
	ds.Anio = make([]uint8, n)
	ds.Franja = make([]uint8, n)
	ds.Clase = make([]uint8, n)

	// [S11-D] Semáforo con canal con búfer: como máximo `workers` copias simultáneas.
	sem := make(chan struct{}, workers)
	ultimos := make([]int, len(lotes))
	var wg sync.WaitGroup
	for i := range lotes {
		wg.Add(1)
		sem <- struct{}{} // adquirir
		go func(i int) {
			defer func() { <-sem; wg.Done() }() // liberar
			l := &lotes[i]
			o := offsets[i]
			rm := remaps[i]
			ult := 0
			for k, d := range l.DistLocal {
				ds.Dist[o+k] = rm[d]
				t := (int(l.Anio[k])+anioBase)*12 + int(l.Mes[k]) - 1
				if t > ult {
					ult = t
				}
			}
			copy(ds.Mes[o:], l.Mes)
			copy(ds.Anio[o:], l.Anio)
			copy(ds.Franja[o:], l.Franja)
			copy(ds.Clase[o:], l.Clase)
			ultimos[i] = ult // cada goroutine escribe SOLO su índice: sin carrera
		}(i)
	}
	wg.Wait()
	for _, u := range ultimos {
		if u > ds.UltimoMes {
			ds.UltimoMes = u
		}
	}

	// Zona urbana por distrito
	zonaID := map[string]uint16{}
	ds.ZonaDist = make([]uint16, len(ds.Ubigeo))
	for d := range ds.Ubigeo {
		z := zonaDe(ds.DptoDist[d], ds.NomDist[d])
		id, ok := zonaID[z]
		if !ok {
			id = uint16(len(ds.NomZona))
			zonaID[z] = id
			ds.NomZona = append(ds.NomZona, z)
		}
		ds.ZonaDist[d] = id
	}
	return ds, met
}

func zonaDe(dpto, dist string) string {
	if strings.Contains(dpto, "CALLAO") {
		return "CALLAO"
	}
	if dpto == "LIMA METROPOLITANA" || dpto == "LIMA" {
		if z, ok := zonasLima[dist]; ok {
			return z
		}
		return "LIMA (OTROS)"
	}
	return dpto
}

// -----------------------------------------------------------------------------
// 8. MAP-REDUCE: conteos condicionales [Distrito × Franja × Delito]
// -----------------------------------------------------------------------------

type Conteo [][NumFranjas][NumClases]int64

// conteoCondicional — [S11-E] Map-Reduce sin locks: el dataset se parte en N
// rangos contiguos; cada goroutine acumula en SU propia tabla (MAP) y luego
// la goroutine principal suma las N tablas (REDUCE).  Cero contención.
func conteoCondicional(ds *Dataset, workers int) Conteo {
	nd := len(ds.Ubigeo)
	parciales := make([]Conteo, workers)
	var wg sync.WaitGroup
	paso := (ds.N + workers - 1) / workers
	for w := 0; w < workers; w++ {
		ini, fin := w*paso, min((w+1)*paso, ds.N)
		wg.Add(1)
		go func(w, ini, fin int) {
			defer wg.Done()
			t := make(Conteo, nd)
			dist, fr, cl := ds.Dist[ini:fin], ds.Franja[ini:fin], ds.Clase[ini:fin]
			for i := range dist {
				t[dist[i]][fr[i]][cl[i]]++
			}
			parciales[w] = t
		}(w, ini, max(ini, fin))
	}
	wg.Wait()
	total := make(Conteo, nd)
	for _, p := range parciales {
		for d := range p {
			for f := 0; f < NumFranjas; f++ {
				for c := 0; c < NumClases; c++ {
					total[d][f][c] += p[d][f][c]
				}
			}
		}
	}
	return total
}

// -----------------------------------------------------------------------------
// 9. RANDOM FOREST (Bagging + Data Parallelism)
// -----------------------------------------------------------------------------

var nombresFeatures = [NumFeatures]string{"Distrito (UBIGEO)", "Zona urbana", "Franja horaria", "Mes", "Año"}

// Categóricas: el orden de las categorías se elige en cada nodo según la
// proporción de la clase mayoritaria (heurística CART).  Ordinales: umbral v <= t
// en orden natural (un año no visto en el train cae del lado "mayor").
var featureOrdinal = [NumFeatures]bool{false, false, false, true, true}

type Nodo struct {
	Hoja    bool
	Feature int
	Izq     []bool // Izq[v] == true  => el valor v va al hijo izquierdo
	HI, HD  int32
	Prob    [NumClases]float32
}

type Arbol struct {
	Nodos       []Nodo
	Importancia [NumFeatures]float64
	TamMuestra  int
}

type Bosque struct {
	Arboles []*Arbol
}

type MatrizRF struct {
	Cols  [NumFeatures][]uint16
	Cards [NumFeatures]int
	Y     []uint8
}

func construirMatriz(ds *Dataset) *MatrizRF {
	m := &MatrizRF{Y: ds.Clase}
	for f := range m.Cols {
		m.Cols[f] = make([]uint16, ds.N)
	}
	for i := 0; i < ds.N; i++ {
		d := ds.Dist[i]
		m.Cols[0][i] = d
		m.Cols[1][i] = ds.ZonaDist[d]
		m.Cols[2][i] = uint16(ds.Franja[i])
		m.Cols[3][i] = uint16(ds.Mes[i])
		m.Cols[4][i] = uint16(ds.Anio[i])
	}
	m.Cards = [NumFeatures]int{len(ds.Ubigeo), len(ds.NomZona), NumFranjas, 13, anioMax - anioBase + 2}
	return m
}

// dividirTemporal: train = todos los años anteriores; test = último año del dataset.
func dividirTemporal(ds *Dataset) (train, test []int32, anioTest int) {
	anioTest = ds.UltimoMes/12 - anioBase
	for i := 0; i < ds.N; i++ {
		if int(ds.Anio[i]) == anioTest {
			test = append(test, int32(i))
		} else {
			train = append(train, int32(i))
		}
	}
	return train, test, anioTest + anioBase
}

// entrenarBosque — [S11-A] Worker Pool de árboles:
//   - `trabajos` (buffer = nº de árboles) es una cola FIFO de IDs de árbol.
//   - Cada worker toma un ID, genera SU muestra bootstrap con SU semilla
//     (Semilla + ID) y entrena el árbol: Data Parallelism sin memoria compartida
//     de escritura (la matriz de features es de solo lectura).
//   - El resultado se guarda en arboles[id]: cada goroutine escribe un índice
//     distinto => sin carreras y bosque IDÉNTICO con 1 o con N workers.
func entrenarBosque(m *MatrizRF, train []int32, cfg ConfigRF, workers int) *Bosque {
	arboles := make([]*Arbol, cfg.NumArboles)
	trabajos := make(chan int, cfg.NumArboles)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(trabajos <-chan int) { // [S10-B] el worker solo puede LEER IDs
			defer wg.Done()
			for id := range trabajos {
				arboles[id] = entrenarArbol(m, train, cfg, cfg.Semilla+int64(id)*7919)
			}
		}(trabajos)
	}
	for id := 0; id < cfg.NumArboles; id++ {
		trabajos <- id
	}
	close(trabajos)
	wg.Wait()
	return &Bosque{Arboles: arboles}
}

type constructor struct {
	m      *MatrizRF
	cfg    ConfigRF
	rng    *rand.Rand
	hist   []int64
	tot    []int64
	orden  []int
	mejorO []int
	nodos  []Nodo
	imp    [NumFeatures]float64
	feats  [NumFeatures]int
}

func entrenarArbol(m *MatrizRF, train []int32, cfg ConfigRF, semilla int64) *Arbol {
	rng := rand.New(rand.NewSource(semilla))
	n := int(float64(len(train)) * cfg.Fraccion)
	if n < 2*cfg.MinHoja {
		n = len(train)
	}
	// Bootstrap: muestreo CON reemplazo
	idx := make([]int32, n)
	for i := range idx {
		idx[i] = train[rng.Intn(len(train))]
	}
	maxCard := 0
	for _, c := range m.Cards {
		maxCard = max(maxCard, c)
	}
	c := &constructor{m: m, cfg: cfg, rng: rng,
		hist: make([]int64, maxCard*NumClases), tot: make([]int64, maxCard),
		orden: make([]int, 0, maxCard), mejorO: make([]int, 0, maxCard), feats: [NumFeatures]int{0, 1, 2, 3, 4}}
	c.construir(idx, 0)
	return &Arbol{Nodos: c.nodos, Importancia: c.imp, TamMuestra: n}
}

func (c *constructor) construir(idx []int32, prof int) int32 {
	var cnt [NumClases]int64
	y := c.m.Y
	for _, i := range idx {
		cnt[y[i]]++
	}
	n := int64(len(idx))
	pos := int32(len(c.nodos))
	nodo := Nodo{Hoja: true}
	maj, puro := 0, 0
	for k := 0; k < NumClases; k++ {
		nodo.Prob[k] = float32(cnt[k]) / float32(n)
		if cnt[k] > cnt[maj] {
			maj = k
		}
		if cnt[k] > 0 {
			puro++
		}
	}
	c.nodos = append(c.nodos, nodo)
	minH := int64(c.cfg.MinHoja)
	if prof >= c.cfg.ProfMax || n < 2*minH || puro <= 1 {
		return pos
	}

	// Criterio de Gini.  Maximizar  Σ_k cL_k²/nL + Σ_k cR_k²/nR  equivale a
	// minimizar la impureza ponderada de los hijos.
	padre := 0.0
	for k := 0; k < NumClases; k++ {
		padre += float64(cnt[k]) * float64(cnt[k])
	}
	padre /= float64(n)
	mejor, mejorF := padre+1e-9, -1

	// Subconjunto aleatorio de mtry features (Fisher-Yates parcial, sin asignar memoria)
	feats := &c.feats
	for i := 0; i < c.cfg.Mtry; i++ {
		j := i + c.rng.Intn(NumFeatures-i)
		feats[i], feats[j] = feats[j], feats[i]
	}
	for _, f := range feats[:c.cfg.Mtry] {
		col := c.m.Cols[f]
		h, t := c.hist, c.tot
		orden := c.orden[:0]
		for _, i := range idx {
			v := int(col[i])
			if t[v] == 0 {
				orden = append(orden, v)
			}
			t[v]++
			h[v*NumClases+int(y[i])]++
		}
		if len(orden) >= 2 {
			if featureOrdinal[f] {
				sort.Ints(orden)
			} else {
				sort.Slice(orden, func(a, b int) bool {
					va, vb := orden[a], orden[b]
					return h[va*NumClases+maj]*t[vb] < h[vb*NumClases+maj]*t[va]
				})
			}
			var izq [NumClases]int64
			var nIzq int64
			corteF, scoreF := -1, mejor
			for p := 0; p < len(orden)-1; p++ {
				v := orden[p]
				for k := 0; k < NumClases; k++ {
					izq[k] += h[v*NumClases+k]
				}
				nIzq += t[v]
				nDer := n - nIzq
				if nIzq < minH || nDer < minH {
					continue
				}
				var sl, sr float64
				for k := 0; k < NumClases; k++ {
					l, r := float64(izq[k]), float64(cnt[k]-izq[k])
					sl += l * l
					sr += r * r
				}
				if s := sl/float64(nIzq) + sr/float64(nDer); s > scoreF {
					scoreF, corteF = s, p
				}
			}
			if corteF >= 0 {
				mejor, mejorF = scoreF, f
				c.mejorO = append(c.mejorO[:0], orden[:corteF+1]...)
			}
		}
		for _, v := range orden { // limpiar solo lo que se tocó
			t[v] = 0
			for k := 0; k < NumClases; k++ {
				h[v*NumClases+k] = 0
			}
		}
		c.orden = orden
	}
	if mejorF < 0 {
		return pos
	}
	c.imp[mejorF] += mejor - padre // reducción de impureza Gini (ponderada por nº de muestras)

	set := make([]bool, c.m.Cards[mejorF])
	for _, v := range c.mejorO {
		set[v] = true
	}
	col := c.m.Cols[mejorF]
	i, j := 0, len(idx)-1
	for i <= j { // partición in-place
		if set[col[idx[i]]] {
			i++
		} else {
			idx[i], idx[j] = idx[j], idx[i]
			j--
		}
	}
	c.nodos[pos].Hoja = false
	c.nodos[pos].Feature = mejorF
	c.nodos[pos].Izq = set
	hi := c.construir(idx[:i], prof+1)
	hd := c.construir(idx[i:], prof+1)
	c.nodos[pos].HI, c.nodos[pos].HD = hi, hd
	return pos
}

func (a *Arbol) predecir(x *[NumFeatures]uint16) *[NumClases]float32 {
	n := &a.Nodos[0]
	for !n.Hoja {
		v := int(x[n.Feature])
		if v < len(n.Izq) && n.Izq[v] {
			n = &a.Nodos[n.HI]
		} else {
			n = &a.Nodos[n.HD]
		}
	}
	return &n.Prob
}

// probas: votación "suave" (promedio de probabilidades de todos los árboles).
func (b *Bosque) probas(x *[NumFeatures]uint16) [NumClases]float32 {
	var p [NumClases]float32
	for _, a := range b.Arboles {
		q := a.predecir(x)
		for k := range p {
			p[k] += q[k]
		}
	}
	inv := 1 / float32(len(b.Arboles))
	for k := range p {
		p[k] *= inv
	}
	return p
}

func argmax(p []float32) int {
	b := 0
	for k := range p {
		if p[k] > p[b] {
			b = k
		}
	}
	return b
}

// evaluar: predicción paralela del conjunto de prueba (cada goroutine un rango,
// matriz de confusión local, reducción al final).
func evaluar(b *Bosque, m *MatrizRF, test []int32, workers int) [NumClases][NumClases]int64 {
	parc := make([][NumClases][NumClases]int64, workers)
	paso := (len(test) + workers - 1) / workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		ini, fin := min(w*paso, len(test)), min((w+1)*paso, len(test))
		wg.Add(1)
		go func(w int, part []int32) {
			defer wg.Done()
			var x [NumFeatures]uint16
			for _, i := range part {
				for f := 0; f < NumFeatures; f++ {
					x[f] = m.Cols[f][i]
				}
				p := b.probas(&x)
				parc[w][m.Y[i]][argmax(p[:])]++
			}
		}(w, test[ini:fin])
	}
	wg.Wait()
	var cm [NumClases][NumClases]int64
	for _, p := range parc {
		for r := range p {
			for c := range p[r] {
				cm[r][c] += p[r][c]
			}
		}
	}
	return cm
}

func firmaBosque(b *Bosque) (int, float64) {
	nodos, s := 0, 0.0
	for _, a := range b.Arboles {
		nodos += len(a.Nodos)
		for _, n := range a.Nodos {
			if n.Hoja {
				s += float64(n.Prob[0]) + 2*float64(n.Prob[1])
			}
		}
	}
	return nodos, s
}

// -----------------------------------------------------------------------------
// 10. GENERADOR DE DATASET SINTÉTICO (fallback: mismo formato SIDPOL)
// -----------------------------------------------------------------------------

type distGen struct {
	dpto, prov, dist string
	ubigeo           int
	total            float64 // denuncias esperadas en todo el periodo
	shares           *[7]float64
}

var etiquetasGen = [7]string{"Otros", "Violencia contra la mujer e integrantes", "Hurto", "Robo", "Estafa", "Extorsión", "Secuestro"}

// Proporciones por modalidad observadas en el archivo oficial (2018-2026).
var sharesLima = [7]float64{0.322, 0.272, 0.221, 0.132, 0.0343, 0.0173, 0.0014}
var sharesRegiones = [7]float64{0.395, 0.303, 0.186, 0.073, 0.027, 0.0144, 0.0013}

// Volumen total 2018-2026 por distrito (miles de denuncias) — orden de magnitud real.
var limaGen = []struct {
	dist   string
	ubigeo int
	miles  float64
}{
	{"LIMA", 150101, 260}, {"ANCON", 150102, 15.5}, {"ATE", 150103, 134}, {"BARRANCO", 150104, 22.8},
	{"BREÑA", 150105, 38.6}, {"CARABAYLLO", 150106, 82}, {"CHACLACAYO", 150107, 14.5}, {"CHORRILLOS", 150108, 80},
	{"CIENEGUILLA", 150109, 7.8}, {"COMAS", 150110, 150}, {"EL AGUSTINO", 150111, 62}, {"INDEPENDENCIA", 150112, 93},
	{"JESUS MARIA", 150113, 36.5}, {"LA MOLINA", 150114, 32.6}, {"LA VICTORIA", 150115, 79}, {"LINCE", 150116, 25.8},
	{"LOS OLIVOS", 150117, 119}, {"LURIGANCHO - CHOSICA", 150118, 52.6}, {"LURIN", 150119, 25.2}, {"MAGDALENA DEL MAR", 150120, 19.7},
	{"PUEBLO LIBRE", 150121, 23.7}, {"MIRAFLORES", 150122, 61.9}, {"PACHACAMAC", 150123, 25.3}, {"PUCUSANA", 150124, 4.5},
	{"PUENTE PIEDRA", 150125, 71}, {"PUNTA HERMOSA", 150126, 4.4}, {"PUNTA NEGRA", 150127, 2.2}, {"RIMAC", 150128, 65.8},
	{"SAN BARTOLO", 150129, 3.4}, {"SAN BORJA", 150130, 31.7}, {"SAN ISIDRO", 150131, 31.4}, {"SAN JUAN DE LURIGANCHO", 150132, 266},
	{"SAN JUAN DE MIRAFLORES", 150133, 90}, {"SAN LUIS", 150134, 20.7}, {"SAN MARTIN DE PORRES", 150135, 135}, {"SAN MIGUEL", 150136, 35},
	{"SANTA ANITA", 150137, 60.6}, {"SANTA MARIA DEL MAR", 150138, 0.4}, {"SANTA ROSA", 150139, 5.1}, {"SANTIAGO DE SURCO", 150140, 111},
	{"SURQUILLO", 150141, 30.8}, {"VILLA EL SALVADOR", 150142, 97}, {"VILLA MARIA DEL TRIUNFO", 150143, 119},
}
var callaoGen = []struct {
	dist   string
	ubigeo int
	miles  float64
}{
	{"CALLAO", 70101, 181}, {"BELLAVISTA", 70102, 20.6}, {"CARMEN DE LA LEGUA REYNOSO", 70103, 8.1},
	{"LA PERLA", 70104, 13.5}, {"LA PUNTA", 70105, 1.6}, {"VENTANILLA", 70106, 51}, {"MI PERU", 70107, 5.7},
}
var regionesGen = []struct {
	dpto   string
	codigo int
	miles  float64
}{
	{"AMAZONAS", 1, 79}, {"ANCASH", 2, 250}, {"APURIMAC", 3, 99}, {"AREQUIPA", 4, 519}, {"AYACUCHO", 5, 121},
	{"CAJAMARCA", 6, 185}, {"CUSCO", 8, 305}, {"HUANCAVELICA", 9, 32}, {"HUANUCO", 10, 148}, {"ICA", 11, 273},
	{"JUNIN", 12, 308}, {"LA LIBERTAD", 13, 390}, {"LAMBAYEQUE", 14, 443}, {"REGION LIMA", 15, 225}, {"LORETO", 16, 119},
	{"MADRE DE DIOS", 17, 59}, {"MOQUEGUA", 18, 55}, {"PASCO", 19, 40}, {"PIURA", 20, 375}, {"PUNO", 21, 140},
	{"SAN MARTIN", 22, 143}, {"TACNA", 23, 100}, {"TUMBES", 24, 72}, {"UCAYALI", 25, 101},
}

// generarDatasetSintetico escribe con bufio.Writer un CSV con la MISMA estructura
// del archivo SIDPOL (8 columnas, agregado por cantidad) y ~4 % de ruido.
func generarDatasetSintetico(ruta string) (filas, denuncias int64, dur time.Duration, err error) {
	ini := time.Now()
	f, err := os.Create(ruta)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 4<<20)
	rng := rand.New(rand.NewSource(2026))

	var ds []distGen
	for _, d := range limaGen {
		ds = append(ds, distGen{"LIMA METROPOLITANA", "LIMA", d.dist, d.ubigeo, d.miles * 1000, &sharesLima})
	}
	for _, d := range callaoGen {
		ds = append(ds, distGen{"PROV. CONST. DEL CALLAO", "CALLAO", d.dist, d.ubigeo, d.miles * 1000, &sharesLima})
	}
	const distPorRegion = 19
	for _, r := range regionesGen {
		z := 0.0
		for k := 0; k < distPorRegion; k++ {
			z += 1 / math.Pow(float64(k+1), 0.9)
		}
		for k := 0; k < distPorRegion; k++ {
			peso := 1 / math.Pow(float64(k+1), 0.9) / z
			ub := r.codigo*10000 + (k/6+2)*100 + k%6 + 1
			ds = append(ds, distGen{r.dpto, r.dpto, fmt.Sprintf("%s DISTRITO %02d", r.dpto, k+1), ub, r.miles * 1000 * peso, &sharesRegiones})
		}
	}

	w.WriteString(`"ANIO","MES","DPTO_HECHO_NEW","PROV_HECHO","DIST_HECHO","UBIGEO_HECHO","P_MODALIDADES","cantidad"` + "\r\n")
	const meses = 105 // ene-2018 .. sep-2026
	var campos [8]string
	for anio := 2018; anio <= 2026; anio++ {
		for mes := 1; mes <= 12; mes++ {
			if anio == 2026 && mes > 9 {
				break
			}
			for _, d := range ds {
				for m := 0; m < 7; m++ {
					c := muestrearConteo(rng, d.total*d.shares[m]/meses)
					if c <= 0 {
						continue
					}
					campos = [8]string{strconv.Itoa(anio), strconv.Itoa(mes), d.dpto, d.prov, d.dist,
						strconv.Itoa(d.ubigeo), etiquetasGen[m], strconv.Itoa(c)}
					nCampos := 8
					if rng.Float64() < 0.04 {
						nCampos = corromper(rng, &campos)
					}
					for i := 0; i < nCampos; i++ {
						if i > 0 {
							w.WriteByte(',')
						}
						if i == 7 {
							w.WriteString(campos[i])
						} else {
							w.WriteByte('"')
							w.WriteString(campos[i])
							w.WriteByte('"')
						}
					}
					w.WriteString("\r\n")
					filas++
					denuncias += int64(c)
				}
			}
		}
	}
	if err = w.Flush(); err != nil {
		return
	}
	return filas, denuncias, time.Since(ini), nil
}

// corromper inyecta ruido: 6 tipos irrecuperables + 2 recuperables (formato desordenado).
func corromper(rng *rand.Rand, c *[8]string) int {
	switch rng.Intn(8) {
	case 0:
		if rng.Intn(2) == 0 {
			c[4] = ""
		} else {
			c[6] = "  "
		}
	case 1:
		c[7] = []string{"N/D", "s/d", "#N/A", "3,5"}[rng.Intn(4)]
	case 2:
		c[7] = []string{"0", "-3", "-1"}[rng.Intn(3)]
	case 3:
		c[1] = []string{"13", "0", "Set"}[rng.Intn(3)]
	case 4:
		c[5] = []string{"15X1A", "", "1234567", "UBIG"}[rng.Intn(4)]
	case 5:
		return 5 + rng.Intn(2) // fila truncada
	case 6: // recuperable: minúsculas + espacios + sin tilde
		c[4] = "  " + strings.ToLower(c[4]) + " "
		c[6] = " " + strings.ToLower(sinTildes.Replace(strings.ToUpper(c[6]))) + "  "
	case 7: // recuperable: espacios en numéricos + MAYÚSCULAS
		c[0] = " " + c[0] + " "
		c[6] = strings.ToUpper(c[6])
		c[7] = c[7] + " "
	}
	return 8
}

func muestrearConteo(rng *rand.Rand, mu float64) int {
	if mu <= 0 {
		return 0
	}
	mu *= 0.7 + 0.6*rng.Float64()
	if mu < 30 { // Poisson (Knuth)
		L, k, p := math.Exp(-mu), 0, 1.0
		for {
			p *= rng.Float64()
			if p <= L {
				return k
			}
			k++
		}
	}
	return int(math.Round(mu + math.Sqrt(mu)*rng.NormFloat64()))
}

func resolverDataset(flagRuta string) (ruta string, sintetico bool, err error) {
	existe := func(p string) bool {
		st, e := os.Stat(p)
		return e == nil && !st.IsDir()
	}
	if flagRuta != "" {
		if existe(flagRuta) {
			return flagRuta, false, nil
		}
		fmt.Printf("  ⚠ No existe %q; se buscará en el directorio actual.\n", flagRuta)
	}
	for _, c := range candidatosCSV {
		if existe(c) {
			return c, false, nil
		}
	}
	if m, _ := filepath.Glob("DATASET_Denuncias*Policiales*.csv"); len(m) > 0 {
		for _, c := range m {
			if c != rutaSintetica {
				return c, false, nil
			}
		}
	}
	if existe(rutaSintetica) {
		return rutaSintetica, true, nil
	}
	fmt.Printf("  ⚠ No se encontró el CSV oficial. Generando dataset SINTÉTICO con el mismo formato…\n")
	filas, den, dur, err := generarDatasetSintetico(rutaSintetica)
	if err != nil {
		return "", true, err
	}
	fmt.Printf("  ✔ Generado %s │ %s filas agregadas │ Σ cantidad = %s │ %s (bufio.Writer)\n",
		rutaSintetica, miles(filas), miles(den), fmtDur(dur))
	return rutaSintetica, true, nil
}

// -----------------------------------------------------------------------------
// 11. UTILIDADES DE FORMATO (miles con coma, decimales con punto)
// -----------------------------------------------------------------------------

func miles(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	s := strconv.FormatInt(n, 10)
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	pre := len(s) % 3
	if pre == 0 {
		pre = 3
	}
	b.WriteString(s[:pre])
	for i := pre; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func dec(f float64, d int) string {
	s := strconv.FormatFloat(math.Abs(f), 'f', d, 64)
	ent, frac := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		ent, frac = s[:i], s[i:]
	}
	v, _ := strconv.ParseInt(ent, 10, 64)
	r := miles(v) + frac
	if f < 0 {
		r = "-" + r
	}
	return r
}

func pct(a, b int64) string {
	if b == 0 {
		return "0.00%"
	}
	return dec(100*float64(a)/float64(b), 2) + "%"
}

func fmtDur(d time.Duration) string {
	if d < time.Second {
		return dec(float64(d.Microseconds())/1000, 1) + " ms"
	}
	return dec(d.Seconds(), 3) + " s"
}

func padR(s string, w int) string {
	n := utf8.RuneCountInString(s)
	if n >= w {
		return s
	}
	return s + strings.Repeat(" ", w-n)
}

func padL(s string, w int) string {
	n := utf8.RuneCountInString(s)
	if n >= w {
		return s
	}
	return strings.Repeat(" ", w-n) + s
}

func trunc(s string, w int) string {
	if utf8.RuneCountInString(s) <= w {
		return s
	}
	r := []rune(s)
	return string(r[:w-1]) + "…"
}

func barra(frac float64, ancho int) string {
	n := int(math.Round(frac * float64(ancho)))
	n = max(0, min(n, ancho))
	return strings.Repeat("█", n) + strings.Repeat("░", ancho-n)
}

func titulo(s string) {
	fmt.Println()
	fmt.Println("╔" + strings.Repeat("═", 104) + "╗")
	fmt.Println("║ " + padR(s, 103) + "║")
	fmt.Println("╚" + strings.Repeat("═", 104) + "╝")
}

func subtitulo(s string) {
	fmt.Printf("\n  ▸ %s\n", s)
}

// tabla imprime filas con anchos fijos y alineación ('l' / 'r').
func tabla(anchos []int, alin string, cab []string, filas [][]string) {
	sep := func(i, m, d string) {
		var b strings.Builder
		b.WriteString("  " + i)
		for k, a := range anchos {
			b.WriteString(strings.Repeat("─", a+2))
			if k < len(anchos)-1 {
				b.WriteString(m)
			}
		}
		b.WriteString(d)
		fmt.Println(b.String())
	}
	linea := func(cs []string) {
		var b strings.Builder
		b.WriteString("  │")
		for k, a := range anchos {
			v := ""
			if k < len(cs) {
				v = cs[k]
			}
			if alin[k] == 'r' {
				b.WriteString(" " + padL(v, a) + " │")
			} else {
				b.WriteString(" " + padR(v, a) + " │")
			}
		}
		fmt.Println(b.String())
	}
	sep("┌", "┬", "┐")
	linea(cab)
	sep("├", "┼", "┤")
	for _, f := range filas {
		if f == nil {
			sep("├", "┼", "┤")
			continue
		}
		linea(f)
	}
	sep("└", "┴", "┘")
}

// -----------------------------------------------------------------------------
// 12. MAIN
// -----------------------------------------------------------------------------

type FilaBench struct {
	Etapa     string
	Unidad    string
	Registros int64
	T1, TN    time.Duration
	Iguales   bool
}

func mejorTiempo(reps int, fn func() time.Duration) time.Duration {
	mejor := time.Duration(math.MaxInt64)
	for r := 0; r < reps; r++ {
		runtime.GC() // mismas condiciones de heap en cada medición
		if d := fn(); d < mejor {
			mejor = d
		}
	}
	return mejor
}

func main() {
	cfg := leerFlags()
	N := cfg.Workers
	tIni := time.Now()

	titulo("CC65 · PC3 · CASO 2 — Clasificación de Delitos Urbanos (SIDPOL-MININTER/PNP) · Go " + runtime.Version())
	fmt.Printf("  CPU lógicas: %d │ GOMAXPROCS: %d │ Workers (N): %d │ Lote: %s filas │ Búfer FIFO: %d lotes │ Alcance: %s │ Parseo CSV: %s\n",
		runtime.NumCPU(), runtime.GOMAXPROCS(0), N, miles(int64(cfg.TamLote)), cfg.Buffer, strings.ToUpper(cfg.Alcance), cfg.Parseo)

	// ───────────────────────── FASE 0: fuente de datos ─────────────────────────
	titulo("FASE 0 · Fuente de datos")
	ruta, sintetico, err := resolverDataset(cfg.RutaCSV)
	if err != nil {
		fmt.Println("  ✖ Error:", err)
		os.Exit(1)
	}
	if st, e := os.Stat(ruta); e == nil {
		tipo := "OFICIAL (Portal de Datos Abiertos)"
		if sintetico {
			tipo = "SINTÉTICO (fallback, mismo formato)"
		}
		fmt.Printf("  Archivo: %s\n  Tipo   : %s │ Tamaño: %s MB\n", ruta, tipo, dec(float64(st.Size())/1e6, 2))
	}

	// ───────────────────────── FASE 1: pipeline concurrente ─────────────────────
	titulo(fmt.Sprintf("FASE 1 · Pipeline concurrente: Lector → Cola FIFO → Pool de %d workers → Recolector", N))
	if cfg.Parseo == "workers" {
		fmt.Println("  Lector (bufio 1 MiB) corta bloques de líneas; cada worker parsea (encoding/csv), limpia, normaliza, filtra y DESAGREGA.")
	} else {
		fmt.Println("  Lector (bufio 1 MiB + encoding/csv) agrupa lotes; los workers limpian, normalizan, filtran y DESAGREGAN.")
	}
	fmt.Println()
	rp, err := ejecutarPipeline(ruta, cfg, N, cfg.Verbose)
	if err != nil {
		fmt.Println("  ✖ Error en el pipeline:", err)
		os.Exit(1)
	}
	ds, met := rp.DS, rp.Met

	subtitulo("Métricas de lectura y limpieza")
	ruido := met.FilasInvalidas
	tabla([]int{58, 14, 10}, "lrr", []string{"Concepto", "Valor", "%"}, [][]string{
		{"Filas leídas del CSV (sin cabecera)", miles(met.FilasLeidas), "100.00%"},
		{"Filas con formato válido", miles(met.FilasLeidas - ruido), pct(met.FilasLeidas-ruido, met.FilasLeidas)},
		{"  · reparadas al normalizar (formato desordenado)", miles(met.FilasReparadas), pct(met.FilasReparadas, met.FilasLeidas)},
		{"Filas DESCARTADAS por ruido / formato inválido", miles(ruido), pct(ruido, met.FilasLeidas)},
		nil,
		{"Σ cantidad nacional detectada (denuncias policiales)", miles(met.DenunciasNacionales), ""},
		{"Filas fuera del alcance geográfico (" + cfg.Alcance + ")", miles(met.FilasFueraAlcance), pct(met.FilasFueraAlcance, met.FilasLeidas)},
		{"Filas no clasificables en alcance (modalidad \"Otros\")", miles(met.FilasNoClasificables), pct(met.FilasNoClasificables, met.FilasLeidas)},
		{"Filas VÁLIDAS para el clasificador", miles(met.FilasValidas), pct(met.FilasValidas, met.FilasLeidas)},
		nil,
		{"Denuncias en alcance excluidas por \"Otros\"", miles(met.DenunciasNoClasificables), ""},
		{"REGISTROS INDIVIDUALES DESAGREGADOS (1 fila = 1 denuncia)", miles(met.Desagregados), pct(met.Desagregados, met.DenunciasNacionales)},
	})
	if ruido > 0 {
		subtitulo("Detalle del ruido eliminado")
		var fs [][]string
		for i := 0; i < numMotivos; i++ {
			if met.Motivos[i] > 0 {
				fs = append(fs, []string{nombresMotivos[i], miles(met.Motivos[i]), pct(met.Motivos[i], ruido)})
			}
		}
		tabla([]int{58, 14, 11}, "lrr", []string{"Motivo de descarte", "Filas", "% del ruido"}, fs)
	}

	segs := rp.Duracion.Seconds()
	subtitulo("Tiempos de la Fase 1")
	tabla([]int{58, 14, 22}, "lrr", []string{"Etapa", "Tiempo", "Throughput"}, [][]string{
		{"Lector (disco → lotes; incluye espera por backpressure)", fmtDur(rp.Lector.Duracion), miles(rp.Lector.Filas) + " filas"},
		{fmt.Sprintf("Pipeline completo (%d lotes, %d workers, fusión incluida)", rp.Lotes, N), fmtDur(rp.Duracion),
			miles(int64(float64(met.FilasLeidas)/segs)) + " filas/s"},
		{"Generación de denuncias individuales", "", miles(int64(float64(met.Desagregados)/segs)) + " reg/s"},
	})

	if ds.N == 0 {
		fmt.Println("  ✖ No quedaron registros válidos en el alcance seleccionado.")
		os.Exit(1)
	}

	// ───────────────────────── FASE 2: análisis criminológico ───────────────────
	titulo("FASE 2 · Análisis criminológico (Map-Reduce concurrente sobre los registros desagregados)")
	conteo := conteoCondicional(ds, N)
	var porClase [NumClases]int64
	var porFranja [NumFranjas]int64
	var franjaClase [NumFranjas][NumClases]int64
	porDist := make([]int64, len(ds.Ubigeo))
	porZona := make([]int64, len(ds.NomZona))
	for d := range conteo {
		for f := 0; f < NumFranjas; f++ {
			for c := 0; c < NumClases; c++ {
				v := conteo[d][f][c]
				porClase[c] += v
				porFranja[f] += v
				franjaClase[f][c] += v
				porDist[d] += v
				porZona[ds.ZonaDist[d]] += v
			}
		}
	}
	total := int64(ds.N)

	subtitulo("Distribución NACIONAL por modalidad (Σ cantidad, todas las filas con formato válido)")
	{
		var fs [][]string
		nombres := append(clasesCortas[:], "Otros (no clasificable)")
		for i, nmb := range nombres {
			v := met.PorModalidadNacional[i]
			fs = append(fs, []string{nmb, miles(v), pct(v, met.DenunciasNacionales), barra(float64(v)/float64(met.DenunciasNacionales), 30)})
		}
		fs = append(fs, nil, []string{"TOTAL", miles(met.DenunciasNacionales), "100.00%", ""})
		tabla([]int{26, 12, 8, 30}, "lrrl", []string{"Modalidad", "Denuncias", "%", ""}, fs)
	}

	subtitulo(fmt.Sprintf("Clases del clasificador — %s denuncias desagregadas (%s)", miles(total), strings.ToUpper(cfg.Alcance)))
	{
		var fs [][]string
		for c := 0; c < NumClases; c++ {
			fs = append(fs, []string{clasesCortas[c], miles(porClase[c]), pct(porClase[c], total), barra(float64(porClase[c])/float64(total), 30)})
		}
		tabla([]int{26, 12, 8, 30}, "lrrl", []string{"Delito (clase)", "Registros", "%", ""}, fs)
	}

	// Top distritos
	orden := make([]int, len(porDist))
	for i := range orden {
		orden[i] = i
	}
	sort.Slice(orden, func(a, b int) bool { return porDist[orden[a]] > porDist[orden[b]] })
	subtitulo("Top 10 distritos por volumen de denuncias")
	{
		var fs [][]string
		for r, d := range orden[:min(10, len(orden))] {
			var pc [NumClases]int64
			var calle int64
			for f := 0; f < NumFranjas; f++ {
				for c := 0; c < NumClases; c++ {
					pc[c] += conteo[d][f][c]
					if esPatrullable[c] {
						calle += conteo[d][f][c]
					}
				}
			}
			top := 0
			for c := range pc {
				if pc[c] > pc[top] {
					top = c
				}
			}
			fs = append(fs, []string{strconv.Itoa(r + 1), trunc(ds.NomDist[d], 24), ds.NomZona[ds.ZonaDist[d]], miles(porDist[d]),
				pct(porDist[d], total), clasesCortas[top] + " (" + pct(pc[top], porDist[d]) + ")", pct(calle, porDist[d])})
		}
		tabla([]int{2, 24, 12, 10, 7, 28, 9}, "rllrrlr", []string{"#", "Distrito", "Zona", "Denuncias", "%", "Delito predominante", "% calle*"}, fs)
		fmt.Println("    * % calle = Hurto + Robo + Extorsión + Secuestro (delitos disuadibles con patrullaje).")
	}

	subtitulo("Distribución por zona urbana")
	{
		oz := make([]int, len(porZona))
		for i := range oz {
			oz[i] = i
		}
		sort.Slice(oz, func(a, b int) bool { return porZona[oz[a]] > porZona[oz[b]] })
		var fs [][]string
		for _, z := range oz[:min(12, len(oz))] {
			fs = append(fs, []string{trunc(ds.NomZona[z], 22), miles(porZona[z]), pct(porZona[z], total), barra(float64(porZona[z])/float64(total), 30)})
		}
		tabla([]int{22, 12, 8, 30}, "lrrl", []string{"Zona", "Denuncias", "%", ""}, fs)
	}

	subtitulo("Distribución por franja horaria (variable DERIVADA — ver supuesto en el código)")
	{
		var fs [][]string
		for f := 0; f < NumFranjas; f++ {
			calle := franjaClase[f][1] + franjaClase[f][2] + franjaClase[f][4] + franjaClase[f][5]
			top := argmax64(franjaClase[f][:])
			fs = append(fs, []string{nombresFranjas[f], miles(porFranja[f]), pct(porFranja[f], total),
				barra(float64(porFranja[f])/float64(total), 24), clasesCortas[top], miles(calle)})
		}
		tabla([]int{16, 11, 7, 24, 19, 11}, "lrrllr", []string{"Franja", "Denuncias", "%", "", "Predominante", "De calle"}, fs)
	}

	subtitulo("Perfil horario por delito: P(franja | delito)")
	{
		var fs [][]string
		for c := 0; c < NumClases; c++ {
			fila := []string{clasesCortas[c]}
			for f := 0; f < NumFranjas; f++ {
				fila = append(fila, pct(franjaClase[f][c], porClase[c]))
			}
			fs = append(fs, fila)
		}
		tabla([]int{20, 10, 10, 10, 10}, "lrrrr", []string{"Delito", franjasCortas[0], franjasCortas[1], franjasCortas[2], franjasCortas[3]}, fs)
	}

	// ───────────────────────── FASE 3: Random Forest ───────────────────────────
	titulo("FASE 3 · Random Forest distribuido (prototipo local: Bagging + Data Parallelism)")
	tM := time.Now()
	mat := construirMatriz(ds)
	train, test, anioTest := dividirTemporal(ds)
	fmt.Printf("  Matriz de features construida en %s │ Features: %s\n", fmtDur(time.Since(tM)), strings.Join(nombresFeatures[:], ", "))
	fmt.Printf("  Split temporal │ Train: %s registros (años < %d) │ Test: %s registros (año %d)\n",
		miles(int64(len(train))), anioTest, miles(int64(len(test))), anioTest)
	if len(train) < 2*cfg.RF.MinHoja || len(test) == 0 {
		fmt.Println("  ✖ Datos insuficientes para entrenar/evaluar.")
		os.Exit(1)
	}
	tamMuestra := int(float64(len(train)) * cfg.RF.Fraccion)
	fmt.Printf("  Bosque │ %d árboles │ profundidad máx %d │ min/hoja %d │ mtry %d de %d │ bootstrap %s registros/árbol (%s del train)\n",
		cfg.RF.NumArboles, cfg.RF.ProfMax, cfg.RF.MinHoja, cfg.RF.Mtry, NumFeatures, miles(int64(tamMuestra)), dec(cfg.RF.Fraccion*100, 0)+"%")

	tRF := time.Now()
	bosque := entrenarBosque(mat, train, cfg.RF, N)
	durRFN := time.Since(tRF)
	fmt.Printf("  ✔ Entrenamiento con %d workers: %s\n", N, fmtDur(durRFN))

	tEv := time.Now()
	cm := evaluar(bosque, mat, test, N)
	durEv := time.Since(tEv)

	var trainClase [NumClases]int64
	for _, i := range train {
		trainClase[ds.Clase[i]]++
	}
	mayor := argmax64(trainClase[:])
	// Línea base: predecir siempre la clase mayoritaria del train.
	var aciertos, nTest, base int64
	for r := 0; r < NumClases; r++ {
		for c := 0; c < NumClases; c++ {
			nTest += cm[r][c]
			if r == c {
				aciertos += cm[r][c]
			}
			if r == mayor {
				base += cm[r][c]
			}
		}
	}
	acc := float64(aciertos) / float64(nTest)
	accBase := float64(base) / float64(nTest)

	subtitulo(fmt.Sprintf("Evaluación sobre %s denuncias de %d (predicción paralela en %s)", miles(nTest), anioTest, fmtDur(durEv)))
	{
		var fs [][]string
		var sumRecall float64
		for k := 0; k < NumClases; k++ {
			var real, pred int64
			for j := 0; j < NumClases; j++ {
				real += cm[k][j]
				pred += cm[j][k]
			}
			prec, rec := 0.0, 0.0
			if pred > 0 {
				prec = float64(cm[k][k]) / float64(pred)
			}
			if real > 0 {
				rec = float64(cm[k][k]) / float64(real)
			}
			f1 := 0.0
			if prec+rec > 0 {
				f1 = 2 * prec * rec / (prec + rec)
			}
			sumRecall += rec
			fs = append(fs, []string{clasesCortas[k], miles(real), miles(pred), dec(prec*100, 1) + "%", dec(rec*100, 1) + "%", dec(f1, 3)})
		}
		tabla([]int{20, 10, 10, 10, 10, 6}, "lrrrrr", []string{"Clase", "Reales", "Predichas", "Precisión", "Recall", "F1"}, fs)
		fmt.Printf("    Exactitud (accuracy) RF: %s%%  │  Línea base (siempre \"%s\"): %s%%  │  Mejora: %s p.p.  │  Recall macro: %s%%\n",
			dec(acc*100, 2), clasesCortas[mayor], dec(accBase*100, 2), dec((acc-accBase)*100, 2), dec(sumRecall/NumClases*100, 2))
	}

	subtitulo("Importancia de variables (reducción media de impureza Gini)")
	{
		var imp [NumFeatures]float64
		var tot float64
		for _, a := range bosque.Arboles {
			for f := range imp {
				imp[f] += a.Importancia[f]
				tot += a.Importancia[f]
			}
		}
		var fs [][]string
		for f := 0; f < NumFeatures; f++ {
			fr := imp[f] / math.Max(tot, 1e-12)
			fs = append(fs, []string{nombresFeatures[f], dec(fr*100, 2) + "%", barra(fr, 30)})
		}
		tabla([]int{20, 8, 30}, "lrl", []string{"Feature", "Import.", ""}, fs)
	}

	// Patrullaje preventivo: predicción RF para el mes siguiente al último del dataset
	sig := ds.UltimoMes + 1
	anioP, mesP := sig/12, sig%12+1
	subtitulo(fmt.Sprintf("Plan de patrullaje preventivo — predicción RF para %02d/%d (delito de calle más probable por franja)", mesP, anioP))
	{
		type cand struct {
			d     int
			calle int64
		}
		var cs []cand
		for d := range conteo {
			var s int64
			for f := 0; f < NumFranjas; f++ {
				for c := 0; c < NumClases; c++ {
					if esPatrullable[c] {
						s += conteo[d][f][c]
					}
				}
			}
			cs = append(cs, cand{d, s})
		}
		sort.Slice(cs, func(a, b int) bool { return cs[a].calle > cs[b].calle })
		var fs [][]string
		for r, cd := range cs[:min(10, len(cs))] {
			d := cd.d
			fila := []string{strconv.Itoa(r + 1), trunc(ds.NomDist[d], 22)}
			// franja crítica empírica (más delitos de calle)
			critica, maxC := 0, int64(-1)
			for f := 0; f < NumFranjas; f++ {
				var s int64
				for c := 0; c < NumClases; c++ {
					if esPatrullable[c] {
						s += conteo[d][f][c]
					}
				}
				if s > maxC {
					critica, maxC = f, s
				}
			}
			for f := 0; f < NumFranjas; f++ {
				x := [NumFeatures]uint16{uint16(d), ds.ZonaDist[d], uint16(f), uint16(mesP), uint16(anioP - anioBase)}
				p := bosque.probas(&x)
				best := -1
				for c := 0; c < NumClases; c++ {
					if esPatrullable[c] && (best < 0 || p[c] > p[best]) {
						best = c
					}
				}
				cel := clasesCortas[best] + " " + dec(float64(p[best])*100, 0) + "%"
				if f == critica {
					cel += " ★"
				}
				fila = append(fila, cel)
			}
			fila = append(fila, miles(cd.calle))
			fs = append(fs, fila)
		}
		tabla([]int{2, 22, 14, 14, 14, 14, 11}, "rlllllr",
			[]string{"#", "Distrito", franjasCortas[0], franjasCortas[1], franjasCortas[2], franjasCortas[3], "Hist. calle"}, fs)
		fmt.Println("    Celda = delito de calle con mayor P_RF(delito | distrito, zona, franja, mes, año).  ★ = franja con más delitos de calle (histórico).")
	}

	// ───────────────────────── FASE 4: benchmark ───────────────────────────────
	titulo(fmt.Sprintf("FASE 4 · Razonamiento cuantitativo — Benchmark 1 worker (secuencial) vs %d workers (mejor de %d corridas)", N, cfg.RepsBench))
	fmt.Println("  Ejecutando… (logs silenciados durante las mediciones)")
	var bench []FilaBench

	// (a) Pipeline de ingesta + limpieza + desagregación (ambos modos de parseo)
	for _, modo := range []string{"lector", "workers"} {
		c := cfg
		c.Parseo = modo
		var d1, dN int64
		t1 := mejorTiempo(cfg.RepsBench, func() time.Duration {
			r, _ := ejecutarPipeline(ruta, c, 1, false)
			d1 = r.Met.Desagregados
			return r.Duracion
		})
		tN := mejorTiempo(cfg.RepsBench, func() time.Duration {
			r, _ := ejecutarPipeline(ruta, c, N, false)
			dN = r.Met.Desagregados
			return r.Duracion
		})
		bench = append(bench, FilaBench{"Ingesta · parseo CSV en " + modo, "denuncias", met.Desagregados, t1, tN,
			d1 == dN && dN == met.Desagregados})
	}

	// (b) Map-Reduce de probabilidades condicionales.  Cada pasada dura ~1-2 ms,
	// así que se reporta el PROMEDIO de 50 pasadas para que el ruido no domine.
	var c1, cN Conteo
	promedio := func(w int, out *Conteo) time.Duration {
		runtime.GC()
		t := time.Now()
		for k := 0; k < 50; k++ {
			*out = conteoCondicional(ds, w)
		}
		return time.Since(t) / 50
	}
	t1 := mejorTiempo(cfg.RepsBench, func() time.Duration { return promedio(1, &c1) })
	tN := mejorTiempo(cfg.RepsBench, func() time.Duration { return promedio(N, &cN) })
	iguales := len(c1) == len(cN)
	for d := range c1 {
		if c1[d] != cN[d] {
			iguales = false
		}
	}
	bench = append(bench, FilaBench{"Conteo P(delito | distrito, franja)", "registros", int64(ds.N), t1, tN, iguales})

	// (c) Entrenamiento del Random Forest (Bagging).  Se miden ambos en las mismas
	// condiciones (GC previo); el bosque de 1 worker debe ser idéntico al de N.
	repsRF := cfg.RepsBench
	var bosque1, bosqueN *Bosque
	t1 = mejorTiempo(repsRF, func() time.Duration {
		t := time.Now()
		bosque1 = entrenarBosque(mat, train, cfg.RF, 1)
		return time.Since(t)
	})
	tN = mejorTiempo(repsRF, func() time.Duration {
		t := time.Now()
		bosqueN = entrenarBosque(mat, train, cfg.RF, N)
		return time.Since(t)
	})
	n1, s1 := firmaBosque(bosque1)
	nN, sN := firmaBosque(bosqueN)
	n0, s0 := firmaBosque(bosque)
	bench = append(bench, FilaBench{fmt.Sprintf("Entrenamiento RF (%d árboles)", cfg.RF.NumArboles), "muestras",
		int64(tamMuestra) * int64(cfg.RF.NumArboles), t1, tN, n1 == nN && s1 == sN && n0 == nN && s0 == sN})

	{
		var fs [][]string
		for _, b := range bench {
			sp := b.T1.Seconds() / b.TN.Seconds()
			ef := sp / float64(N)
			ok := "✔"
			if !b.Iguales {
				ok = "✖"
			}
			fs = append(fs, []string{b.Etapa, miles(b.Registros), fmtDur(b.T1), fmtDur(b.TN), dec(sp, 2) + "x", dec(ef*100, 1) + "%",
				miles(int64(float64(b.Registros) / b.T1.Seconds())), miles(int64(float64(b.Registros) / b.TN.Seconds())), ok})
		}
		tabla([]int{38, 11, 10, 10, 7, 6, 14, 14, 1}, "lrrrrrrrl",
			[]string{"Etapa", "Registros", "T(1 W)", fmt.Sprintf("T(%d W)", N), "Speedup", "Efic.", "Reg/s 1 W", fmt.Sprintf("Reg/s %d W", N), "="}, fs)
		fmt.Println("    Ingesta = lectura + parseo + limpieza + desagregación + fusión.  Speedup S = T(1)/T(N) │ Eficiencia E = S/N")
		fmt.Println("    \"=\" ✔: el resultado con N workers es idéntico al secuencial (mismas denuncias, mismos conteos, mismo bosque).")
		if N > 1 {
			fmt.Println()
			for _, b := range bench {
				sp := b.T1.Seconds() / b.TN.Seconds()
				e := (1/sp - 1/float64(N)) / (1 - 1/float64(N))
				lim := "∞ (e ≈ 0; S ≥ N por variación de medición/caché)"
				if e > 0.001 {
					lim = dec(1/e, 1) + "x"
				}
				fmt.Printf("    Karp-Flatt · %s → fracción serial e = %s │ Speedup máx. (Amdahl, N→∞) ≈ %s\n",
					padR(b.Etapa, 36), padL(dec(math.Max(e, 0)*100, 1)+"%", 6), lim)
			}
		} else {
			fmt.Println("    (Con 1 sola CPU no hay paralelismo real: ejecute en una máquina con más núcleos o use -workers.)")
		}
	}

	titulo("RESUMEN")
	fmt.Printf("  Filas CSV leídas: %s │ Denuncias nacionales: %s │ Registros desagregados procesados: %s\n",
		miles(met.FilasLeidas), miles(met.DenunciasNacionales), miles(met.Desagregados))
	fmt.Printf("  Ruido eliminado: %s filas (%s) │ RF: exactitud %s%% vs línea base %s%% │ Tiempo total del programa: %s\n",
		miles(met.FilasInvalidas), pct(met.FilasInvalidas, met.FilasLeidas), dec(acc*100, 2), dec(accBase*100, 2), fmtDur(time.Since(tIni)))
	fmt.Println()
}

func argmax64(v []int64) int {
	b := 0
	for i := range v {
		if v[i] > v[b] {
			b = i
		}
	}
	return b
}
