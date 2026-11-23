// Package dataset carga, valida y desagrega el CSV de denuncias policiales
// (SIDPOL – MININTER) con las mismas reglas de la PC3, y genera las
// particiones (shards) que entrena cada nodo ML del clúster.
package dataset

import (
	"strings"
)

// Dimensiones del problema (idénticas a la PC3).
const (
	NumClasses  = 6
	NumFeatures = 5
	NumFranjas  = 4
	NumZones    = 5

	FDistrict = 0 // distrito (id por UBIGEO, categórica)
	FZone     = 1 // zona urbana (categórica)
	FFranja   = 2 // franja horaria imputada (categórica)
	FMonth    = 3 // mes 0..11 (ordinal)
	FYear     = 4 // año - BaseYear (ordinal)

	BaseYear = 2018
	TestYear = 2026 // partición temporal: train 2018–2025, test 2026 (como la PC3)

	MaxYearIndex = 15 // años admitidos en consultas: 2018..2033
)

var ClassNames = [NumClasses]string{
	"Violencia contra la mujer e IGF", "Hurto", "Robo", "Estafa", "Extorsión", "Secuestro",
}

// StreetCrime indica los delitos de calle que el patrullaje puede disuadir
// (mismo criterio que la PC3: la violencia en el hogar y la estafa no definen ruta).
var StreetCrime = [NumClasses]bool{false, true, true, false, true, true}

var FranjaNames = [NumFranjas]string{"Madrugada (00-06)", "Mañana (06-12)", "Tarde (12-18)", "Noche (18-24)"}

var ZoneNames = [NumZones]string{"Lima Centro", "Lima Norte", "Lima Este", "Lima Sur", "Callao"}

var FeatureNames = [NumFeatures]string{"Distrito (UBIGEO)", "Zona urbana", "Franja horaria (imputada)", "Mes", "Año"}

// Categorical marca las variables nominales (se dividen por subconjuntos).
var Categorical = [NumFeatures]bool{true, true, true, false, false}

// Modalidad normalizada (mayúsculas, sin tildes) -> clase. "OTROS" no es clasificable.
var modalidades = map[string]int{
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

// Zonas de Lima Metropolitana (tabla de la PC3). Callao se resuelve por departamento.
var zonasLima = map[string]int{
	"LIMA": 0, "BREÑA": 0, "LA VICTORIA": 0, "RIMAC": 0, "SAN LUIS": 0, "BARRANCO": 0, "JESUS MARIA": 0,
	"LINCE": 0, "MAGDALENA DEL MAR": 0, "PUEBLO LIBRE": 0, "MIRAFLORES": 0, "SAN BORJA": 0,
	"SAN ISIDRO": 0, "SAN MIGUEL": 0, "SANTIAGO DE SURCO": 0, "SURQUILLO": 0,
	"ANCON": 1, "CARABAYLLO": 1, "COMAS": 1, "INDEPENDENCIA": 1, "LOS OLIVOS": 1, "PUENTE PIEDRA": 1,
	"SAN MARTIN DE PORRES": 1, "SANTA ROSA": 1,
	"ATE": 2, "CHACLACAYO": 2, "CIENEGUILLA": 2, "EL AGUSTINO": 2, "LURIGANCHO - CHOSICA": 2,
	"SAN JUAN DE LURIGANCHO": 2, "SANTA ANITA": 2, "LA MOLINA": 2,
	"CHORRILLOS": 3, "LURIN": 3, "PACHACAMAC": 3, "PUCUSANA": 3, "PUNTA HERMOSA": 3, "PUNTA NEGRA": 3,
	"SAN BARTOLO": 3, "SAN JUAN DE MIRAFLORES": 3, "SANTA MARIA DEL MAR": 3, "VILLA EL SALVADOR": 3,
	"VILLA MARIA DEL TRIUNFO": 3,
}

// Distribución a priori de la franja por modalidad (supuesto declarado en la PC3, Tabla 11).
var priorFranja = [NumClasses][NumFranjas]float64{
	{0.20, 0.17, 0.23, 0.40}, // Violencia mujer/IGF
	{0.08, 0.32, 0.38, 0.22}, // Hurto
	{0.24, 0.17, 0.25, 0.34}, // Robo
	{0.04, 0.46, 0.40, 0.10}, // Estafa
	{0.07, 0.43, 0.36, 0.14}, // Extorsión
	{0.30, 0.14, 0.18, 0.38}, // Secuestro
}

var acumFranja [NumClasses][NumFranjas]float64

func init() {
	for c := 0; c < NumClasses; c++ {
		s := 0.0
		for f := 0; f < NumFranjas; f++ {
			s += priorFranja[c][f]
			acumFranja[c][f] = s
		}
		acumFranja[c][NumFranjas-1] = 1
	}
}

var sinTildes = strings.NewReplacer("Á", "A", "É", "E", "Í", "I", "Ó", "O", "Ú", "U", "Ü", "U", "\uFEFF", "")

// Normalize: mayúsculas, sin tildes (conserva la Ñ), sin BOM y con espacios simples.
// Es la misma regla de la PC3; su ausencia hacía que "EXTORSIÓN" se descartara.
func Normalize(s string) string {
	return strings.Join(strings.Fields(sinTildes.Replace(strings.ToUpper(s))), " ")
}

// ClassOf devuelve la clase (0..5), -1 para "OTROS" y -2 si la modalidad no existe.
func ClassOf(modalidadNormalizada string) int {
	if c, ok := modalidades[modalidadNormalizada]; ok {
		return c
	}
	return -2
}

// InScope: Lima Metropolitana y Callao (misma condición que la PC3).
func InScope(dept, prov string) bool {
	return dept == "LIMA METROPOLITANA" || strings.Contains(dept, "CALLAO") || (dept == "LIMA" && prov == "LIMA")
}

// ZoneOf asigna la zona urbana; -1 si el distrito no figura en la tabla.
func ZoneOf(dept, district string) int {
	if strings.Contains(dept, "CALLAO") {
		return 4
	}
	if z, ok := zonasLima[district]; ok {
		return z
	}
	return -1
}

// splitmix64 es el generador de la PC3 (determinista y sin estado compartido).
func splitmix64(x *uint64) uint64 {
	*x += 0x9E3779B97F4A7C15
	z := *x
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// franjaStream reproduce la imputación de la PC3: la semilla depende solo del
// número global de fila, por lo que el resultado no depende del nodo ni del orden.
type franjaStream struct {
	state uint64
	acum  *[NumFranjas]float64
}

func newFranjaStream(rowNum int64, class int) franjaStream {
	return franjaStream{state: uint64(rowNum)*0x9E3779B97F4A7C15 ^ 0x5EED2026, acum: &acumFranja[class]}
}

func (f *franjaStream) next() uint8 {
	u := float64(splitmix64(&f.state)>>11) * (1.0 / (1 << 53))
	fr := 0
	for fr < NumFranjas-1 && u >= f.acum[fr] {
		fr++
	}
	return uint8(fr)
}
