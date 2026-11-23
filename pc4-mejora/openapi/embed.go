// Package openapi incrusta la especificación en el binario: /swagger.yaml
// funciona sin depender de la carpeta desde la que se ejecute el coordinador.
package openapi

import _ "embed"

//go:embed swagger.yaml
var Spec []byte
