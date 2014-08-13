package vega

import "fmt"

const cDebug = false

func debugf(s string, vals ...interface{}) {
	if cDebug {
		fmt.Printf("DEBUG: "+s, vals...)
	}
}
