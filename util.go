package main

import (
	"fmt"
	"os"
)

func logError(msg string) {
	fmt.Fprint(os.Stderr, msg)
}
