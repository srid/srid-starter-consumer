package main

import (
	"github.com/nieksand/gokinesis/src/kinesis"
)

func main() {
	var consumer LogConsumer
	kinesis.Run(&consumer)
}
