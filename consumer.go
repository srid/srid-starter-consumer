package main

import (
	"encoding/base64"
	"fmt"
	"github.com/nieksand/gokinesis/src/kinesis"
	"os"
)

type LogConsumer struct {
	shardId string
}

func (ec *LogConsumer) Init(shardId string) error {
	ec.shardId = shardId
	fmt.Fprintf(os.Stderr, "init: %s\n", shardId)
	return nil
}

func (ec *LogConsumer) ProcessRecords(
	records []*kinesis.KclRecord,
	checkpointer *kinesis.Checkpointer) error {

	// var lastSeq string

	for _, record := range records {
		data, err := base64.StdEncoding.DecodeString(record.DataB64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "base64 decode error: %v\n", err)
		}
		if false {
			fmt.Fprintf(os.Stderr, "log: %s\n", string(data))
		}
	}

	// Abort execution on checkpointing errors.  We could retry here instead if
	// we wanted.
	return checkpointer.CheckpointAll()
}

func (ec *LogConsumer) Shutdown(
	shutdownType kinesis.ShutdownType,
	checkpointer *kinesis.Checkpointer) error {

	fmt.Fprintf(os.Stderr, "shutdown: %s\n", shutdownType)
	if shutdownType == kinesis.GracefulShutdown {
		if err := checkpointer.CheckpointAll(); err != nil {
			return err
		}
	}
	return nil
}
