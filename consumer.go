package main

import (
	"fmt"
	"github.com/nieksand/kinesis/src/kinesis"
	"os"
)

type LogConsumer struct {
	shardId string
}

func (ec *LogConsumer) Init(shardId string) error {
	ec.shardId = shardId
	fmt.Printf("init: %s\n", shardId)
	return nil
}

func (ec *LogConsumer) ProcessRecords(
	records []*kinesis.KclRecord,
	checkpointer *kinesis.Checkpointer) error {

	for _, record := range records {
		fmt.Printf("log: %s\n", record.DataB64)
	}

	// Abort execution on checkpointing errors.  We could retry here instead if
	// we wanted.
	return checkpointer.CheckpointAll()
}

func (ec *LogConsumer) Shutdown(
	shutdownType kinesis.ShutdownType,
	checkpointer *kinesis.Checkpointer) error {

	fmt.Printf("shutdown: %s\n", shutdownType)
	if shutdownType == kinesis.GracefulShutdown {
		if err := checkpointer.CheckpointAll(); err != nil {
			return err
		}
	}
	return nil
}
