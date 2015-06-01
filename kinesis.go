package main

import (
	kinesis "github.com/sendgridlabs/go-kinesis"
	"log"
)

var K *kinesis.Kinesis

type Record struct {
	partitionKey string
	data         []byte
}

func init() {
	auth, err := kinesis.NewAuthFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	K = kinesis.New(auth, kinesis.Region{"us-west-2"})
}

func putRecords(records []Record) error {
	args := kinesis.NewArgs()
	args.Add("StreamName", config.StreamName)
	for _, record := range records {
		args.AddRecord(record.data, record.partitionKey)
	}
	_, err := K.PutRecords(args)
	return err
}
