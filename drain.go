package main

import (
	"encoding/json"
	"fmt"
	"github.com/srid/drain"
	"log"
	"net/http"
	"strings"
)

// kinesis.go

type KinesisRecord struct {
	partitionKey string
	data         []byte
}

func (r *KinesisRecord) decodeDrainRecord() (*drain.Record, error) {
	var record drain.Record
	err := json.Unmarshal(r.data, &record)
	return &record, err
}

func HandleRecords(kinesisRecords []KinesisRecord) {
	for _, kinesisRecord := range kinesisRecords {
		record, err := kinesisRecord.decodeDrainRecord()
		if err != nil {
			log.Printf("ERROR: invalid json from kinesis: %v\n", err)
			continue
		}

		drainManager.SendRecord(record)
		// theOnlyDrain.ch <- record
	}
}

// drain.go

type Drain struct {
	url      string
	appToken string
	client   *http.Client
	ch       chan *drain.Record
}

// TODO: make this an env
const DRAIN_BUFFER = 100

func NewDrain(appToken, url string) *Drain {
	d := new(Drain)
	d.appToken = appToken
	d.url = url
	d.ch = make(chan *drain.Record, DRAIN_BUFFER)
	d.client = &http.Client{}
	return d
}

func (d *Drain) Send(record *drain.Record) {
	d.ch <- record
}

func (d *Drain) Start() {
	// TODO: open pesistent http connection
	for drainRecord := range d.ch {
		// TODO: buffer records
		// ...
		drainRecords := []*drain.Record{drainRecord}

		req, err := NewLogplexRequest(d.url, drainRecords)
		if err != nil {
			logError(err.Error())
			continue
		}
		response, err := d.client.Do(req)
		if err != nil {
			logError(err.Error())
			continue
		}
		// TODO: handle other success codes (201, ..)
		if response.StatusCode != 200 {
			logError(fmt.Sprintf("Drain returned non-200 status: %v\n", response.Status))
		}
	}
}

func NewLogplexRequest(url string, logs []*drain.Record) (*http.Request, error) {
	body := drain.MakeLogplexFrame(logs)
	req, err := http.NewRequest(
		"POST",
		url,
		strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/logplex-1")
	req.Header.Set("User-Agent", "SridStarterProject/v2.dev")
	req.Header.Set("Content-Length", string(len(body)))
	return req, nil
}
