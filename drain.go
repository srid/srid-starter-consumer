package main

import (
	"encoding/json"
	"fmt"
	"github.com/bmizerany/lpx"
	"github.com/srid/drain"
	"net/http"
	"strings"
)

type Record struct {
	partitionKey string
	data         []byte
}

type LogplexLogLine struct {
	Header *lpx.Header
	Data   []byte
}

func (r *Record) decodeDrainRecord() (*drain.Record, error) {
	var record drain.Record
	err := json.Unmarshal(r.data, &record)
	return &record, err
}

func HandleRecords(records []Record) {
	for _, record := range records {
		theOnlyDrain.ch <- record
	}
}

var theOnlyDrain *Drain

func init() {
	theOnlyDrain = NewDrain(config.DrainUrl)
	go theOnlyDrain.Start()
}

type Drain struct {
	url    string
	client *http.Client
	ch     chan Record
}

// TODO: make this an env
const DRAIN_BUFFER = 100

func NewDrain(url string) *Drain {
	d := new(Drain)
	d.url = url
	d.ch = make(chan Record, DRAIN_BUFFER)
	d.client = &http.Client{}
	return d
}

func (d *Drain) Start() {
	// TODO: open pesistent http connection
	for kinesisRecord := range d.ch {
		drainRecord, err := kinesisRecord.decodeDrainRecord()
		if err != nil {
			logError(fmt.Sprintf("Invalid JSON {{{%v}}}: %v\n", string(kinesisRecord.data), err))
			continue
		}

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
