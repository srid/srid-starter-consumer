package main

import (
	"encoding/json"
	"fmt"
	"github.com/bmizerany/lpx"
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

func (r *Record) decodeLogplex() (*LogplexLogLine, error) {
	var logline LogplexLogLine
	err := json.Unmarshal(r.data, &logline)
	return &logline, err
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
	for record := range d.ch {
		logline, err := record.decodeLogplex()
		if err != nil {
			logError(fmt.Sprintf("Invalid JSON {{{%v}}}: %v\n", string(record.data), err))
			continue
		}

		// TODO: buffer records
		// ...
		req, err := NewLogplexRequest(d.url, []*LogplexLogLine{logline})
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

func NewLogplexRequest(url string, logs []*LogplexLogLine) (*http.Request, error) {
	body := makeLogplexBody(logs)
	req, err := http.NewRequest(
		"POST",
		url,
		strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/logplex-1")
	req.Header.Set("User-Agent", "SridStarterProject/v1.dev")
	req.Header.Set("Content-Length", string(len(body)))
	return req, nil
}

// 70 <174>1 2012-07-22T00:06:26+00:00 host erlang console - Hi from erlang
func makeLogplexBody(logs []*LogplexLogLine) string {
	lines := make([]string, 0, len(logs))
	for _, logline := range logs {
		line := fmt.Sprintf("%v %v %v %v %v %v %v\n",
			logline.Header.PrivalVersion,
			logline.Header.Time,
			logline.Header.Hostname,
			logline.Header.Name,
			logline.Header.Procid,
			logline.Header.Msgid,
			string(logline.Data))
		line = fmt.Sprintf("%d %v", len(line), line)
		lines = append(lines, line)
	}

	return strings.Join(lines, "")
}
