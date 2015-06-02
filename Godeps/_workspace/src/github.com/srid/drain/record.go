package drain

import (
	"fmt"
	"github.com/bmizerany/lpx"
	"strings"
)

// Record represents a log line processed by drains
type Record struct {
	Header *lpx.Header
	Data   []byte
}

// String returns the original log string back
func (r Record) String() string {
	line := fmt.Sprintf("%s %s %s %s %s %s %s\n",
		string(r.Header.PrivalVersion),
		string(r.Header.Time),
		string(r.Header.Hostname),
		string(r.Header.Name),
		string(r.Header.Procid),
		string(r.Header.Msgid),
		string(r.Data))
	return line
}

// 70 <174>1 2012-07-22T00:06:26+00:00 host erlang console - Hi from erlang
// Make a logplex POST body frame from multiple records
func MakeLogplexFrame(records []*Record) string {
	lines := make([]string, 0, len(records))
	for _, record := range records {
		line := fmt.Sprintf("%s %s %s %s %s %s %s\n",
			record.Header.PrivalVersion,
			record.Header.Time,
			record.Header.Hostname,
			record.Header.Name,
			record.Header.Procid,
			record.Header.Msgid,
			string(record.Data))
		line = fmt.Sprintf("%d %s", len(line), line)
		lines = append(lines, line)
	}
	return strings.Join(lines, "")
}
