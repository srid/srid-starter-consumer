package main

import (
	"github.com/srid/drain"
	"strings"
)

type DrainManager struct {
	drains map[string][]*Drain
}

var drainManager *DrainManager

func NewDrainManager() *DrainManager {
	return &DrainManager{
		make(map[string][]*Drain),
	}
}
func NewDrainManagerFromHardcodedDrains(drainsSpec string) *DrainManager {
	mgr := NewDrainManager()

	for _, pairs := range strings.SplitN(drainsSpec, ";", 20) {
		parts := strings.SplitN(pairs, "=", 2)
		if len(parts) != 2 {
			panic("invalid drainsspec")
		}
		appToken := parts[0]
		drainUrl := parts[1]

		mgr.AddDrain(appToken, drainUrl)
	}

	return mgr
}

func (mgr *DrainManager) AddDrain(appToken string, drainUrl string) {
	drain := NewDrain(appToken, drainUrl)
	mgr.drains[appToken] = append(mgr.drains[appToken], drain)
}

func (mgr *DrainManager) Run() {
	for _, drainsForApp := range mgr.drains {
		for _, drain := range drainsForApp {
			go drain.Start()
		}
	}
}

func (mgr *DrainManager) SendRecord(record *drain.Record) {
	appToken := string(record.Header.Name)
	for _, drain := range mgr.drains[appToken] {
		drain.Send(record)
	}
}

func init() {
	drainManager = NewDrainManagerFromHardcodedDrains(config.Drains)
	go drainManager.Run()
}
