package main

import (
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"log"
)

type Config struct {
	Port   string `envconfig:"PORT"`
	Drains string `envconfig:"DRAINS"`
}

var config Config

func (c Config) validate() error {
	if c.Port == "" {
		return fmt.Errorf("$PORT is empty")
	}
	if c.Drains == "" {
		return fmt.Errorf("$DRAINS is empty")
	}
	return nil
}

func init() {
	err := envconfig.Process("consumer", &config)
	if err == nil {
		err = config.validate()
	}

	if err != nil {
		log.Fatal(err.Error())
	}
}
