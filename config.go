package main

import (
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"log"
)

type Config struct {
	Port     string `envconfig:"PORT"`
	DrainUrl string `envconfig:"DRAIN_URL"`
}

var config Config

func (c Config) validate() error {
	if c.Port == "" {
		return fmt.Errorf("$PORT is empty")
	}
	if c.DrainUrl == "" {
		return fmt.Errorf("$DRAIN_URL is empty")
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
