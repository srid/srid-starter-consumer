package main

import (
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"log"
	"strings"
)

type Config struct {
	Port      string `envconfig:"PORT"`
	BasicAuth string `envconfig:"BASIC_AUTH"`
}

var config Config

func (c Config) validate() error {
	if c.Port == "" {
		return fmt.Errorf("$PORT is empty")
	}
	return nil
}

func (c Config) GetUserPass() (string, string, bool) {
	if c.BasicAuth == "" {
		return "", "", false
	}
	parts := strings.SplitN(c.BasicAuth, ":", 2)
	if len(parts) != 2 {
		panic("invalid BASIC_AUTH")
	}
	return parts[0], parts[1], true
}

func init() {
	err := envconfig.Process("drainsrv", &config)
	if err == nil {
		err = config.validate()
	}

	if err != nil {
		log.Fatal(err.Error())
	}
}
