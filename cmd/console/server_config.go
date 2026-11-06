package main

import "errors"

type serverConfig struct {
	databasePath string
	listen       string
	origin       string
	certificate  string
	key          string
}

var errInvalidServerConfig = errors.New("invalid Console server configuration")
