package mesos

import (
    "errors"
)


var (
	ErrInvalidResponse = errors.New("Invalid response from Mesos")
	ErrDoesNotExist = errors.New("The resource does not exist")
	ErrInternalServerError = errors.New("Mesos returned an internal server error")
	ErrSlaveStateLoadError = errors.New("An error was encountered loading the state of one or more Mesos slaves")
	ErrClusterDiscoveryError = errors.New("An error was encountered while running Mesos cluster discovery")
)
