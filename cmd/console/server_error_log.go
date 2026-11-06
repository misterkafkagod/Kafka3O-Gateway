package main

import "log/slog"

type serverErrorLog struct {
	logger *slog.Logger
}

func (w serverErrorLog) Write(message []byte) (int, error) {
	w.logger.Error("Console HTTP transport error")
	return len(message), nil
}
