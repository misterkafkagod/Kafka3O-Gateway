package gatewayauth

import "net/http"

// flushBufferedResponse sends a buffered response to the client unchanged.
func flushBufferedResponse(w http.ResponseWriter, buffered *bufferedResponse) {
	for key, values := range buffered.header {
		w.Header()[key] = values
	}
	status := buffered.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(buffered.body.Bytes())
}
