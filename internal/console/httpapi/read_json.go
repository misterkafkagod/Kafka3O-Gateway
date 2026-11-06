package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

func readJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	values := r.Header.Values("Content-Type")
	if len(values) != 1 {
		return errInvalidRequest
	}
	mediaType, params, err := mime.ParseMediaType(values[0])
	if err != nil || mediaType != "application/json" || (params["charset"] != "" && !strings.EqualFold(params["charset"], "utf-8")) {
		return errInvalidRequest
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytes *http.MaxBytesError
		if errors.As(err, &maxBytes) {
			return errRequestTooLarge
		}
		return errInvalidRequest
	}
	if !utf8.Valid(data) {
		return errInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errInvalidRequest
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return errInvalidRequest
	}
	return nil
}
