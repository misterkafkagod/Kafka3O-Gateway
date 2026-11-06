package httpapi

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/misterkafkagod/Kafka3O-Gateway/internal/console/identity"
)

func readLocalAccountListQuery(r *http.Request) (string, int, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return "", 0, errInvalidRequest
	}
	for key, entries := range values {
		if (key != "after" && key != "limit") || len(entries) != 1 {
			return "", 0, errInvalidRequest
		}
	}
	limit := identity.DefaultLocalAccountPageSize
	if entries, found := values["limit"]; found {
		limit, err = strconv.Atoi(entries[0])
		if err != nil || limit < 1 || limit > identity.MaxLocalAccountPageSize {
			return "", 0, errInvalidRequest
		}
	}
	after := values.Get("after")
	if entries, found := values["after"]; found {
		if entries[0] == "" || identity.ValidateLocalAccountCursor(after) != nil {
			return "", 0, errInvalidRequest
		}
	}
	return after, limit, nil
}
