package gatewayauth

import (
	"net/http"
	"strings"
)

// ClassifyRoute maps a method and escaped path to its policy action. Only liveness is anonymous; any route
// or method not listed here is unknown and must be denied.
func ClassifyRoute(method, escapedPath string) (Route, bool) {
	segments := strings.Split(strings.TrimPrefix(escapedPath, "/api/v1/"), "/")
	if !strings.HasPrefix(escapedPath, "/api/v1/") {
		return Route{}, false
	}
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return Route{}, false
		}
	}
	key := method + " "
	switch {
	case len(segments) == 2 && segments[0] == "health" && segments[1] == "live":
		if method == http.MethodGet {
			return Route{Anonymous: true}, true
		}
		return Route{}, false
	case len(segments) == 1:
		key += segments[0]
	case len(segments) >= 3 && segments[0] == "consumer-groups":
		key += "consumer-groups/{}/" + strings.Join(segments[2:], "/")
	case len(segments) >= 2 && (segments[0] == "topics" || segments[0] == "brokers"):
		key += segments[0] + "/{}"
		if len(segments) > 2 {
			key += "/" + strings.Join(segments[2:], "/")
		}
		if len(segments) == 5 && segments[0] == "topics" && segments[2] == "partitions" && segments[4] == "replicas" {
			key = method + " topics/{}/partitions/{}/replicas"
		}
	default:
		return Route{}, false
	}
	action, ok := routeActions[key]
	return Route{Action: action}, ok
}
