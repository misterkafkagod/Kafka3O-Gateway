package session

import "errors"

const BrowserMarkerCookieName = "__Host-kafka3o_browser"

var ErrInvalidBrowserMarker = errors.New("browser marker is missing or invalid")
