package cognito

import (
	"encoding/json"
	"mime"
	"net/url"
)

func validOptionalRefreshToken(contentType string, body []byte) bool {
	content, _, _ := mime.ParseMediaType(contentType)
	if content == "application/x-www-form-urlencoded" || content == "text/plain" {
		values, err := url.ParseQuery(string(body))
		if err != nil {
			return true
		}
		tokens, present := values["refresh_token"]
		return !present || (len(tokens) == 1 && validRefreshToken(tokens[0]))
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return true
	}
	raw, present := fields["refresh_token"]
	if !present {
		return true
	}
	var token string
	return json.Unmarshal(raw, &token) == nil && validRefreshToken(token)
}
