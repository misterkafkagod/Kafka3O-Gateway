package cognito

type BrowserCallback struct {
	State  string `json:"-"`
	Code   string `json:"-"`
	Denied bool   `json:"denied"`
}
