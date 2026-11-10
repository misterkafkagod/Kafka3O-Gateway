package connect

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// SendConnectAction sends one lifecycle action to Kafka Connect.
type SendConnectAction interface {
	Execute(ctx context.Context, action Action) error
}

type sendConnectAction struct {
	baseURL string
	client  *http.Client
}

func NewSendConnectAction(baseURL string, client *http.Client) SendConnectAction {
	return sendConnectAction{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

// Execute follows the kafka-connect-api discovery recipe; Connect applies every action asynchronously.
func (s sendConnectAction) Execute(ctx context.Context, action Action) error {
	if s.baseURL == "" {
		return ErrConnectNotConfigured
	}
	if err := ValidateConnectorName(action.Connector); err != nil {
		return err
	}
	base := s.baseURL + "/connectors/" + url.PathEscape(action.Connector)
	var method, target string
	switch action.Kind {
	case ActionPause:
		method, target = http.MethodPut, base+"/pause"
	case ActionResume:
		method, target = http.MethodPut, base+"/resume"
	case ActionRestart:
		method, target = http.MethodPost, base+"/restart?includeTasks=true&onlyFailed=false"
	case ActionRestartTask:
		if action.Task < 0 {
			return ErrInvalidConnectAction
		}
		method, target = http.MethodPost, base+"/tasks/"+strconv.Itoa(int(action.Task))+"/restart"
	default:
		return ErrInvalidConnectAction
	}
	request, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrConnectUnreachable, err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		// Without a response the action may or may not have reached Connect.
		return fmt.Errorf("%w: %w", ErrConnectActionUnknown, err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		return nil
	case response.StatusCode == http.StatusNotFound:
		return ErrConnectorNotFound
	case response.StatusCode >= 400 && response.StatusCode < 500:
		return fmt.Errorf("%w: status %d", ErrConnectRejected, response.StatusCode)
	default:
		return fmt.Errorf("%w: status %d", ErrConnectUnreachable, response.StatusCode)
	}
}
