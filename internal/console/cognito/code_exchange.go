package cognito

import (
	"context"
	"errors"
	"net/http"
	"time"

	"golang.org/x/oauth2"
)

type ClientAuthentication string

const (
	ClientAuthenticationPublic ClientAuthentication = "public"
	ClientAuthenticationBasic  ClientAuthentication = "basic"
)

type CodeExchangeStore interface {
	ReserveCognitoCodeExchange(context.Context, CodeExchangeReservation) error
	FinishCognitoCodeExchange(context.Context, CodeExchangeOutcome) error
}

type CodeExchangeReservation struct {
	OperationID string
	DenialID    string
	OccurredAt  time.Time
	ExpiresAt   time.Time
}

type CodeExchangeOutcome struct {
	OperationID string
	OccurredAt  time.Time
	ValidUntil  time.Time
	Status      CodeExchangeStatus
}

type CodeExchangeStatus string

const (
	CodeExchangeSucceeded CodeExchangeStatus = "succeeded"
	CodeExchangeFailed    CodeExchangeStatus = "failed"
	CodeExchangeUnknown   CodeExchangeStatus = "unknown"
)

type CodeExchangeResult struct {
	OperationID  string
	Identity     VerifiedIdentity `json:"-"`
	RefreshToken string           `json:"-"`
}

type CodeExchanger struct {
	attempts *AttemptManager
	store    CodeExchangeStore
	verifier *Verifier
	oauth    oauth2.Config
	client   *http.Client
}

var (
	ErrCodeExchangeConfig      = errors.New("invalid Cognito authorization code exchange configuration")
	ErrCodeExchangeUnavailable = errors.New("Cognito authorization code exchange is unavailable or already used")
	ErrCodeExchangeUnknown     = errors.New("Cognito authorization code exchange outcome is unknown")
	ErrCodeExchangeAudit       = errors.New("Cognito authorization code exchange audit failed")
	ErrCodeExchangeExpired     = errors.New("Cognito authorization code exchange expired before completion")
)
