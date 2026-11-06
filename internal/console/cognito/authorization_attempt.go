package cognito

import (
	"context"
	"crypto/cipher"
	"errors"
	"io"
	"sync"
	"time"
)

const (
	AttemptLifetime       = 5 * time.Minute
	MaxActiveAttempts     = 1000
	EncryptedVerifierSize = 12 + 43 + 16
)

type AuthorizationStart struct {
	OperationID    string
	State          string `json:"-"`
	Nonce          string `json:"-"`
	CodeChallenge  string `json:"-"`
	BrowserBinding string `json:"-"`
	ExpiresAt      time.Time
	proof          *authorizationStartProof
}

type authorizationStartProof struct {
	manager     *AttemptManager
	operationID string
	valueHashes [4][32]byte
	createdAt   time.Time
	expiresAt   time.Time
}

type ConsumedAuthorization struct {
	OperationID  string
	NonceHash    [32]byte `json:"-"`
	CodeVerifier string   `json:"-"`
	proof        *consumedAttemptProof
}

type consumedAttemptProof struct {
	manager      *AttemptManager
	operationID  string
	nonceHash    [32]byte
	codeVerifier string
	expiresAt    time.Time
}

type AttemptRecord struct {
	OperationID   string
	StateHash     [32]byte `json:"-"`
	BindingHash   [32]byte `json:"-"`
	NonceHash     [32]byte `json:"-"`
	EncryptedPKCE []byte   `json:"-"`
	OccurredAt    time.Time
	ExpiresAt     time.Time
}

type AttemptConsumption struct {
	EventID     string
	StateHash   [32]byte `json:"-"`
	BindingHash [32]byte `json:"-"`
	Valid       bool
	OccurredAt  time.Time
}

type AttemptFailure struct {
	EventID     string
	OperationID string
	OccurredAt  time.Time
}

type AttemptStore interface {
	CreateCognitoAttempt(context.Context, AttemptRecord) error
	ConsumeCognitoAttempt(context.Context, AttemptConsumption) (AttemptRecord, error)
	FailCognitoAttempt(context.Context, AttemptFailure) error
}

type AttemptManager struct {
	store           AttemptStore
	random          io.Reader
	aead            cipher.AEAD
	keyFingerprint  [32]byte
	providerBinding [32]byte
	providerIssuer  string
	now             func() time.Time
	mu              sync.Mutex
}

type attemptSecrets struct {
	operationID string
	state       string
	nonce       string
	verifier    string
	binding     string
	cryptoNonce [12]byte
}

var (
	ErrInvalidAttemptConfig = errors.New("invalid Cognito authorization attempt configuration")
	ErrInvalidAttempt       = errors.New("Cognito authorization attempt is unavailable or expired")
	ErrAttemptLimit         = errors.New("Cognito authorization attempt limit reached")
	ErrAttemptUnknown       = errors.New("Cognito authorization attempt outcome is unknown")
	ErrAttemptEncryption    = errors.New("Cognito authorization attempt encryption failed")
	ErrAttemptAudit         = errors.New("Cognito authorization attempt audit failed")
)
