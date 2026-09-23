// Package security wires the SCRAM credential and client-quota commands
// (FUNC-SPEC §8.7 S1, S2) onto Huma operations: routes, DTOs, and the
// mapping between them and internal/service/security's plain domain results
// (TECH-SPEC I5).
package security

// ScramCredentialDTO is one mechanism a user has a password configured for
// (FUNC-SPEC §8.7 S1 list). No secret material ever appears here
// (TECH-SPEC C7).
type ScramCredentialDTO struct {
	Mechanism  string `json:"mechanism"`
	Iterations int32  `json:"iterations"`
}

// ScramUserDTO is one user's configured SCRAM credentials (FUNC-SPEC §8.7 S1 list).
type ScramUserDTO struct {
	Name       string               `json:"name"`
	Mechanisms []ScramCredentialDTO `json:"mechanisms"`
}

// ListUsersInput takes no parameters (FUNC-SPEC §8.7 S1 list).
type ListUsersInput struct{}

// ListUsersBody is GET /v1/scram-users's response body (FUNC-SPEC §8.7 S1
// list: "{ users: [...] }").
type ListUsersBody struct {
	Users []ScramUserDTO `json:"users"`
}

// ListUsersOutput wraps ListUsersBody for Huma.
type ListUsersOutput struct {
	Body ListUsersBody
}

// CreateUserRequestBody is POST /v1/scram-users's request body (FUNC-SPEC
// §8.7 S1 create). Iterations omitted (or zero) uses the service's default
// (TECH-SPEC C11: 4096).
type CreateUserRequestBody struct {
	Name       string `json:"name"`
	Mechanism  string `json:"mechanism"`
	Password   string `json:"password"`
	Iterations int32  `json:"iterations,omitempty"`
}

// CreateUserInput is POST /v1/scram-users's parameters. S1 create carries
// no `confirm` and no `?dryRun=true` — it is not in FUNC-SPEC §5.6's
// destructive set (only S1 delete is), and its own IO row (§8.7) lists no
// dryRun input, unlike T5's.
type CreateUserInput struct {
	Body CreateUserRequestBody
}

// CreateUserBody is POST /v1/scram-users's 201 response body (FUNC-SPEC
// §8.7 S1 create). It carries no password (FUNC-SPEC §8.2: never echoed back).
type CreateUserBody struct {
	Name      string `json:"name"`
	Mechanism string `json:"mechanism"`
}

// CreateUserOutput wraps CreateUserBody for Huma.
type CreateUserOutput struct {
	Status int
	Body   CreateUserBody
}

// DeleteUserRequestBody is DELETE /v1/scram-users/{name}'s request body
// (FUNC-SPEC §8.7 S1 delete). Carried in the DELETE body itself (TECH-SPEC
// R3: some intermediaries strip it — a lost body fails closed as 400
// CONFIRMATION_MISMATCH, since Confirm then arrives empty). Mechanism
// omitted deletes every mechanism the user currently has.
type DeleteUserRequestBody struct {
	Confirm   string `json:"confirm"`
	Mechanism string `json:"mechanism,omitempty"`
}

// DeleteUserInput is DELETE /v1/scram-users/{name}'s parameters (FUNC-SPEC
// §8.7 S1 delete, §8.2 `?dryRun=true`).
type DeleteUserInput struct {
	Name   string `path:"name"`
	DryRun bool   `query:"dryRun"`
	Body   DeleteUserRequestBody
}

// DeleteUserPlanDTO is a dry-run S1 delete's plan (FUNC-SPEC §8.6: "{ user,
// mechanisms[] }").
type DeleteUserPlanDTO struct {
	User       string   `json:"user"`
	Mechanisms []string `json:"mechanisms"`
}

// DeleteUserBody is DELETE /v1/scram-users/{name}'s response body: either
// `{ deleted }` directly (FUNC-SPEC §8.7 S1 delete), or — when DryRun — the
// FUNC-SPEC §8.3 dry-run envelope. See topic.DeleteTopicBody (a sibling
// package) for why both shapes share one Go type.
type DeleteUserBody struct {
	DryRun  bool               `json:"dryRun,omitempty"`
	Plan    *DeleteUserPlanDTO `json:"plan,omitempty"`
	Deleted string             `json:"deleted,omitempty"`
}

// DeleteUserOutput wraps DeleteUserBody for Huma.
type DeleteUserOutput struct {
	Body DeleteUserBody
}

// QuotaEntityDTO is one entity a set of quotas applies to (FUNC-SPEC §8.7
// S2: "{ user?, clientId?, ip? }"). More than one field may be set to scope
// a quota to, e.g., one client id under one user.
type QuotaEntityDTO struct {
	User     *string `json:"user,omitempty"`
	ClientID *string `json:"clientId,omitempty"`
	IP       *string `json:"ip,omitempty"`
}

// QuotaValuesDTO is a set of quota values, keyed by the wire's camelCase
// names (FUNC-SPEC §8.7 S2: "{ producerByteRate?, consumerByteRate?,
// requestPercentage? }"). Used both for a described entity's current
// values and for S2 alter's `set` object.
type QuotaValuesDTO struct {
	ProducerByteRate  *float64 `json:"producerByteRate,omitempty"`
	ConsumerByteRate  *float64 `json:"consumerByteRate,omitempty"`
	RequestPercentage *float64 `json:"requestPercentage,omitempty"`
}

// QuotaItemDTO is one entity's configured quotas (FUNC-SPEC §8.7 S2 list,
// and S2 alter's `{ entity, quotas }` response).
type QuotaItemDTO struct {
	Entity QuotaEntityDTO `json:"entity"`
	Quotas QuotaValuesDTO `json:"quotas"`
}

// ListQuotasInput is GET /v1/quotas's parameters (FUNC-SPEC §8.7 S2 list).
type ListQuotasInput struct {
	EntityType string `query:"entityType"`
}

// ListQuotasBody is GET /v1/quotas's response body.
type ListQuotasBody struct {
	Items []QuotaItemDTO `json:"items"`
}

// ListQuotasOutput wraps ListQuotasBody for Huma.
type ListQuotasOutput struct {
	Body ListQuotasBody
}

// AlterQuotaRequestBody is PATCH /v1/quotas's request body (FUNC-SPEC §8.7
// S2 alter). Remove names quota keys (the same camelCase names as Set's
// fields) to unset.
type AlterQuotaRequestBody struct {
	Confirm string         `json:"confirm"`
	Entity  QuotaEntityDTO `json:"entity"`
	Set     QuotaValuesDTO `json:"set,omitempty"`
	Remove  []string       `json:"remove,omitempty"`
}

// AlterQuotaInput is PATCH /v1/quotas's parameters (FUNC-SPEC §8.7 S2 alter,
// §8.2 `?dryRun=true`).
type AlterQuotaInput struct {
	DryRun bool `query:"dryRun"`
	Body   AlterQuotaRequestBody
}

// QuotaChangeDetailDTO is one quota key's planned or applied change
// (FUNC-SPEC §8.6 S2). To is nil for a removal.
type QuotaChangeDetailDTO struct {
	Key  string   `json:"key"`
	From *float64 `json:"from,omitempty"`
	To   *float64 `json:"to,omitempty"`
}

// AlterQuotaPlanDTO is a dry-run S2 alter's plan (FUNC-SPEC §8.6: "{ changes[] }").
type AlterQuotaPlanDTO struct {
	Changes []QuotaChangeDetailDTO `json:"changes"`
}

// AlterQuotaBody is PATCH /v1/quotas's response body: either `{ entity,
// quotas }` directly (FUNC-SPEC §8.7 S2 alter), or — when DryRun — the
// FUNC-SPEC §8.3 dry-run envelope.
type AlterQuotaBody struct {
	DryRun bool               `json:"dryRun,omitempty"`
	Plan   *AlterQuotaPlanDTO `json:"plan,omitempty"`
	Entity *QuotaEntityDTO    `json:"entity,omitempty"`
	Quotas *QuotaValuesDTO    `json:"quotas,omitempty"`
}

// AlterQuotaOutput wraps AlterQuotaBody for Huma.
type AlterQuotaOutput struct {
	Body AlterQuotaBody
}
