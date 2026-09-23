package security

import (
	"github.com/misterkafkagod/kafka3o/internal/kafka"
	"github.com/misterkafkagod/kafka3o/internal/service/security"
)

// quotaWireKeys maps the wire's camelCase quota key names onto the raw
// Kafka protocol key names kafka.QuotaValue.Key carries (FUNC-SPEC §8.7 S2).
// A fixed translation table, not domain state (D2).
//
//nolint:gochecknoglobals // fixed translation table, not domain state (D2)
var quotaWireKeys = map[string]string{
	"producerByteRate":  "producer_byte_rate",
	"consumerByteRate":  "consumer_byte_rate",
	"requestPercentage": "request_percentage",
}

// quotaAPIKeys is quotaWireKeys' inverse.
//
//nolint:gochecknoglobals // fixed translation table, not domain state (D2)
var quotaAPIKeys = func() map[string]string {
	out := make(map[string]string, len(quotaWireKeys))
	for api, wire := range quotaWireKeys {
		out[wire] = api
	}
	return out
}()

// toWireQuotaKey converts a camelCase API key to its raw protocol name. An
// unrecognised key passes through unchanged — the broker itself is the
// authority on valid quota keys, so this never needs its own validation.
func toWireQuotaKey(apiKey string) string {
	if wire, ok := quotaWireKeys[apiKey]; ok {
		return wire
	}
	return apiKey
}

// toAPIQuotaKey is toWireQuotaKey's inverse.
func toAPIQuotaKey(wireKey string) string {
	if api, ok := quotaAPIKeys[wireKey]; ok {
		return api
	}
	return wireKey
}

// toScramUserDTO converts a security.UserItem into the wire shape
// (FUNC-SPEC §8.7 S1 list).
func toScramUserDTO(u security.UserItem) ScramUserDTO {
	mechanisms := make([]ScramCredentialDTO, len(u.Credentials))
	for i, c := range u.Credentials {
		mechanisms[i] = ScramCredentialDTO{Mechanism: string(c.Mechanism), Iterations: c.Iterations}
	}
	return ScramUserDTO{Name: u.Name, Mechanisms: mechanisms}
}

// toScramUserDTOs converts every security.UserItem into its wire shape.
func toScramUserDTOs(users []security.UserItem) []ScramUserDTO {
	out := make([]ScramUserDTO, len(users))
	for i, u := range users {
		out[i] = toScramUserDTO(u)
	}
	return out
}

// toDeleteUserPlanDTO converts a security.DeletePlan into its wire shape
// (FUNC-SPEC §8.6 S1 delete).
func toDeleteUserPlanDTO(p security.DeletePlan) *DeleteUserPlanDTO {
	mechanisms := make([]string, len(p.Mechanisms))
	for i, m := range p.Mechanisms {
		mechanisms[i] = string(m)
	}
	return &DeleteUserPlanDTO{User: p.User, Mechanisms: mechanisms}
}

// toQuotaEntity converts the wire's { user?, clientId?, ip? } shape into
// the port's generic component list (FUNC-SPEC §8.7 S2).
func toQuotaEntity(dto QuotaEntityDTO) kafka.QuotaEntity {
	var entity kafka.QuotaEntity
	if dto.User != nil {
		entity = append(entity, kafka.QuotaEntityComponent{Type: "user", Name: dto.User})
	}
	if dto.ClientID != nil {
		entity = append(entity, kafka.QuotaEntityComponent{Type: "client-id", Name: dto.ClientID})
	}
	if dto.IP != nil {
		entity = append(entity, kafka.QuotaEntityComponent{Type: "ip", Name: dto.IP})
	}
	return entity
}

// toQuotaEntityDTO is toQuotaEntity's inverse.
func toQuotaEntityDTO(entity kafka.QuotaEntity) QuotaEntityDTO {
	var dto QuotaEntityDTO
	for _, c := range entity {
		switch c.Type {
		case "user":
			dto.User = c.Name
		case "client-id":
			dto.ClientID = c.Name
		case "ip":
			dto.IP = c.Name
		}
	}
	return dto
}

// toQuotaValuesDTO converts the port's raw-keyed values into the wire's
// fixed camelCase fields (FUNC-SPEC §8.7 S2).
func toQuotaValuesDTO(values []kafka.QuotaValue) QuotaValuesDTO {
	var dto QuotaValuesDTO
	for _, v := range values {
		value := v.Value
		switch toAPIQuotaKey(v.Key) {
		case "producerByteRate":
			dto.ProducerByteRate = &value
		case "consumerByteRate":
			dto.ConsumerByteRate = &value
		case "requestPercentage":
			dto.RequestPercentage = &value
		}
	}
	return dto
}

// toQuotaItemDTO converts a security.QuotaItem into its wire shape
// (FUNC-SPEC §8.7 S2 list).
func toQuotaItemDTO(item security.QuotaItem) QuotaItemDTO {
	return QuotaItemDTO{Entity: toQuotaEntityDTO(item.Entity), Quotas: toQuotaValuesDTO(item.Values)}
}

// toQuotaItemDTOs converts every security.QuotaItem into its wire shape.
func toQuotaItemDTOs(items []security.QuotaItem) []QuotaItemDTO {
	out := make([]QuotaItemDTO, len(items))
	for i, item := range items {
		out[i] = toQuotaItemDTO(item)
	}
	return out
}

// toQuotaSet converts the wire's fixed-field Set object into the service's
// raw-keyed map (FUNC-SPEC §8.7 S2 alter).
func toQuotaSet(dto QuotaValuesDTO) map[string]float64 {
	set := map[string]float64{}
	if dto.ProducerByteRate != nil {
		set[toWireQuotaKey("producerByteRate")] = *dto.ProducerByteRate
	}
	if dto.ConsumerByteRate != nil {
		set[toWireQuotaKey("consumerByteRate")] = *dto.ConsumerByteRate
	}
	if dto.RequestPercentage != nil {
		set[toWireQuotaKey("requestPercentage")] = *dto.RequestPercentage
	}
	return set
}

// toQuotaRemove converts the wire's camelCase remove[] key names into the
// service's raw-keyed form (FUNC-SPEC §8.7 S2 alter).
func toQuotaRemove(remove []string) []string {
	out := make([]string, len(remove))
	for i, k := range remove {
		out[i] = toWireQuotaKey(k)
	}
	return out
}

// toAlterQuotaPlanDTO converts a security.AlterQuotaPlan into its wire shape
// (FUNC-SPEC §8.6 S2 alter).
func toAlterQuotaPlanDTO(p security.AlterQuotaPlan) *AlterQuotaPlanDTO {
	changes := make([]QuotaChangeDetailDTO, len(p.Changes))
	for i, c := range p.Changes {
		changes[i] = QuotaChangeDetailDTO{Key: toAPIQuotaKey(c.Key), From: c.From, To: c.To}
	}
	return &AlterQuotaPlanDTO{Changes: changes}
}
