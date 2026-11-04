package httpapi

import (
	"reflect"

	"github.com/danielgtaylor/huma/v2"
)

type optionalQueryParameter struct {
	Value string
	IsSet bool
}

func (p optionalQueryParameter) Schema(registry huma.Registry) *huma.Schema {
	return huma.SchemaFromType(registry, reflect.TypeOf(p.Value))
}

func (p *optionalQueryParameter) Receiver() reflect.Value {
	return reflect.ValueOf(p).Elem().Field(0)
}

func (p *optionalQueryParameter) OnParamSet(isSet bool, _ any) {
	p.IsSet = isSet
}
