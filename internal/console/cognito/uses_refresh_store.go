package cognito

import "reflect"

func (e *CodeExchanger) UsesRefreshStore(store RefreshStore) bool {
	if e == nil || e.attempts == nil || store == nil {
		return false
	}
	attached, ok := e.attempts.store.(RefreshStore)
	if !ok || attached == nil {
		return false
	}
	first, second := reflect.ValueOf(attached), reflect.ValueOf(store)
	if !first.Comparable() || !second.Comparable() ||
		(first.Kind() == reflect.Pointer && first.IsNil()) ||
		(second.Kind() == reflect.Pointer && second.IsNil()) {
		return false
	}
	return attached == store
}
