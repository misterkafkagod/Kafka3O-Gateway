package storage

func browserBindingValue(hash [32]byte) any {
	if hash == ([32]byte{}) {
		return nil
	}
	return hash[:]
}
