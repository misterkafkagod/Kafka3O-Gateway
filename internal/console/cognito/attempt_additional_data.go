package cognito

func attemptAdditionalData(stateHash, bindingHash, nonceHash, providerBinding [32]byte) [128]byte {
	var additional [128]byte
	copy(additional[:32], stateHash[:])
	copy(additional[32:64], bindingHash[:])
	copy(additional[64:96], nonceHash[:])
	copy(additional[96:], providerBinding[:])
	return additional
}
