package cognito

func attemptAdditionalData(stateHash, bindingHash, nonceHash [32]byte) [96]byte {
	var additional [96]byte
	copy(additional[:32], stateHash[:])
	copy(additional[32:64], bindingHash[:])
	copy(additional[64:], nonceHash[:])
	return additional
}
