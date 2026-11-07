package cognito

func validRefreshToken(token string) bool {
	if len(token) == 0 || len(token) > MaxRefreshTokenBytes {
		return false
	}
	for i := 0; i < len(token); i++ {
		if token[i] < 0x20 || token[i] > 0x7e {
			return false
		}
	}
	return true
}
