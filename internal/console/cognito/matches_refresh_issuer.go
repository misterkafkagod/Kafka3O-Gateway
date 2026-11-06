package cognito

func (p *RefreshTokenProtector) MatchesIssuer(issuer string) bool {
	return p != nil && p.providerIssuer != "" && p.providerIssuer == issuer
}
