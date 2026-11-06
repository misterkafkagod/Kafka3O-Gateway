package httpapi

import (
	"net/http"
	"net/netip"
)

func clientAddress(r *http.Request) (netip.Addr, error) {
	addr, err := netip.ParseAddrPort(r.RemoteAddr)
	if err != nil || !addr.Addr().IsValid() || addr.Addr().Zone() != "" {
		return netip.Addr{}, ErrInvalidAPIConfig
	}
	return addr.Addr().Unmap(), nil
}
