package api

import (
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/misterkafkagod/kafka3o/internal/api/middleware"
	"github.com/misterkafkagod/kafka3o/internal/command"
)

// buildRouteLookup walks every operation humaAPI has registered and returns
// a middleware.RouteLookup resolving a request to its command (TECH-SPEC
// O5). mux itself performs the method+pattern match (the same Go 1.22
// ServeMux humago registered every operation against via "METHOD /path"),
// so this never duplicates Huma's own routing — mux.Handler(r) is used only
// to resolve the matched pattern, never to actually serve the request.
func buildRouteLookup(mux *http.ServeMux, humaAPI huma.API) middleware.RouteLookup {
	byPattern := map[string]middleware.RouteCommand{}
	for _, item := range humaAPI.OpenAPI().Paths {
		for _, op := range pathOperations(item) {
			id, _ := op.Extensions[commandIDExtension].(string)
			desc, ok := command.Lookup(id)
			if !ok {
				continue
			}
			pattern := op.Method + " " + op.Path
			byPattern[pattern] = middleware.RouteCommand{
				CommandID: desc.ID, CommandName: desc.Name, IsWrite: desc.Access == command.W,
			}
		}
	}

	return func(r *http.Request) (middleware.RouteCommand, bool) {
		_, pattern := mux.Handler(r)
		rc, ok := byPattern[pattern]
		return rc, ok
	}
}

// pathOperations returns every method p defines, in no particular order.
func pathOperations(p *huma.PathItem) []*huma.Operation {
	all := []*huma.Operation{p.Get, p.Put, p.Post, p.Delete, p.Options, p.Head, p.Patch, p.Trace}
	out := make([]*huma.Operation, 0, len(all))
	for _, op := range all {
		if op != nil {
			out = append(out, op)
		}
	}
	return out
}
