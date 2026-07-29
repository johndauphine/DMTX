package migrate

import "github.com/johndauphine/dmtx/internal/schema"

func cloneSchemaIdentity(identity *schema.Identity) *schema.Identity {
	if identity == nil {
		return nil
	}
	cloned := *identity
	if identity.Frontier != nil {
		frontier := *identity.Frontier
		cloned.Frontier = &frontier
	}
	return &cloned
}
