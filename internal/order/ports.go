package order

import "context"

// Repository is the persistence boundary used by the application service.
// The business layer does not need to know that PostgreSQL is underneath.
type Repository interface {
	Create(ctx context.Context, draft OrderDraft) (Response, error)
}
