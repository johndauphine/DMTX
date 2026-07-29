package migrate

import (
	"context"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type migrationRunner func(context.Context, config.Config, TableObserver) (Result, error)

// adapterRows is the narrow row-stream contract shared by database/sql source
// adapters and test-only streams.
type adapterRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

// sourceAdapter owns source discovery, ordered reads, and source-side counts.
// It never prepares or mutates a target.
type sourceAdapter interface {
	Engine() string
	DisplayName() string
	ListTables(context.Context) ([]string, error)
	InspectTable(context.Context, string) (schema.Table, error)
	OpenRows(context.Context, schema.Table, []string) (adapterRows, error)
	CountRows(context.Context, schema.Table) (int, error)
	Close() error
}

// targetAdapter owns target schema planning, preparation, durable writes, and
// target-side counts. PlanTable is deliberately context-free: it must remain a
// pure transformation that performs no I/O or target mutation.
type targetAdapter interface {
	Engine() string
	PlanTable(string, schema.Table, string) (schema.Table, error)
	PrepareTable(context.Context, schema.Table, string) error
	WriteBatch(
		context.Context,
		schema.Table,
		[]string,
		string,
		[][]any,
	) (WriteReceipt, error)
	CountRows(context.Context, schema.Table) (int, error)
	Close() error
}

type endpointValidator func(config.Endpoint) error

type sourceAdapterFactory func(
	context.Context,
	config.Endpoint,
) (sourceAdapter, error)

type targetAdapterFactory func(
	context.Context,
	config.Endpoint,
) (targetAdapter, error)

// sourceRole and targetRole are immutable production registrations. A nil
// factory is permitted only when every certified route for that role has an
// explicit compatibility override.
type sourceRole struct {
	engine   string
	validate endpointValidator
	open     sourceAdapterFactory
}

type targetRole struct {
	engine     string
	capability engine.Capability
	validate   endpointValidator
	open       targetAdapterFactory
}
