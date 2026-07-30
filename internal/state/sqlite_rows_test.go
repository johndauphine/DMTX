package state

import (
	"errors"
	"strings"
	"testing"
)

type sqliteRowsResultStub struct {
	iterationErr error
	closeErr     error
	closed       bool
}

func (rows *sqliteRowsResultStub) Err() error {
	return rows.iterationErr
}

func (rows *sqliteRowsResultStub) Close() error {
	rows.closed = true
	return rows.closeErr
}

func TestFinishSQLiteRowsRejectsDeferredIterationErrorBeforeSelection(t *testing.T) {
	iterationErr := errors.New("deferred row failure")
	closeErr := errors.New("close failure")
	rows := &sqliteRowsResultStub{
		iterationErr: iterationErr,
		closeErr:     closeErr,
	}
	err := finishSQLiteRows(
		rows,
		"iterate evidence",
		"close evidence query",
	)
	if !rows.closed {
		t.Fatal("rows were not closed after an iteration failure")
	}
	if !errors.Is(err, iterationErr) || !errors.Is(err, closeErr) ||
		!strings.Contains(err.Error(), "iterate evidence") ||
		!strings.Contains(err.Error(), "close evidence query") {
		t.Fatalf("finish rows error = %v", err)
	}
}

func TestFinishSQLiteRowsChecksCloseAfterSuccessfulIteration(t *testing.T) {
	closeErr := errors.New("close failure")
	rows := &sqliteRowsResultStub{closeErr: closeErr}
	err := finishSQLiteRows(
		rows,
		"iterate evidence",
		"close evidence query",
	)
	if !rows.closed || !errors.Is(err, closeErr) ||
		!strings.Contains(err.Error(), "close evidence query") {
		t.Fatalf("finish rows closed=%v error=%v", rows.closed, err)
	}
}
