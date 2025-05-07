package task1

import (
	"context"
	"database/sql"
)

type Tx interface {
	Commit() error
	Rollback() error
	NewContext() context.Context
}

type TxManager interface {
	Do(ctx context.Context, fn func(tx Tx) error) error
}

type txKey struct{}

type sqlTxWrapper struct {
	tx  *sql.Tx
	ctx context.Context
}

func (t *sqlTxWrapper) Commit() error {
	return t.tx.Commit()
}

func (t *sqlTxWrapper) Rollback() error {
	return t.tx.Rollback()
}

func (t *sqlTxWrapper) NewContext() context.Context {
	return context.WithValue(t.ctx, txKey{}, t.tx)
}

func txFromContext(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(*sql.Tx)
	return tx, ok
}

type SQLTxManager struct {
	db *sql.DB
}

func NewSQLTxManager(db *sql.DB) *SQLTxManager {
	return &SQLTxManager{db: db}
}

func (m *SQLTxManager) Do(ctx context.Context, fn func(tx Tx) error) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	wrapper := &sqlTxWrapper{tx: tx, ctx: ctx}
	err = fn(wrapper)
	if err != nil {
		_ = wrapper.Rollback()
		return err
	}
	return wrapper.Commit()
}
