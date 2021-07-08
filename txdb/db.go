package txdb

import (
	_ "github.com/mattn/go-sqlite3"

	"database/sql"
)

type TxDb struct {
	db       *sql.DB
	tx       *sql.Tx
	stmt     *sql.Stmt
	cache    uint64
	numCalls uint64
}

var insertStatement = `
INSERT or IGNORE INTO diffs
    (txHash, tx, verified, pass)
    VALUES
    ($1, $2, $3, $4)
`
var createStmt = `
CREATE TABLE IF NOT EXISTS diffs (
    "txHash" STRING NOT NULL PRIMARY KEY,
    "tx" STRING,
    "verified" BOOL,
    "pass" BOOL
)
`
var selectStmt = `
SELECT * from diffs WHERE verified = $1
`

var selectTx = `
SELECT count(*) from diffs WHERE txHash = $1
`

func (txDb *TxDb) InsertTx(txHash, tx string) error {
	_, err := txDb.stmt.Exec(txHash, tx, false, false)
	if err != nil {
		return err
	}
	txDb.numCalls += 1

	// if we had enough calls, commit it
	if txDb.numCalls >= txDb.cache {
		if err := txDb.ForceCommit(); err != nil {
			return err
		}
	}
	return nil
}

func (txDb *TxDb) ForceCommit() error {
	if err := txDb.tx.Commit(); err != nil {
		return err
	}
	return txDb.resetTx()
}

func (txDb *TxDb) resetTx() error {
	txDb.numCalls = 0

	tx, err := txDb.db.Begin()
	if err != nil {
		return err
	}
	txDb.tx = tx

	stmt, err := txDb.tx.Prepare(insertStatement)
	if err != nil {
		return err
	}
	txDb.stmt = stmt

	return nil
}

func (diff *TxDb) Close() error {
	return diff.db.Close()
}

func NewTxDb(path string) (*TxDb, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createStmt)
	if err != nil {
		return nil, err
	}

	txDb := &TxDb{db: db, cache: 256}

	// initialize the transaction
	if err := txDb.resetTx(); err != nil {
		return nil, err
	}
	return txDb, nil
}
