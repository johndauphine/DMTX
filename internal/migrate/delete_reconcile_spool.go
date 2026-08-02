package migrate

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

// The delete key spool: a private on-disk staging area for candidate keys,
// its lifecycle, and the path confinement that keeps it inside the run.

type deleteKeySpoolSnapshot struct {
	transaction *sql.Tx
}

func (spool *deleteKeySpool) beginReadSnapshot(
	ctx context.Context,
) (*deleteKeySpoolSnapshot, error) {
	if spool == nil || spool.db == nil {
		return nil, fmt.Errorf(
			"delete reconciliation spool is not open",
		)
	}
	transaction, err := spool.db.BeginTx(
		ctx,
		&sql.TxOptions{ReadOnly: true},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"begin delete reconciliation spool read snapshot: %w",
			err,
		)
	}
	return &deleteKeySpoolSnapshot{transaction: transaction}, nil
}

func (snapshot *deleteKeySpoolSnapshot) Close() error {
	if snapshot == nil || snapshot.transaction == nil {
		return nil
	}
	err := snapshot.transaction.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func newDeletePlanID() (string, error) {
	random := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, random); err != nil {
		return "", fmt.Errorf(
			"generate delete reconciliation plan ID: %w",
			err,
		)
	}
	return hex.EncodeToString(random), nil
}

func validateDeleteSpoolDirectory(directory string) (string, error) {
	if strings.TrimSpace(directory) == "" {
		return "", fmt.Errorf(
			"delete reconciliation spool directory is required",
		)
	}
	absolute, err := filepath.Abs(filepath.Clean(directory))
	if err != nil {
		return "", fmt.Errorf(
			"resolve delete reconciliation spool directory: %w",
			err,
		)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf(
			"inspect delete reconciliation spool directory: %w",
			err,
		)
	}
	if !info.IsDir() {
		return "", fmt.Errorf(
			"delete reconciliation spool path is not a directory",
		)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf(
			"resolve delete reconciliation spool directory symlinks: %w",
			err,
		)
	}
	return filepath.Clean(resolved), nil
}

func newDeleteKeySpool(
	directory string,
	planID string,
) (*deleteKeySpool, error) {
	directory, err := validateDeleteSpoolDirectory(directory)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(
		directory,
		"dmtx-delete-"+planID+".db",
	)
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_EXCL|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create delete reconciliation spool: %w",
			err,
		)
	}
	if err := file.Close(); err != nil {
		cleanupErr := removeDeleteSpoolPath(directory, path)
		return nil, errors.Join(
			fmt.Errorf(
				"close new delete reconciliation spool: %w",
				err,
			),
			cleanupErr,
		)
	}
	spool, err := openDeleteKeySpool(directory, path)
	if err != nil {
		return nil, errors.Join(
			err,
			removeDeleteSpoolPath(directory, path),
		)
	}
	statements := []string{
		`PRAGMA journal_mode=DELETE`,
		`PRAGMA synchronous=FULL`,
		`PRAGMA temp_store=FILE`,
		`CREATE TABLE source_keys (
			canonical BLOB PRIMARY KEY
		) WITHOUT ROWID`,
		`CREATE TABLE target_keys (
			canonical BLOB PRIMARY KEY,
			parameters BLOB NOT NULL
		) WITHOUT ROWID`,
		`CREATE TABLE plan_meta (
			name TEXT PRIMARY KEY,
			value TEXT NOT NULL
		) WITHOUT ROWID`,
	}
	for _, statement := range statements {
		if _, err := spool.db.Exec(statement); err != nil {
			return nil, errors.Join(
				fmt.Errorf(
					"initialize delete reconciliation spool: %w",
					err,
				),
				cleanupDeleteKeySpool(directory, spool),
			)
		}
	}
	return spool, nil
}

func openDeleteKeySpool(
	directory string,
	path string,
) (*deleteKeySpool, error) {
	resolved, err := validateDeleteSpoolPath(directory, path)
	if err != nil {
		return nil, err
	}
	database, err := sql.Open("sqlite", resolved)
	if err != nil {
		return nil, fmt.Errorf(
			"open delete reconciliation spool: %w",
			err,
		)
	}
	database.SetMaxOpenConns(1)
	if err := database.Ping(); err != nil {
		database.Close()
		return nil, fmt.Errorf(
			"ping delete reconciliation spool: %w",
			err,
		)
	}
	return &deleteKeySpool{path: resolved, db: database}, nil
}

func validateDeleteSpoolPath(
	directory string,
	path string,
) (string, error) {
	directory, err := validateDeleteSpoolDirectory(directory)
	if err != nil {
		return "", err
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf(
			"resolve delete reconciliation spool path: %w",
			err,
		)
	}
	linkInfo, err := os.Lstat(absolute)
	if err != nil {
		return "", fmt.Errorf(
			"inspect delete reconciliation spool path: %w",
			err,
		)
	}
	if linkInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf(
			"delete reconciliation spool must not be a symbolic link",
		)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf(
			"resolve delete reconciliation spool symlinks: %w",
			err,
		)
	}
	resolved = filepath.Clean(resolved)
	relative, err := filepath.Rel(directory, resolved)
	if err != nil || relative == "." ||
		relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf(
			"delete reconciliation spool escapes its configured directory",
		)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf(
			"inspect delete reconciliation spool: %w",
			err,
		)
	}
	if !info.Mode().IsRegular() ||
		info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf(
			"delete reconciliation spool must be a private regular file",
		)
	}
	return resolved, nil
}

func removeDeleteSpoolPath(directory string, path string) error {
	resolved, err := validateDeleteSpoolPath(directory, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.Remove(resolved); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf(
			"remove private delete reconciliation spool: %w",
			err,
		)
	}
	return nil
}

func (spool *deleteKeySpool) Close() error {
	if spool == nil || spool.db == nil {
		return nil
	}
	return spool.db.Close()
}

func cleanupDeleteKeySpool(
	directory string,
	spool *deleteKeySpool,
) error {
	if spool == nil {
		return nil
	}
	closeErr := spool.Close()
	removeErr := removeDeleteSpoolPath(directory, spool.path)
	return errors.Join(closeErr, removeErr)
}

func (spool *deleteKeySpool) sync() error {
	file, err := os.OpenFile(spool.path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open delete spool for sync: %w", err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync delete reconciliation spool: %w", err)
	}
	return nil
}

func encodeDeleteParameters(values []driver.Value) ([]byte, error) {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, uint32(len(values)))
	for _, value := range values {
		var kind byte
		var payload []byte
		switch typed := value.(type) {
		case int64:
			kind = 1
			payload = make([]byte, 8)
			binary.BigEndian.PutUint64(payload, uint64(typed))
		case float64:
			kind = 2
			payload = make([]byte, 8)
			binary.BigEndian.PutUint64(
				payload,
				math.Float64bits(typed),
			)
		case bool:
			kind = 3
			if typed {
				payload = []byte{1}
			} else {
				payload = []byte{0}
			}
		case []byte:
			kind = 4
			payload = append([]byte(nil), typed...)
		case string:
			kind = 5
			payload = []byte(typed)
		case time.Time:
			kind = 6
			payload = []byte(
				typed.UTC().Format(time.RFC3339Nano),
			)
		default:
			return nil, fmt.Errorf(
				"unsupported delete parameter type %T",
				value,
			)
		}
		encoded = append(encoded, kind)
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(payload)))
		encoded = append(encoded, length...)
		encoded = append(encoded, payload...)
	}
	return encoded, nil
}

func decodeDeleteParameters(encoded []byte) ([]driver.Value, error) {
	if len(encoded) < 4 {
		return nil, fmt.Errorf("delete parameter payload is truncated")
	}
	count := int(binary.BigEndian.Uint32(encoded[:4]))
	encoded = encoded[4:]
	values := make([]driver.Value, count)
	for index := 0; index < count; index++ {
		if len(encoded) < 9 {
			return nil, fmt.Errorf(
				"delete parameter %d is truncated",
				index,
			)
		}
		kind := encoded[0]
		length := binary.BigEndian.Uint64(encoded[1:9])
		encoded = encoded[9:]
		if length > uint64(len(encoded)) {
			return nil, fmt.Errorf(
				"delete parameter %d length is invalid",
				index,
			)
		}
		payload := encoded[:int(length)]
		encoded = encoded[int(length):]
		switch kind {
		case 1:
			if len(payload) != 8 {
				return nil, fmt.Errorf("invalid int64 parameter")
			}
			values[index] = int64(binary.BigEndian.Uint64(payload))
		case 2:
			if len(payload) != 8 {
				return nil, fmt.Errorf("invalid float64 parameter")
			}
			values[index] = math.Float64frombits(
				binary.BigEndian.Uint64(payload),
			)
		case 3:
			if len(payload) != 1 || payload[0] > 1 {
				return nil, fmt.Errorf("invalid boolean parameter")
			}
			values[index] = payload[0] == 1
		case 4:
			values[index] = append([]byte(nil), payload...)
		case 5:
			values[index] = string(payload)
		case 6:
			value, err := time.Parse(
				time.RFC3339Nano,
				string(payload),
			)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid timestamp parameter: %w",
					err,
				)
			}
			values[index] = value
		default:
			return nil, fmt.Errorf(
				"unknown delete parameter kind %d",
				kind,
			)
		}
	}
	if len(encoded) != 0 {
		return nil, fmt.Errorf(
			"delete parameter payload contains trailing data",
		)
	}
	return values, nil
}

func (spool *deleteKeySpool) scanKeys(
	ctx context.Context,
	side deleteKeySide,
	table schema.Table,
	columns []string,
	proof deleteKeyEqualityProof,
	canonicalizer deleteKeyCanonicalizer,
	maxKeyBytes int64,
	open func(
		context.Context,
		schema.Table,
		[]string,
	) (deleteKeyRows, error),
) error {
	if maxKeyBytes <= 0 {
		return fmt.Errorf(
			"delete key byte ceiling must be positive",
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	rows, err := open(
		ctx,
		table,
		append([]string(nil), columns...),
	)
	if err != nil {
		return fmt.Errorf("open %s delete keys: %w", side, err)
	}
	if rows == nil {
		return fmt.Errorf("%s delete key reader returned nil", side)
	}
	transaction, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		rows.Close()
		return fmt.Errorf("begin %s delete key spool: %w", side, err)
	}
	defer transaction.Rollback()
	statement := `INSERT OR IGNORE INTO source_keys (canonical) VALUES (?)`
	if side == deleteKeyTargetSide {
		statement = `INSERT OR IGNORE INTO target_keys
			(canonical, parameters) VALUES (?, ?)`
	}
	insert, err := transaction.PrepareContext(ctx, statement)
	if err != nil {
		rows.Close()
		return fmt.Errorf("prepare %s delete key spool: %w", side, err)
	}
	var scanErr error
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			scanErr = err
			break
		}
		values, err := rows.Values()
		if err != nil {
			scanErr = fmt.Errorf("read %s delete key: %w", side, err)
			break
		}
		canonical, parameters, err := canonicalDeleteKey(
			canonicalizer,
			side,
			proof,
			values,
		)
		if err != nil {
			scanErr = fmt.Errorf("%s delete key: %w", side, err)
			break
		}
		var encodedParameters []byte
		if side == deleteKeyTargetSide {
			encodedParameters, err = encodeDeleteParameters(
				parameters,
			)
			if err != nil {
				scanErr = err
				break
			}
		}
		encodedBytes := int64(len(canonical) +
			len(encodedParameters) + 16)
		if encodedBytes > maxKeyBytes {
			scanErr = fmt.Errorf(
				"%s delete key requires %d encoded bytes, exceeding the %d-byte ceiling",
				side,
				encodedBytes,
				maxKeyBytes,
			)
			break
		}
		var result sql.Result
		if side == deleteKeySourceSide {
			result, err = insert.ExecContext(ctx, canonical)
		} else {
			result, err = insert.ExecContext(
				ctx,
				canonical,
				encodedParameters,
			)
		}
		if err != nil {
			scanErr = fmt.Errorf("spool %s delete key: %w", side, err)
			break
		}
		affected, err := result.RowsAffected()
		if err != nil {
			scanErr = fmt.Errorf(
				"verify %s delete key uniqueness: %w",
				side,
				err,
			)
			break
		}
		if affected != 1 {
			scanErr = fmt.Errorf(
				"%s delete keys contain a duplicate complete primary key",
				side,
			)
			break
		}
	}
	if scanErr == nil {
		if err := rows.Err(); err != nil {
			scanErr = fmt.Errorf(
				"iterate %s delete keys: %w",
				side,
				err,
			)
		}
	}
	closeErr := rows.Close()
	statementErr := insert.Close()
	if scanErr == nil {
		scanErr = errors.Join(closeErr, statementErr)
	}
	if scanErr != nil {
		return scanErr
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit %s delete key spool: %w", side, err)
	}
	return spool.sync()
}

func writeDeleteHashFrame(
	digest hash.Hash,
	label string,
	payload []byte,
) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(label)))
	digest.Write(length[:])
	digest.Write([]byte(label))
	binary.BigEndian.PutUint64(length[:], uint64(len(payload)))
	digest.Write(length[:])
	digest.Write(payload)
}
