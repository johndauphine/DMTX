package migrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// mysqlDeleteJournalTable is deliberately a target-private relation.  It is
// created only by the Stage 4 readiness boundary; ApplyDeleteBatch never
// creates it lazily because a missing journal after durable readiness is an
// unsafe target replacement, not an invitation to issue DDL again.
const (
	mysqlDeleteJournalTable       = "dmtx_internal_delete_batch_receipts"
	mysqlDeleteJournalVersion     = 1
	mysqlDeleteJournalLockSeconds = 15
	mysqlDeleteJournalCommentV1   = "dmtx.stage4.delete-journal/v1"
)

const mysqlDeleteJournalCreateSQLPrefix = "CREATE TABLE `dmtx_internal_delete_batch_receipts` (" +
	"`journal_version` SMALLINT UNSIGNED NOT NULL, " +
	"`entry_kind` CHAR(1) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, " +
	"`token` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, " +
	"`plan_id` CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, " +
	"`sequence` BIGINT UNSIGNED NOT NULL, " +
	"`batch_digest` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, " +
	"`candidates` BIGINT UNSIGNED NOT NULL, " +
	"`deleted_rows` BIGINT UNSIGNED NOT NULL, " +
	"`target_catalog_digest` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, " +
	"`target_identity_digest` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, " +
	"`journal_identity` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, " +
	"`receipt_digest` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, " +
	"PRIMARY KEY (`token`)" +
	") ENGINE=InnoDB DEFAULT CHARACTER SET ascii COLLATE ascii_bin COMMENT = "

const (
	mysqlDeleteJournalHeaderKind  = "H"
	mysqlDeleteJournalReceiptKind = "R"
	mysqlDeleteJournalHeaderToken = "0000000000000000000000000000000000000000000000000000000000000000"
	mysqlDeleteJournalZeroPlanID  = "00000000000000000000000000000000"
)

type mysqlDeleteJournalCatalog struct {
	Exists          bool
	EmptyPrefix     bool
	CreatorDigest   string
	JournalIdentity string
	CatalogDigest   string
}

// mysqlDeleteJournalShape is the immutable CREATE TABLE authority. Its
// dynamic comment is committed by the DDL itself, before the separate header
// INSERT can crash. That prevents a no-receipt run from taking over an
// unrelated lookalike table merely because its column shape is identical.
type mysqlDeleteJournalShape struct {
	Exists          bool
	CreatorDigest   string
	JournalIdentity string
}

type mysqlDeleteJournalHeader struct {
	JournalVersion       int
	EntryKind            string
	Token                string
	PlanID               string
	Sequence             uint64
	BatchDigest          string
	Candidates           uint64
	DeletedRows          uint64
	TargetCatalogDigest  string
	TargetIdentityDigest string
	JournalIdentity      string
	ReceiptDigest        string
}

type mysqlDeleteReceiptRow struct {
	JournalVersion       int
	EntryKind            string
	Receipt              deleteTargetBatchReceipt
	TargetCatalogDigest  string
	TargetIdentityDigest string
	JournalIdentity      string
}

func isMySQLDeleteJournalRelation(name string) bool {
	return strings.EqualFold(strings.TrimSpace(name), mysqlDeleteJournalTable)
}

// PreflightStage4DeleteJournalReadiness is the read-only half of the generic
// lifecycle protocol. It deliberately proves the live flavor, selected
// database identity, journal collision shape, and the privileges needed to
// create or use the private journal before any checkpoint is written.
func (adapter *mysqlTargetAdapter) PreflightStage4DeleteJournalReadiness(
	ctx context.Context,
) error {
	return preflightMySQLDeleteReceiptJournal(ctx, adapter)
}

func preflightMySQLDeleteReceiptJournal(
	ctx context.Context,
	adapter *mysqlTargetAdapter,
) error {
	if adapter == nil || adapter.database == nil ||
		!supportedMySQLDeleteFlavor(adapter.flavor) ||
		strings.TrimSpace(adapter.namespace) == "" ||
		strings.TrimSpace(adapter.workloadIdentity) == "" {
		return errors.New("MySQL delete receipt journal target is unavailable")
	}
	if ctx == nil {
		return errors.New("MySQL delete receipt journal preflight context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := engine.VerifyMySQLTargetForFlavor(ctx, adapter.database, adapter.flavor); err != nil {
		return fmt.Errorf("verify MySQL delete journal target flavor/session: %w", err)
	}
	identity, err := readMySQLDeleteEndpointIdentity(
		ctx,
		adapter.database,
		adapter.flavor,
	)
	if err != nil {
		return fmt.Errorf("read MySQL delete journal target identity: %w", err)
	}
	if identity.database != adapter.namespace {
		return fmt.Errorf("MySQL delete journal selected database %q differs from target database %q", identity.database, adapter.namespace)
	}
	if _, err := mysqlDeleteCanonicalTargetIdentity(identity); err != nil {
		return err
	}
	catalog, err := inspectMySQLDeleteReceiptJournal(
		ctx,
		adapter.database,
		adapter.flavor,
		adapter.namespace,
	)
	if err != nil {
		return err
	}
	privileges, err := mysqlDeleteJournalPrivileges(
		ctx,
		adapter.database,
		adapter.namespace,
	)
	if err != nil {
		return err
	}
	required := []string{"SELECT", "INSERT"}
	if !catalog.Exists {
		required = append(required, "CREATE")
	}
	for _, privilege := range required {
		if !privileges[privilege] {
			return fmt.Errorf("MySQL delete receipt journal requires %s privilege on target database %s", privilege, adapter.namespace)
		}
	}
	return nil
}

// PrepareStage4DeleteJournalReadiness serializes the auto-committing journal
// DDL with a pinned MySQL GET_LOCK.  Once durable state already holds a
// receipt, this method is verification-only: an absent or replaced journal
// fails closed rather than being recreated.
func (adapter *mysqlTargetAdapter) PrepareStage4DeleteJournalReadiness(
	ctx context.Context,
	request Stage4DeleteJournalReadinessRequest,
) (result state.Stage4DeleteJournalReadiness, resultErr error) {
	if err := validateMySQLDeleteReadinessRequest(request); err != nil {
		return result, err
	}
	if adapter == nil || adapter.database == nil ||
		!supportedMySQLDeleteFlavor(adapter.flavor) ||
		strings.TrimSpace(adapter.namespace) == "" ||
		strings.TrimSpace(adapter.workloadIdentity) == "" {
		return result, errors.New("MySQL delete journal readiness target is unavailable")
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return result, fmt.Errorf("acquire pinned MySQL delete journal connection: %w", err)
	}
	closed := false
	lockName := mysqlDeleteJournalLockName(adapter.namespace)
	locked := false
	defer func() {
		if locked {
			cleanupCtx, cancel := mysqlDeleteDetachedContext(ctx)
			releaseErr := releaseMySQLDeleteJournalLock(cleanupCtx, connection, lockName)
			cancel()
			if releaseErr != nil {
				discardMySQLConnection(connection)
				resultErr = errors.Join(resultErr, fmt.Errorf("release pinned MySQL delete journal lock: %w", releaseErr))
			}
		}
		if !closed {
			if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
				resultErr = errors.Join(resultErr, fmt.Errorf("close pinned MySQL delete journal connection: %w", closeErr))
			}
		}
	}()
	if err := engine.VerifyMySQLTargetForFlavor(ctx, connection, adapter.flavor); err != nil {
		return result, fmt.Errorf("verify pinned MySQL delete journal target flavor/session: %w", err)
	}
	identity, err := readMySQLDeleteEndpointIdentity(ctx, connection, adapter.flavor)
	if err != nil {
		return result, fmt.Errorf("read pinned MySQL delete journal target identity: %w", err)
	}
	if identity.database != adapter.namespace {
		return result, fmt.Errorf("pinned MySQL delete journal database %q differs from target database %q", identity.database, adapter.namespace)
	}
	if request.Existing != nil &&
		request.Existing.Readiness.TargetIdentity != adapter.workloadIdentity {
		return result, errors.New("durable MySQL delete journal readiness target identity differs from the configured target")
	}
	targetIdentity, err := mysqlDeleteCanonicalTargetIdentity(identity)
	if err != nil {
		return result, err
	}
	creatorDigest, err := mysqlDeleteJournalCreatorDigest(request, targetIdentity)
	if err != nil {
		return result, err
	}
	var acquired int
	if err := connection.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, mysqlDeleteJournalLockSeconds).Scan(&acquired); err != nil || acquired != 1 {
		if err == nil {
			err = fmt.Errorf("GET_LOCK returned %d", acquired)
		}
		return result, fmt.Errorf("acquire pinned MySQL delete journal lock: %w", err)
	}
	locked = true
	catalog, err := inspectMySQLDeleteReceiptJournal(
		ctx,
		connection,
		adapter.flavor,
		adapter.namespace,
	)
	if err != nil {
		return result, fmt.Errorf("inspect exact MySQL delete receipt journal: %w", err)
	}
	action, err := mysqlDeleteJournalPreparationAction(
		catalog,
		request.Existing != nil,
	)
	if err != nil {
		return result, err
	}
	switch action {
	case mysqlDeleteJournalPrepareCreate:
		privileges, privilegeErr := mysqlDeleteJournalPrivileges(ctx, connection, adapter.namespace)
		if privilegeErr != nil {
			return result, privilegeErr
		}
		for _, privilege := range []string{"CREATE", "SELECT", "INSERT"} {
			if !privileges[privilege] {
				return result, fmt.Errorf("MySQL delete receipt journal requires %s privilege on target database %s", privilege, adapter.namespace)
			}
		}
		if err := createMySQLDeleteReceiptJournal(ctx, connection, adapter.flavor, adapter.namespace, creatorDigest); err != nil {
			return result, err
		}
	case mysqlDeleteJournalPrepareAuthenticatePrefix:
		// Creator equality is intentionally limited to the headerless CREATE
		// prefix. Once the header authenticates a journal, later independent
		// runs reuse its catalog authority; their readiness receipt binds that
		// observed catalog to the new run and inventory instead.
		if err := validateMySQLDeleteJournalPrefixCreator(catalog, creatorDigest); err != nil {
			return result, err
		}
		privileges, privilegeErr := mysqlDeleteJournalPrivileges(ctx, connection, adapter.namespace)
		if privilegeErr != nil {
			return result, privilegeErr
		}
		for _, privilege := range []string{"SELECT", "INSERT"} {
			if !privileges[privilege] {
				return result, fmt.Errorf("MySQL delete receipt journal requires %s privilege on target database %s", privilege, adapter.namespace)
			}
		}
		if err := authenticateEmptyMySQLDeleteReceiptJournal(ctx, connection, adapter.flavor, adapter.namespace, creatorDigest); err != nil {
			return result, err
		}
	case mysqlDeleteJournalPrepareVerify:
	default:
		return result, errors.New("MySQL delete receipt journal preparation action is invalid")
	}
	catalog, err = inspectMySQLDeleteReceiptJournal(ctx, connection, adapter.flavor, adapter.namespace)
	if err != nil {
		return result, fmt.Errorf("reread prepared MySQL delete receipt journal: %w", err)
	}
	if !catalog.Exists || catalog.EmptyPrefix {
		return result, errors.New("prepared MySQL delete receipt journal lacks immutable authenticated authority")
	}
	return mysqlDeleteReadinessFromCatalog(
		request,
		adapter.workloadIdentity,
		identity,
		catalog,
	)
}

func validateMySQLDeleteReadinessRequest(request Stage4DeleteJournalReadinessRequest) error {
	if strings.TrimSpace(request.RunID) == "" || request.RunID != strings.TrimSpace(request.RunID) {
		return errors.New("MySQL delete journal readiness run ID is required")
	}
	if err := validateLowerSHA256("MySQL delete journal readiness inventory digest", request.InventoryDigest); err != nil {
		return err
	}
	if request.Existing != nil {
		if err := request.Existing.Validate(); err != nil {
			return fmt.Errorf("stored MySQL delete journal readiness receipt is invalid: %w", err)
		}
		if request.Existing.Readiness.RunID != request.RunID ||
			request.Existing.Readiness.InventoryDigest != request.InventoryDigest {
			return errors.New("stored MySQL delete journal readiness receipt differs from run inventory")
		}
	}
	return nil
}

func mysqlDeleteDetachedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
}

func mysqlDeleteJournalLockName(namespace string) string {
	digest := sha256.Sum256([]byte("dmtx.stage4.delete-journal.v1\x00" + namespace))
	return "dmtx:delete-journal:" + hex.EncodeToString(digest[:20])
}

func releaseMySQLDeleteJournalLock(ctx context.Context, connection *sql.Conn, lockName string) error {
	if connection == nil || strings.TrimSpace(lockName) == "" {
		return errors.New("MySQL delete journal lock connection is unavailable")
	}
	var released sql.NullInt64
	if err := connection.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", lockName).Scan(&released); err != nil {
		return err
	}
	if !released.Valid || released.Int64 != 1 {
		return fmt.Errorf("RELEASE_LOCK returned %v", released)
	}
	return nil
}

type mysqlDeleteEndpointIdentity struct {
	flavor         engine.MySQLServerFlavor
	serverIdentity string
	database       string
	version        string
}

func readMySQLDeleteEndpointIdentity(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	flavor engine.MySQLServerFlavor,
) (mysqlDeleteEndpointIdentity, error) {
	if queryer == nil || !supportedMySQLDeleteFlavor(flavor) {
		return mysqlDeleteEndpointIdentity{}, errors.New("MySQL delete endpoint identity is unavailable")
	}
	var server, database, version sql.NullString
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		if err := queryer.QueryRowContext(ctx, "SELECT @@server_uuid, DATABASE(), VERSION()").Scan(&server, &database, &version); err != nil {
			return mysqlDeleteEndpointIdentity{}, err
		}
	case engine.MySQLServerFlavorMariaDB1011:
		if err := queryer.QueryRowContext(ctx, "SELECT @@global.server_uid, DATABASE(), VERSION()").Scan(&server, &database, &version); err != nil {
			return mysqlDeleteEndpointIdentity{}, err
		}
	default:
		return mysqlDeleteEndpointIdentity{}, errors.New("unsupported MySQL delete endpoint flavor")
	}
	identity := mysqlDeleteEndpointIdentity{
		flavor: flavor, serverIdentity: strings.TrimSpace(server.String),
		database: database.String, version: strings.TrimSpace(version.String),
	}
	if !server.Valid || identity.serverIdentity == "" || !database.Valid || identity.database == "" || !version.Valid || identity.version == "" {
		return mysqlDeleteEndpointIdentity{}, errors.New("MySQL delete endpoint identity/version is incomplete")
	}
	if flavor == engine.MySQLServerFlavorOracle80 {
		identity.serverIdentity = strings.ToLower(identity.serverIdentity)
	}
	return identity, nil
}

func mysqlDeleteCanonicalTargetIdentity(identity mysqlDeleteEndpointIdentity) (string, error) {
	if !supportedMySQLDeleteFlavor(identity.flavor) || strings.TrimSpace(identity.serverIdentity) == "" || identity.database == "" {
		return "", errors.New("MySQL delete target identity is incomplete")
	}
	flavor := ""
	switch identity.flavor {
	case engine.MySQLServerFlavorOracle80:
		flavor = "mysql80"
		identity.serverIdentity = strings.ToLower(identity.serverIdentity)
	case engine.MySQLServerFlavorMariaDB1011:
		flavor = "mariadb1011"
	}
	return "mysql/" + flavor + "/server-hex=" + hex.EncodeToString([]byte(identity.serverIdentity)) + "/database-hex=" + hex.EncodeToString([]byte(identity.database)), nil
}

// mysqlDeleteJournalCreatorDigest is deliberately not a secret. It is a
// deterministic, create-time claim that this exact private-object prefix was
// issued for one run, immutable inventory, and canonical target identity.
// The table comment commits with CREATE TABLE, unlike the random header row
// that follows it, so recovery never has to trust shape alone.
func mysqlDeleteJournalCreatorDigest(
	request Stage4DeleteJournalReadinessRequest,
	targetIdentity string,
) (string, error) {
	if strings.TrimSpace(request.RunID) == "" ||
		request.RunID != strings.TrimSpace(request.RunID) {
		return "", errors.New("MySQL delete journal creator run ID is required")
	}
	if err := validateLowerSHA256(
		"MySQL delete journal creator inventory digest",
		request.InventoryDigest,
	); err != nil {
		return "", err
	}
	if strings.TrimSpace(targetIdentity) == "" ||
		targetIdentity != strings.TrimSpace(targetIdentity) {
		return "", errors.New("MySQL delete journal creator target identity is required")
	}
	payload, err := json.Marshal(struct {
		Version         int    `json:"version"`
		RunID           string `json:"run_id"`
		InventoryDigest string `json:"inventory_digest"`
		TargetIdentity  string `json:"target_identity"`
	}{
		Version: mysqlDeleteJournalVersion, RunID: request.RunID,
		InventoryDigest: request.InventoryDigest, TargetIdentity: targetIdentity,
	})
	if err != nil {
		return "", fmt.Errorf("encode MySQL delete journal creator authority: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func mysqlDeleteJournalComment(
	creatorDigest string,
	journalIdentity string,
) (string, error) {
	if err := validateLowerSHA256(
		"MySQL delete journal creator digest",
		creatorDigest,
	); err != nil {
		return "", err
	}
	if err := validateLowerSHA256(
		"MySQL delete journal identity",
		journalIdentity,
	); err != nil {
		return "", err
	}
	return mysqlDeleteJournalCommentV1 + ";creator=" + creatorDigest +
		";journal=" + journalIdentity, nil
}

func parseMySQLDeleteJournalComment(
	comment string,
) (creatorDigest string, journalIdentity string, _ error) {
	const (
		prefix = mysqlDeleteJournalCommentV1 + ";creator="
		middle = ";journal="
	)
	expectedLength := len(prefix) + sha256.Size*2 + len(middle) + sha256.Size*2
	if len(comment) != expectedLength || !strings.HasPrefix(comment, prefix) {
		return "", "", errors.New("MySQL delete receipt object collides with or differs from the exact DMTX journal creator marker")
	}
	creatorDigest = comment[len(prefix) : len(prefix)+sha256.Size*2]
	if comment[len(prefix)+sha256.Size*2:len(prefix)+sha256.Size*2+len(middle)] != middle {
		return "", "", errors.New("MySQL delete receipt object collides with or differs from the exact DMTX journal creator marker")
	}
	journalIdentity = comment[len(prefix)+sha256.Size*2+len(middle):]
	if err := validateLowerSHA256("MySQL delete journal creator digest", creatorDigest); err != nil {
		return "", "", errors.New("MySQL delete receipt journal creator digest is invalid")
	}
	if err := validateLowerSHA256("MySQL delete journal identity", journalIdentity); err != nil {
		return "", "", errors.New("MySQL delete receipt journal identity is invalid")
	}
	return creatorDigest, journalIdentity, nil
}

func mysqlDeleteJournalCreateSQL(
	creatorDigest string,
	journalIdentity string,
) (string, error) {
	comment, err := mysqlDeleteJournalComment(creatorDigest, journalIdentity)
	if err != nil {
		return "", err
	}
	// Both dynamic fragments were fixed-width lowercase SHA-256 hex before
	// interpolation, so the SQL literal cannot carry executable input.
	return mysqlDeleteJournalCreateSQLPrefix + "'" + comment + "'", nil
}

func validateMySQLDeleteJournalPrefixCreator(
	catalog mysqlDeleteJournalCatalog,
	expectedCreatorDigest string,
) error {
	if !catalog.Exists || !catalog.EmptyPrefix {
		return errors.New("MySQL delete receipt journal prefix authority is unavailable")
	}
	if err := validateLowerSHA256(
		"MySQL delete journal expected creator digest",
		expectedCreatorDigest,
	); err != nil {
		return err
	}
	if err := validateLowerSHA256(
		"MySQL delete journal observed creator digest",
		catalog.CreatorDigest,
	); err != nil {
		return err
	}
	if err := validateLowerSHA256(
		"MySQL delete journal observed identity",
		catalog.JournalIdentity,
	); err != nil {
		return err
	}
	if catalog.CreatorDigest != expectedCreatorDigest {
		return errors.New("MySQL delete receipt journal prefix creator authority differs from this run inventory or target; refusing takeover")
	}
	return nil
}

func mysqlDeleteReadinessFlavor(flavor engine.MySQLServerFlavor) (string, error) {
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		return "mysql80", nil
	case engine.MySQLServerFlavorMariaDB1011:
		return "mariadb1011", nil
	default:
		return "", errors.New("unsupported MySQL delete readiness flavor")
	}
}

func mysqlDeleteReadinessFromCatalog(
	request Stage4DeleteJournalReadinessRequest,
	workloadIdentity string,
	identity mysqlDeleteEndpointIdentity,
	catalog mysqlDeleteJournalCatalog,
) (state.Stage4DeleteJournalReadiness, error) {
	if strings.TrimSpace(workloadIdentity) == "" || !catalog.Exists ||
		catalog.EmptyPrefix {
		return state.Stage4DeleteJournalReadiness{}, errors.New("MySQL delete receipt journal lacks immutable authenticated authority")
	}
	if err := validateLowerSHA256("MySQL delete journal creator digest", catalog.CreatorDigest); err != nil {
		return state.Stage4DeleteJournalReadiness{}, errors.New("MySQL delete receipt journal creator authority is incomplete")
	}
	if err := validateLowerSHA256("MySQL delete journal identity", catalog.JournalIdentity); err != nil {
		return state.Stage4DeleteJournalReadiness{}, errors.New("MySQL delete receipt journal identity authority is incomplete")
	}
	expectedCatalogDigest, err := mysqlDeleteJournalCatalogDigest(
		identity.flavor,
		identity.database,
		catalog.CreatorDigest,
		catalog.JournalIdentity,
	)
	if err != nil || catalog.CatalogDigest != expectedCatalogDigest {
		return state.Stage4DeleteJournalReadiness{}, errors.New("MySQL delete receipt journal catalog authority differs from the exact authenticated journal")
	}
	journalDigest, err := mysqlDeleteReadinessJournalDigest(identity, catalog)
	if err != nil {
		return state.Stage4DeleteJournalReadiness{}, err
	}
	flavor, err := mysqlDeleteReadinessFlavor(identity.flavor)
	if err != nil {
		return state.Stage4DeleteJournalReadiness{}, err
	}
	readyAt := time.Now().UTC()
	if request.Existing != nil {
		readyAt = request.Existing.Readiness.ReadyAt
	}
	ready, err := state.NewStage4DeleteJournalReadiness(
		request.RunID,
		request.InventoryDigest,
		workloadIdentity,
		"mysql",
		flavor,
		identity.version,
		journalDigest,
		mysqlDeleteJournalVersion,
		readyAt,
	)
	if err != nil {
		return state.Stage4DeleteJournalReadiness{}, fmt.Errorf("construct MySQL delete journal readiness: %w", err)
	}
	if request.Existing != nil && !ready.Equal(request.Existing.Readiness) {
		return state.Stage4DeleteJournalReadiness{}, errors.New("exact native MySQL delete journal reread differs from durable readiness authority")
	}
	return ready, nil
}

// mysqlDeleteReadinessJournalDigest keeps the application's configured
// workload identity in the state receipt while folding the exact private
// journal catalog and native server/database incarnation into its immutable
// catalog authority. A matching endpoint string alone can never make a
// replaced database appear to be the previously verified journal target.
func mysqlDeleteReadinessJournalDigest(
	identity mysqlDeleteEndpointIdentity,
	catalog mysqlDeleteJournalCatalog,
) (string, error) {
	nativeIdentity, err := mysqlDeleteCanonicalTargetIdentity(identity)
	if err != nil {
		return "", err
	}
	if !catalog.Exists || catalog.EmptyPrefix ||
		validateLowerSHA256(
			"MySQL delete journal catalog digest",
			catalog.CatalogDigest,
		) != nil {
		return "", errors.New("MySQL delete journal readiness catalog authority is incomplete")
	}
	payload, err := json.Marshal(struct {
		Version     int    `json:"version"`
		Incarnation string `json:"incarnation"`
		Catalog     string `json:"catalog"`
	}{
		Version: 1, Incarnation: nativeIdentity, Catalog: catalog.CatalogDigest,
	})
	if err != nil {
		return "", fmt.Errorf("encode MySQL delete journal readiness authority: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

type mysqlDeleteJournalPrepareAction uint8

const (
	mysqlDeleteJournalPrepareVerify mysqlDeleteJournalPrepareAction = iota + 1
	mysqlDeleteJournalPrepareCreate
	mysqlDeleteJournalPrepareAuthenticatePrefix
)

// mysqlDeleteJournalPreparationAction is intentionally a small pure policy
// seam. It documents the crash-recovery distinction between a no-receipt run
// (which may finish an exact empty CREATE TABLE prefix) and a run with durable
// receipt authority (which may only exact-reread a fully authenticated
// journal).
func mysqlDeleteJournalPreparationAction(
	catalog mysqlDeleteJournalCatalog,
	hasExistingReceipt bool,
) (mysqlDeleteJournalPrepareAction, error) {
	if hasExistingReceipt {
		if !catalog.Exists {
			return 0, errors.New("durable MySQL delete journal readiness exists but the private journal is absent; refusing recreation")
		}
		if catalog.EmptyPrefix {
			return 0, errors.New("durable MySQL delete journal readiness exists but the private journal lacks its immutable header; refusing authentication or recreation")
		}
		return mysqlDeleteJournalPrepareVerify, nil
	}
	if !catalog.Exists {
		return mysqlDeleteJournalPrepareCreate, nil
	}
	if catalog.EmptyPrefix {
		return mysqlDeleteJournalPrepareAuthenticatePrefix, nil
	}
	return mysqlDeleteJournalPrepareVerify, nil
}

func mysqlDeleteJournalPrivileges(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	namespace string,
) (map[string]bool, error) {
	return mysqlDeletePrivileges(ctx, queryer, namespace, mysqlDeleteJournalTable)
}

// inspectMySQLDeleteReceiptJournal authenticates the private table shape plus
// its immutable header marker. The marker makes an ordinary DROP/CREATE
// replacement observably different even if its DDL text is copied exactly.
//
// An exact, completely empty shape is the one recoverable auto-commit prefix:
// CREATE TABLE may commit before the process reaches the separate immutable
// header INSERT. It is deliberately reported rather than authenticated. Only
// a no-receipt Prepare call holding the pinned GET_LOCK and matching the
// creator claim may turn that prefix into a journal. A headerless table
// containing any row is never a prefix and fails closed.
func inspectMySQLDeleteReceiptJournal(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	flavor engine.MySQLServerFlavor,
	namespace string,
) (mysqlDeleteJournalCatalog, error) {
	shape, err := inspectMySQLDeleteJournalShape(ctx, queryer, flavor, namespace)
	if err != nil || !shape.Exists {
		return mysqlDeleteJournalCatalog{Exists: shape.Exists}, err
	}
	header, found, err := loadMySQLDeleteJournalHeader(ctx, queryer, false)
	if err != nil {
		return mysqlDeleteJournalCatalog{}, err
	}
	if !found {
		rowCount, countErr := countMySQLDeleteJournalRows(ctx, queryer)
		if countErr != nil {
			return mysqlDeleteJournalCatalog{}, countErr
		}
		return mysqlDeleteJournalHeaderlessCatalog(
			rowCount,
			shape.CreatorDigest,
			shape.JournalIdentity,
		)
	}
	if err := validateMySQLDeleteJournalHeader(
		header,
		shape.CreatorDigest,
		shape.JournalIdentity,
	); err != nil {
		return mysqlDeleteJournalCatalog{}, err
	}
	var headerCount int
	if err := queryer.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(mysqlDeleteJournalTable)+" WHERE `entry_kind` = ?",
		mysqlDeleteJournalHeaderKind,
	).Scan(&headerCount); err != nil {
		return mysqlDeleteJournalCatalog{}, fmt.Errorf("count MySQL delete journal headers: %w", err)
	}
	if headerCount != 1 {
		return mysqlDeleteJournalCatalog{}, fmt.Errorf("MySQL delete receipt journal has %d header rows, want exactly one", headerCount)
	}
	digest, err := mysqlDeleteJournalCatalogDigest(
		flavor,
		namespace,
		shape.CreatorDigest,
		shape.JournalIdentity,
	)
	if err != nil {
		return mysqlDeleteJournalCatalog{}, err
	}
	return mysqlDeleteJournalCatalog{
		Exists:          true,
		CreatorDigest:   shape.CreatorDigest,
		JournalIdentity: shape.JournalIdentity,
		CatalogDigest:   digest,
	}, nil
}

func mysqlDeleteJournalHeaderlessCatalog(
	rowCount int,
	creatorDigest string,
	journalIdentity string,
) (mysqlDeleteJournalCatalog, error) {
	if rowCount < 0 {
		return mysqlDeleteJournalCatalog{}, errors.New("MySQL delete receipt journal row count is invalid")
	}
	if rowCount != 0 {
		return mysqlDeleteJournalCatalog{}, fmt.Errorf("MySQL delete receipt journal lacks its immutable header and has %d rows", rowCount)
	}
	if err := validateLowerSHA256("MySQL delete journal creator digest", creatorDigest); err != nil {
		return mysqlDeleteJournalCatalog{}, err
	}
	if err := validateLowerSHA256("MySQL delete journal identity", journalIdentity); err != nil {
		return mysqlDeleteJournalCatalog{}, err
	}
	return mysqlDeleteJournalCatalog{
		Exists:          true,
		EmptyPrefix:     true,
		CreatorDigest:   creatorDigest,
		JournalIdentity: journalIdentity,
	}, nil
}

func countMySQLDeleteJournalRows(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
) (int, error) {
	var count int
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+mySQLIdentifier(mysqlDeleteJournalTable),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count MySQL delete receipt journal rows: %w", err)
	}
	return count, nil
}

func inspectMySQLDeleteJournalShape(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	flavor engine.MySQLServerFlavor,
	namespace string,
) (mysqlDeleteJournalShape, error) {
	if queryer == nil || !supportedMySQLDeleteFlavor(flavor) || strings.TrimSpace(namespace) == "" {
		return mysqlDeleteJournalShape{}, errors.New("MySQL delete receipt journal catalog is unavailable")
	}
	var tableType, engineName, collation, comment, createOptions sql.NullString
	err := queryer.QueryRowContext(ctx, `
		SELECT TABLE_TYPE, ENGINE, TABLE_COLLATION, TABLE_COMMENT, CREATE_OPTIONS
		  FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, namespace, mysqlDeleteJournalTable,
	).Scan(&tableType, &engineName, &collation, &comment, &createOptions)
	if errors.Is(err, sql.ErrNoRows) {
		return mysqlDeleteJournalShape{}, nil
	}
	if err != nil {
		return mysqlDeleteJournalShape{}, fmt.Errorf("inspect MySQL delete receipt relation: %w", err)
	}
	if !tableType.Valid || !engineName.Valid || !collation.Valid ||
		!strings.EqualFold(tableType.String, "BASE TABLE") ||
		!strings.EqualFold(engineName.String, "InnoDB") ||
		!strings.EqualFold(collation.String, "ascii_bin") ||
		!comment.Valid ||
		(createOptions.Valid && strings.TrimSpace(createOptions.String) != "") {
		return mysqlDeleteJournalShape{}, errors.New("MySQL delete receipt object collides with or differs from the exact DMTX journal")
	}
	creatorDigest, journalIdentity, err := parseMySQLDeleteJournalComment(comment.String)
	if err != nil {
		return mysqlDeleteJournalShape{}, err
	}
	if err := validateMySQLDeleteJournalColumns(ctx, queryer, flavor, namespace); err != nil {
		return mysqlDeleteJournalShape{}, err
	}
	if err := validateMySQLDeleteJournalIndexes(ctx, queryer, namespace); err != nil {
		return mysqlDeleteJournalShape{}, err
	}
	for _, check := range []struct {
		label string
		query string
	}{
		{"foreign key", "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL"},
		{"trigger", "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?"},
		{"partition", "SELECT COUNT(*) FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL"},
	} {
		var count int
		if err := queryer.QueryRowContext(ctx, check.query, namespace, mysqlDeleteJournalTable).Scan(&count); err != nil {
			return mysqlDeleteJournalShape{}, fmt.Errorf("inspect MySQL delete receipt journal %s authority: %w", check.label, err)
		}
		if count != 0 {
			return mysqlDeleteJournalShape{}, fmt.Errorf("MySQL delete receipt journal has unexpected %s authority", check.label)
		}
	}
	rows, err := queryer.QueryContext(ctx, `
		SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE
		  FROM information_schema.TABLE_CONSTRAINTS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY CONSTRAINT_NAME, CONSTRAINT_TYPE`, namespace, mysqlDeleteJournalTable)
	if err != nil {
		return mysqlDeleteJournalShape{}, fmt.Errorf("inspect MySQL delete receipt journal constraints: %w", err)
	}
	defer rows.Close()
	constraints := 0
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return mysqlDeleteJournalShape{}, fmt.Errorf("read MySQL delete receipt journal constraint: %w", err)
		}
		if name != "PRIMARY" || !strings.EqualFold(kind, "PRIMARY KEY") {
			return mysqlDeleteJournalShape{}, errors.New("MySQL delete receipt journal has unexpected constraint authority")
		}
		constraints++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return mysqlDeleteJournalShape{}, fmt.Errorf("iterate MySQL delete receipt journal constraints: %w", err)
	}
	if constraints != 1 {
		return mysqlDeleteJournalShape{}, errors.New("MySQL delete receipt journal lacks its exact primary-key constraint")
	}
	return mysqlDeleteJournalShape{
		Exists:          true,
		CreatorDigest:   creatorDigest,
		JournalIdentity: journalIdentity,
	}, nil
}

func validateMySQLDeleteJournalColumns(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	flavor engine.MySQLServerFlavor,
	namespace string,
) error {
	type expectedColumn struct {
		name, typ, charset, collation string
	}
	expected := []expectedColumn{
		{"journal_version", "smallint unsigned", "", ""},
		{"entry_kind", "char(1)", "ascii", "ascii_bin"},
		{"token", "char(64)", "ascii", "ascii_bin"},
		{"plan_id", "char(32)", "ascii", "ascii_bin"},
		{"sequence", "bigint unsigned", "", ""},
		{"batch_digest", "char(64)", "ascii", "ascii_bin"},
		{"candidates", "bigint unsigned", "", ""},
		{"deleted_rows", "bigint unsigned", "", ""},
		{"target_catalog_digest", "char(64)", "ascii", "ascii_bin"},
		{"target_identity_digest", "char(64)", "ascii", "ascii_bin"},
		{"journal_identity", "char(64)", "ascii", "ascii_bin"},
		{"receipt_digest", "char(64)", "ascii", "ascii_bin"},
	}
	rows, err := queryer.QueryContext(ctx, `
		SELECT ORDINAL_POSITION, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE,
		       COLUMN_DEFAULT, EXTRA, CHARACTER_SET_NAME, COLLATION_NAME
		  FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY ORDINAL_POSITION`, namespace, mysqlDeleteJournalTable)
	if err != nil {
		return fmt.Errorf("inspect MySQL delete receipt journal columns: %w", err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var ordinal int
		var name, typ, nullable, extra string
		var defaultValue, charset, collation sql.NullString
		if err := rows.Scan(&ordinal, &name, &typ, &nullable, &defaultValue, &extra, &charset, &collation); err != nil {
			return fmt.Errorf("read MySQL delete receipt journal column: %w", err)
		}
		if index >= len(expected) || ordinal != index+1 || name != expected[index].name ||
			!strings.EqualFold(mysqlDeleteJournalColumnType(flavor, typ), expected[index].typ) || !strings.EqualFold(nullable, "NO") ||
			defaultValue.Valid || strings.TrimSpace(extra) != "" ||
			strings.ToLower(charset.String) != expected[index].charset ||
			strings.ToLower(collation.String) != expected[index].collation {
			return errors.New("MySQL delete receipt journal column authority differs")
		}
		index++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("iterate MySQL delete receipt journal columns: %w", err)
	}
	if index != len(expected) {
		return errors.New("MySQL delete receipt journal column count differs")
	}
	return nil
}

// mysqlDeleteJournalColumnType accepts only MariaDB 10.11's server-rendered
// integer display widths for the exact integer declarations in the private
// journal DDL. They do not alter the stored type or its unsigned authority;
// every other catalog spelling remains an authority mismatch.
func mysqlDeleteJournalColumnType(flavor engine.MySQLServerFlavor, typ string) string {
	if flavor != engine.MySQLServerFlavorMariaDB1011 {
		return typ
	}
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "smallint(5) unsigned":
		return "smallint unsigned"
	case "bigint(20) unsigned":
		return "bigint unsigned"
	default:
		return typ
	}
}

func validateMySQLDeleteJournalIndexes(ctx context.Context, queryer engine.MySQLCatalogQueryer, namespace string) error {
	rows, err := queryer.QueryContext(ctx, `
		SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME,
		       COLLATION, INDEX_TYPE, SUB_PART
		  FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY INDEX_NAME, SEQ_IN_INDEX`, namespace, mysqlDeleteJournalTable)
	if err != nil {
		return fmt.Errorf("inspect MySQL delete receipt journal indexes: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name, column, indexType string
		var nonUnique, sequence int
		var collation sql.NullString
		var subPart sql.NullInt64
		if err := rows.Scan(&name, &nonUnique, &sequence, &column, &collation, &indexType, &subPart); err != nil {
			return fmt.Errorf("read MySQL delete receipt journal index: %w", err)
		}
		if name != "PRIMARY" || nonUnique != 0 || sequence != 1 || column != "token" ||
			!collation.Valid || !strings.EqualFold(collation.String, "A") ||
			!strings.EqualFold(indexType, "BTREE") || subPart.Valid {
			return errors.New("MySQL delete receipt journal index authority differs")
		}
		count++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("iterate MySQL delete receipt journal indexes: %w", err)
	}
	if count != 1 {
		return errors.New("MySQL delete receipt journal lacks an exact token primary-key index")
	}
	return nil
}

func mysqlDeleteJournalCatalogDigest(
	flavor engine.MySQLServerFlavor,
	namespace string,
	creatorDigest string,
	journalIdentity string,
) (string, error) {
	if !supportedMySQLDeleteFlavor(flavor) || strings.TrimSpace(namespace) == "" ||
		validateLowerSHA256("MySQL delete journal creator digest", creatorDigest) != nil ||
		validateLowerSHA256("MySQL delete journal identity", journalIdentity) != nil {
		return "", errors.New("MySQL delete receipt journal catalog authority is incomplete")
	}
	payload, err := json.Marshal(struct {
		Version         int                      `json:"version"`
		Flavor          engine.MySQLServerFlavor `json:"flavor"`
		Namespace       string                   `json:"namespace"`
		Table           string                   `json:"table"`
		CreatorDigest   string                   `json:"creator_digest"`
		JournalIdentity string                   `json:"journal_identity"`
	}{mysqlDeleteJournalVersion, flavor, namespace, mysqlDeleteJournalTable, creatorDigest, journalIdentity})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func loadMySQLDeleteJournalHeader(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	forUpdate bool,
) (mysqlDeleteJournalHeader, bool, error) {
	if queryer == nil {
		return mysqlDeleteJournalHeader{}, false, errors.New("MySQL delete receipt journal queryer is unavailable")
	}
	query := "SELECT `journal_version`, `entry_kind`, `token`, `plan_id`, `sequence`, `batch_digest`, `candidates`, `deleted_rows`, `target_catalog_digest`, `target_identity_digest`, `journal_identity`, `receipt_digest` FROM " + mySQLIdentifier(mysqlDeleteJournalTable) + " WHERE `token` = ?"
	if forUpdate {
		query += " FOR UPDATE"
	}
	var header mysqlDeleteJournalHeader
	err := queryer.QueryRowContext(ctx, query, mysqlDeleteJournalHeaderToken).Scan(
		&header.JournalVersion,
		&header.EntryKind,
		&header.Token,
		&header.PlanID,
		&header.Sequence,
		&header.BatchDigest,
		&header.Candidates,
		&header.DeletedRows,
		&header.TargetCatalogDigest,
		&header.TargetIdentityDigest,
		&header.JournalIdentity,
		&header.ReceiptDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return mysqlDeleteJournalHeader{}, false, nil
	}
	if err != nil {
		return mysqlDeleteJournalHeader{}, false, fmt.Errorf("load MySQL delete receipt journal header: %w", err)
	}
	return header, true, nil
}

func validateMySQLDeleteJournalHeader(
	header mysqlDeleteJournalHeader,
	creatorDigest string,
	journalIdentity string,
) error {
	if header.JournalVersion != mysqlDeleteJournalVersion ||
		header.EntryKind != mysqlDeleteJournalHeaderKind ||
		header.Token != mysqlDeleteJournalHeaderToken ||
		header.PlanID != mysqlDeleteJournalZeroPlanID ||
		header.Sequence != 0 ||
		header.BatchDigest != mysqlDeleteJournalHeaderToken ||
		header.Candidates != 0 || header.DeletedRows != 0 ||
		header.TargetCatalogDigest != mysqlDeleteJournalHeaderToken ||
		header.TargetIdentityDigest != mysqlDeleteJournalHeaderToken ||
		validateLowerSHA256("MySQL delete journal header identity", header.JournalIdentity) != nil ||
		header.JournalIdentity != journalIdentity {
		return errors.New("MySQL delete receipt journal header differs from the exact DMTX authority")
	}
	expected, err := mysqlDeleteJournalHeaderDigest(creatorDigest, journalIdentity)
	if err != nil || header.ReceiptDigest != expected {
		return errors.New("MySQL delete receipt journal header digest differs")
	}
	return nil
}

func mysqlDeleteJournalHeaderDigest(
	creatorDigest string,
	journalIdentity string,
) (string, error) {
	if err := validateLowerSHA256("MySQL delete journal creator digest", creatorDigest); err != nil {
		return "", err
	}
	if err := validateLowerSHA256("MySQL delete journal header identity", journalIdentity); err != nil {
		return "", err
	}
	payload, err := json.Marshal(struct {
		JournalVersion  int    `json:"journal_version"`
		EntryKind       string `json:"entry_kind"`
		Token           string `json:"token"`
		PlanID          string `json:"plan_id"`
		CreatorDigest   string `json:"creator_digest"`
		JournalIdentity string `json:"journal_identity"`
	}{mysqlDeleteJournalVersion, mysqlDeleteJournalHeaderKind, mysqlDeleteJournalHeaderToken, mysqlDeleteJournalZeroPlanID, creatorDigest, journalIdentity})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func newMySQLDeleteJournalIdentity() (string, error) {
	buffer := make([]byte, sha256.Size)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate MySQL delete journal identity: %w", err)
	}
	return hex.EncodeToString(buffer), nil
}

func createMySQLDeleteReceiptJournal(
	ctx context.Context,
	connection *sql.Conn,
	flavor engine.MySQLServerFlavor,
	namespace string,
	creatorDigest string,
) error {
	if connection == nil {
		return errors.New("pinned MySQL delete receipt journal connection is unavailable")
	}
	journalIdentity, err := newMySQLDeleteJournalIdentity()
	if err != nil {
		return err
	}
	createSQL, err := mysqlDeleteJournalCreateSQL(creatorDigest, journalIdentity)
	if err != nil {
		return err
	}
	_, createErr := connection.ExecContext(ctx, createSQL)
	shape, shapeErr := inspectMySQLDeleteJournalShape(ctx, connection, flavor, namespace)
	if createErr != nil && (shapeErr != nil || !shape.Exists) {
		return errors.Join(
			fmt.Errorf("create MySQL delete receipt journal: %w", createErr),
			shapeErr,
		)
	}
	if shapeErr != nil || !shape.Exists {
		return errors.Join(
			fmt.Errorf("verify created MySQL delete receipt journal: %w", shapeErr),
			createErr,
		)
	}
	if shape.CreatorDigest != creatorDigest || shape.JournalIdentity != journalIdentity {
		return errors.Join(
			errors.New("created MySQL delete receipt journal marker differs from this run authority"),
			createErr,
		)
	}
	return authenticateEmptyMySQLDeleteReceiptJournal(
		ctx,
		connection,
		flavor,
		namespace,
		creatorDigest,
	)
}

// authenticateEmptyMySQLDeleteReceiptJournal completes only the exact empty
// CREATE TABLE prefix. It must run while Prepare owns the session-pinned
// GET_LOCK. It never repairs a table with an unexpected row or a malformed
// header, because either could be a replacement after durable authority.
func authenticateEmptyMySQLDeleteReceiptJournal(
	ctx context.Context,
	connection *sql.Conn,
	flavor engine.MySQLServerFlavor,
	namespace string,
	creatorDigest string,
) error {
	if connection == nil {
		return errors.New("pinned MySQL delete receipt journal connection is unavailable")
	}
	catalog, err := inspectMySQLDeleteReceiptJournal(
		ctx,
		connection,
		flavor,
		namespace,
	)
	if err != nil {
		return fmt.Errorf("inspect MySQL delete receipt journal before authentication: %w", err)
	}
	if !catalog.Exists {
		return errors.New("MySQL delete receipt journal is absent before immutable header authentication")
	}
	if err := validateMySQLDeleteJournalPrefixCreator(catalog, creatorDigest); err != nil {
		return err
	}
	if !catalog.EmptyPrefix {
		return nil
	}
	digest, err := mysqlDeleteJournalHeaderDigest(
		creatorDigest,
		catalog.JournalIdentity,
	)
	if err != nil {
		return err
	}
	_, insertErr := connection.ExecContext(ctx,
		"INSERT INTO "+mySQLIdentifier(mysqlDeleteJournalTable)+" (`journal_version`, `entry_kind`, `token`, `plan_id`, `sequence`, `batch_digest`, `candidates`, `deleted_rows`, `target_catalog_digest`, `target_identity_digest`, `journal_identity`, `receipt_digest`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		mysqlDeleteJournalVersion,
		mysqlDeleteJournalHeaderKind,
		mysqlDeleteJournalHeaderToken,
		mysqlDeleteJournalZeroPlanID,
		uint64(0),
		mysqlDeleteJournalHeaderToken,
		uint64(0),
		uint64(0),
		mysqlDeleteJournalHeaderToken,
		mysqlDeleteJournalHeaderToken,
		catalog.JournalIdentity,
		digest,
	)
	observed, readErr := inspectMySQLDeleteReceiptJournal(
		ctx,
		connection,
		flavor,
		namespace,
	)
	if insertErr != nil && (readErr != nil || !observed.Exists || observed.EmptyPrefix ||
		observed.CreatorDigest != creatorDigest || observed.JournalIdentity != catalog.JournalIdentity) {
		return errors.Join(
			fmt.Errorf("create immutable MySQL delete receipt journal header: %w", insertErr),
			readErr,
		)
	}
	if readErr != nil || !observed.Exists || observed.EmptyPrefix {
		return errors.Join(
			fmt.Errorf("reread immutable MySQL delete receipt journal header: %w", readErr),
			insertErr,
		)
	}
	if observed.CreatorDigest != creatorDigest || observed.JournalIdentity != catalog.JournalIdentity {
		return errors.New("MySQL delete receipt journal header identity differs after creation")
	}
	return nil
}

func mysqlDeleteReceiptDigest(
	receipt deleteTargetBatchReceipt,
	authority mysqlDeleteCatalogAuthority,
	targetIdentityDigest string,
	journal mysqlDeleteJournalCatalog,
) (string, error) {
	if !sameMySQLDeleteCatalogAuthority(authority, mustMySQLDeleteCatalogAuthority(authority)) ||
		validateLowerSHA256("MySQL delete receipt target identity", targetIdentityDigest) != nil ||
		!journal.Exists || journal.EmptyPrefix ||
		validateLowerSHA256("MySQL delete receipt journal creator digest", journal.CreatorDigest) != nil ||
		validateLowerSHA256("MySQL delete receipt journal identity", journal.JournalIdentity) != nil {
		return "", errors.New("MySQL delete receipt authority is incomplete")
	}
	payload, err := json.Marshal(struct {
		JournalVersion       int    `json:"journal_version"`
		CreatorDigest        string `json:"creator_digest"`
		JournalIdentity      string `json:"journal_identity"`
		PlanID               string `json:"plan_id"`
		Token                string `json:"token"`
		Sequence             int64  `json:"sequence"`
		BatchDigest          string `json:"batch_digest"`
		Candidates           int64  `json:"candidates"`
		DeletedRows          int64  `json:"deleted_rows"`
		TargetCatalogDigest  string `json:"target_catalog_digest"`
		TargetIdentityDigest string `json:"target_identity_digest"`
	}{mysqlDeleteJournalVersion, journal.CreatorDigest, journal.JournalIdentity, receipt.PlanID, receipt.Token, receipt.Sequence, receipt.BatchDigest, receipt.Candidates, receipt.DeletedRows, authority.CatalogDigest, targetIdentityDigest})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func mysqlDeleteIdentityDigest(identity string) string {
	digest := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(digest[:])
}

func validateMySQLDeleteBatchAuthority(
	batch deleteTargetBatch,
	authority mysqlDeleteCatalogAuthority,
) error {
	if !sameMySQLDeleteCatalogAuthority(authority, mustMySQLDeleteCatalogAuthority(authority)) {
		return errors.New("MySQL delete target catalog authority is malformed")
	}
	if !mysqlDeleteTableMatchesAuthority(batch.Table, authority.Table) {
		return errors.New("MySQL delete batch table differs from admitted target catalog authority")
	}
	if len(batch.Columns) != len(authority.PrimaryKey) {
		return errors.New("MySQL delete batch key width differs from admitted target primary key")
	}
	for index := range authority.PrimaryKey {
		if batch.Columns[index] != authority.PrimaryKey[index].Name {
			return errors.New("MySQL delete batch columns are not in exact admitted primary-key order")
		}
	}
	return nil
}

func lockMySQLDeleteTargetRelation(
	ctx context.Context,
	connection *sql.Conn,
	table schema.Table,
) error {
	if connection == nil {
		return errors.New("pinned MySQL delete target connection is unavailable")
	}
	rows, err := connection.QueryContext(ctx,
		"SELECT 1 FROM "+mySQLQualified(table.Schema, table.Name)+" WHERE 1 = 0 FOR UPDATE")
	if err != nil {
		return err
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	return nil
}

func loadMySQLDeleteReceipt(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	token string,
	authority mysqlDeleteCatalogAuthority,
	targetIdentityDigest string,
	journal mysqlDeleteJournalCatalog,
	forUpdate bool,
) (deleteTargetBatchReceipt, bool, error) {
	if queryer == nil || !journal.Exists || journal.EmptyPrefix ||
		validateLowerSHA256("MySQL delete receipt token", token) != nil {
		return deleteTargetBatchReceipt{}, false, errors.New("MySQL delete receipt lookup authority is incomplete")
	}
	query := "SELECT `journal_version`, `entry_kind`, `plan_id`, `token`, `sequence`, `batch_digest`, `candidates`, `deleted_rows`, `target_catalog_digest`, `target_identity_digest`, `journal_identity`, `receipt_digest` FROM " + mySQLIdentifier(mysqlDeleteJournalTable) + " WHERE `token` = ?"
	if forUpdate {
		query += " FOR UPDATE"
	}
	var row mysqlDeleteReceiptRow
	err := queryer.QueryRowContext(ctx, query, token).Scan(
		&row.JournalVersion,
		&row.EntryKind,
		&row.Receipt.PlanID,
		&row.Receipt.Token,
		&row.Receipt.Sequence,
		&row.Receipt.BatchDigest,
		&row.Receipt.Candidates,
		&row.Receipt.DeletedRows,
		&row.TargetCatalogDigest,
		&row.TargetIdentityDigest,
		&row.JournalIdentity,
		&row.Receipt.ReceiptDigest,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return deleteTargetBatchReceipt{}, false, nil
	}
	if err != nil {
		return deleteTargetBatchReceipt{}, false, fmt.Errorf("load MySQL delete receipt: %w", err)
	}
	if row.JournalVersion != mysqlDeleteJournalVersion ||
		row.EntryKind != mysqlDeleteJournalReceiptKind ||
		row.TargetCatalogDigest != authority.CatalogDigest ||
		row.TargetIdentityDigest != targetIdentityDigest ||
		row.JournalIdentity != journal.JournalIdentity {
		return deleteTargetBatchReceipt{}, false, errors.New("MySQL delete receipt authority differs from the exact target/journal")
	}
	expected, err := mysqlDeleteReceiptDigest(row.Receipt, authority, targetIdentityDigest, journal)
	if err != nil || row.Receipt.ReceiptDigest != expected {
		return deleteTargetBatchReceipt{}, false, errors.New("MySQL delete receipt digest differs from immutable receipt authority")
	}
	return row.Receipt, true, nil
}

func validateMySQLDeleteReceipt(
	batch deleteTargetBatch,
	receipt deleteTargetBatchReceipt,
	authority mysqlDeleteCatalogAuthority,
	targetIdentityDigest string,
	journal mysqlDeleteJournalCatalog,
) error {
	if receipt.PlanID != batch.PlanID || receipt.Token != batch.Token ||
		receipt.Sequence != batch.Sequence || receipt.BatchDigest != batch.BatchDigest ||
		receipt.Candidates != int64(len(batch.Keys)) || receipt.DeletedRows < 0 ||
		receipt.DeletedRows > receipt.Candidates || receipt.FailClosedReason != "" {
		return errors.New("MySQL delete receipt differs from pending batch")
	}
	expected, err := mysqlDeleteReceiptDigest(receipt, authority, targetIdentityDigest, journal)
	if err != nil || receipt.ReceiptDigest != expected {
		return errors.New("MySQL delete receipt digest differs from immutable receipt authority")
	}
	return nil
}

func insertMySQLDeleteReceipt(
	ctx context.Context,
	connection *sql.Conn,
	receipt deleteTargetBatchReceipt,
	authority mysqlDeleteCatalogAuthority,
	targetIdentityDigest string,
	journal mysqlDeleteJournalCatalog,
) error {
	if connection == nil {
		return errors.New("pinned MySQL delete receipt connection is unavailable")
	}
	result, err := connection.ExecContext(ctx,
		"INSERT INTO "+mySQLIdentifier(mysqlDeleteJournalTable)+" (`journal_version`, `entry_kind`, `token`, `plan_id`, `sequence`, `batch_digest`, `candidates`, `deleted_rows`, `target_catalog_digest`, `target_identity_digest`, `journal_identity`, `receipt_digest`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		mysqlDeleteJournalVersion,
		mysqlDeleteJournalReceiptKind,
		receipt.Token,
		receipt.PlanID,
		receipt.Sequence,
		receipt.BatchDigest,
		receipt.Candidates,
		receipt.DeletedRows,
		authority.CatalogDigest,
		targetIdentityDigest,
		journal.JournalIdentity,
		receipt.ReceiptDigest,
	)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return fmt.Errorf("persist MySQL delete receipt affected=%d err=%w", affected, err)
	}
	return nil
}

func rollbackMySQLDeleteReceiptTransaction(
	ctx context.Context,
	connection *sql.Conn,
) error {
	if connection == nil {
		return nil
	}
	cleanupCtx, cancel := mysqlDeleteDetachedContext(ctx)
	defer cancel()
	_, err := connection.ExecContext(cleanupCtx, "ROLLBACK")
	return err
}

func (adapter *mysqlTargetAdapter) commitMySQLDeleteReceipt(
	ctx context.Context,
	connection *sql.Conn,
) (sql.Result, error) {
	if adapter != nil && adapter.deleteCommit != nil {
		return adapter.deleteCommit(ctx, connection)
	}
	return connection.ExecContext(ctx, "COMMIT")
}

// ApplyDeleteBatch owns one pinned InnoDB transaction containing both the
// target DELETE and the immutable receipt insert. It re-proves the relation
// after a metadata-locking SELECT FOR UPDATE and refuses to create the journal
// here: readiness is the only authority allowed to issue that DDL.
func (capability *mysqlDeleteTargetCapability) ApplyDeleteBatch(
	ctx context.Context,
	batch deleteTargetBatch,
) (result deleteTargetBatchReceipt, resultErr error) {
	if capability == nil || capability.adapter == nil || capability.adapter.database == nil ||
		strings.TrimSpace(capability.targetIdentity) == "" {
		return result, errors.New("MySQL delete target is unavailable")
	}
	adapter := capability.adapter
	keys, err := validateMySQLDeleteBatch(adapter.namespace, batch)
	if err != nil {
		return result, err
	}
	if err := validateMySQLDeleteBatchAuthority(batch, capability.authority); err != nil {
		return result, err
	}
	statement, err := mysqlDeleteBatchStatement(batch.Table, batch.Columns, len(keys))
	if err != nil {
		return result, err
	}
	connection, err := adapter.database.Conn(ctx)
	if err != nil {
		return result, fmt.Errorf("acquire pinned MySQL delete receipt connection: %w", err)
	}
	active := false
	closed := false
	defer func() {
		if active {
			if rollbackErr := rollbackMySQLDeleteReceiptTransaction(ctx, connection); rollbackErr != nil {
				discardMySQLConnection(connection)
				result = deleteTargetBatchReceipt{}
				resultErr = errors.Join(resultErr, fmt.Errorf("roll back MySQL delete receipt transaction: %w", rollbackErr))
			}
		}
		if !closed {
			if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
				result = deleteTargetBatchReceipt{}
				resultErr = errors.Join(resultErr, fmt.Errorf("close pinned MySQL delete receipt connection: %w", closeErr))
			}
		}
	}()
	if err := engine.VerifyMySQLTargetForFlavor(ctx, connection, adapter.flavor); err != nil {
		return result, fmt.Errorf("verify pinned MySQL delete target flavor/session: %w", err)
	}
	identity, err := readMySQLDeleteEndpointIdentity(ctx, connection, adapter.flavor)
	if err != nil {
		return result, fmt.Errorf("read pinned MySQL delete target identity: %w", err)
	}
	canonicalIdentity, err := mysqlDeleteCanonicalTargetIdentity(identity)
	if err != nil || canonicalIdentity != capability.targetIdentity {
		if err == nil {
			err = errors.New("pinned MySQL delete target identity differs from admitted identity")
		}
		return result, err
	}
	targetIdentityDigest := mysqlDeleteIdentityDigest(canonicalIdentity)
	if _, err := connection.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		return result, fmt.Errorf("set MySQL delete receipt transaction isolation: %w", err)
	}
	if _, err := connection.ExecContext(ctx, "START TRANSACTION"); err != nil {
		return result, fmt.Errorf("begin MySQL delete receipt transaction: %w", err)
	}
	active = true
	if err := lockMySQLDeleteTargetRelation(ctx, connection, batch.Table); err != nil {
		return result, fmt.Errorf("lock MySQL delete target catalog before mutation: %w", err)
	}
	lockedAuthority, err := inspectMySQLDeleteCatalogAuthority(
		ctx, connection, adapter.flavor, adapter.namespace,
		capability.authority.Table, true, true,
	)
	if err != nil {
		return result, fmt.Errorf("revalidate locked MySQL delete target catalog: %w", err)
	}
	if !sameMySQLDeleteCatalogAuthority(capability.authority, lockedAuthority) {
		return result, errors.New("locked MySQL delete target catalog authority changed")
	}
	journal, err := inspectMySQLDeleteReceiptJournal(ctx, connection, adapter.flavor, adapter.namespace)
	if err != nil {
		return result, fmt.Errorf("reread exact MySQL delete receipt journal: %w", err)
	}
	if !journal.Exists || journal.EmptyPrefix {
		return result, errors.New("MySQL delete receipt journal lacks immutable authenticated authority after durable readiness; refusing recreation")
	}
	header, found, err := loadMySQLDeleteJournalHeader(ctx, connection, true)
	if err != nil || !found || header.JournalIdentity != journal.JournalIdentity {
		return result, errors.Join(errors.New("lock exact MySQL delete receipt journal header"), err)
	}
	if err := validateMySQLDeleteJournalHeader(
		header,
		journal.CreatorDigest,
		journal.JournalIdentity,
	); err != nil {
		return result, err
	}
	stored, found, err := loadMySQLDeleteReceipt(ctx, connection, batch.Token, lockedAuthority, targetIdentityDigest, journal, true)
	if err != nil {
		return result, err
	}
	if found {
		if err := validateMySQLDeleteReceipt(batch, stored, lockedAuthority, targetIdentityDigest, journal); err != nil {
			return result, err
		}
		if _, commitErr := adapter.commitMySQLDeleteReceipt(ctx, connection); commitErr != nil {
			active = false
			return capability.classifyMySQLDeleteCommitAmbiguity(ctx, connection, &closed, batch, commitErr)
		}
		active = false
		return stored, nil
	}
	deleteResult, err := connection.ExecContext(ctx, statement, flattenMySQLDeleteArguments(keys)...)
	if err != nil {
		return result, fmt.Errorf("MySQL delete batch failed atomically with no receipt: %w", err)
	}
	deletedRows, err := deleteResult.RowsAffected()
	if err != nil || deletedRows < 0 || deletedRows > int64(len(keys)) {
		return result, fmt.Errorf("MySQL delete batch returned unsafe affected-row count: affected=%d err=%w", deletedRows, err)
	}
	receipt := deleteTargetBatchReceipt{
		PlanID: batch.PlanID, Token: batch.Token, Sequence: batch.Sequence,
		BatchDigest: batch.BatchDigest, Candidates: int64(len(keys)), DeletedRows: deletedRows,
	}
	receipt.ReceiptDigest, err = mysqlDeleteReceiptDigest(receipt, lockedAuthority, targetIdentityDigest, journal)
	if err != nil {
		return result, err
	}
	if err := insertMySQLDeleteReceipt(ctx, connection, receipt, lockedAuthority, targetIdentityDigest, journal); err != nil {
		return result, fmt.Errorf("persist immutable MySQL delete receipt: %w", err)
	}
	if _, commitErr := adapter.commitMySQLDeleteReceipt(ctx, connection); commitErr != nil {
		active = false
		return capability.classifyMySQLDeleteCommitAmbiguity(ctx, connection, &closed, batch, commitErr)
	}
	active = false
	return receipt, nil
}

// classifyMySQLDeleteCommitAmbiguity only treats a commit acknowledgement
// error as completed when a fresh connection can read the exact immutable
// token receipt under the same target, catalog, and journal authority. The
// original pinned connection is discarded before that read so it cannot hold
// an unacknowledged transaction open or leak its named session state.
func (capability *mysqlDeleteTargetCapability) classifyMySQLDeleteCommitAmbiguity(
	ctx context.Context,
	connection *sql.Conn,
	closed *bool,
	batch deleteTargetBatch,
	commitErr error,
) (deleteTargetBatchReceipt, error) {
	if connection == nil || closed == nil {
		return deleteTargetBatchReceipt{}, errors.Join(commitErr, errors.New("MySQL delete commit ambiguity connection is unavailable"))
	}
	discardMySQLConnection(connection)
	closeErr := connection.Close()
	*closed = true
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	verificationCtx, cancel := mysqlDeleteDetachedContext(ctx)
	defer cancel()
	stored, found, verifyErr := capability.loadCommittedMySQLDeleteReceipt(verificationCtx, batch)
	if verifyErr == nil && found && closeErr == nil {
		return stored, nil
	}
	if verifyErr == nil && !found {
		verifyErr = errors.New("exact MySQL delete receipt is absent after commit acknowledgement failure")
	}
	return deleteTargetBatchReceipt{}, errors.Join(
		fmt.Errorf("MySQL delete commit outcome is unknown; resume the existing run with the same pending batch token: %w", commitErr),
		closeErr,
		verifyErr,
	)
}

func (capability *mysqlDeleteTargetCapability) loadCommittedMySQLDeleteReceipt(
	ctx context.Context,
	batch deleteTargetBatch,
) (deleteTargetBatchReceipt, bool, error) {
	if capability == nil || capability.adapter == nil || capability.adapter.database == nil {
		return deleteTargetBatchReceipt{}, false, errors.New("MySQL delete receipt verification target is unavailable")
	}
	adapter := capability.adapter
	if err := engine.VerifyMySQLTargetForFlavor(ctx, adapter.database, adapter.flavor); err != nil {
		return deleteTargetBatchReceipt{}, false, fmt.Errorf("verify MySQL delete receipt recovery target flavor/session: %w", err)
	}
	identity, err := readMySQLDeleteEndpointIdentity(ctx, adapter.database, adapter.flavor)
	if err != nil {
		return deleteTargetBatchReceipt{}, false, err
	}
	canonicalIdentity, err := mysqlDeleteCanonicalTargetIdentity(identity)
	if err != nil || canonicalIdentity != capability.targetIdentity {
		if err == nil {
			err = errors.New("MySQL delete receipt recovery target identity differs from admitted identity")
		}
		return deleteTargetBatchReceipt{}, false, err
	}
	authority, err := inspectMySQLDeleteCatalogAuthority(
		ctx, adapter.database, adapter.flavor, adapter.namespace,
		capability.authority.Table, true, true,
	)
	if err != nil {
		return deleteTargetBatchReceipt{}, false, err
	}
	if !sameMySQLDeleteCatalogAuthority(capability.authority, authority) {
		return deleteTargetBatchReceipt{}, false, errors.New("MySQL delete receipt recovery target catalog authority changed")
	}
	journal, err := inspectMySQLDeleteReceiptJournal(ctx, adapter.database, adapter.flavor, adapter.namespace)
	if err != nil {
		return deleteTargetBatchReceipt{}, false, err
	}
	if !journal.Exists || journal.EmptyPrefix {
		return deleteTargetBatchReceipt{}, false, errors.New("MySQL delete receipt journal is absent after durable readiness")
	}
	stored, found, err := loadMySQLDeleteReceipt(
		ctx,
		adapter.database,
		batch.Token,
		authority,
		mysqlDeleteIdentityDigest(canonicalIdentity),
		journal,
		false,
	)
	if err != nil || !found {
		return stored, found, err
	}
	if err := validateMySQLDeleteReceipt(batch, stored, authority, mysqlDeleteIdentityDigest(canonicalIdentity), journal); err != nil {
		return deleteTargetBatchReceipt{}, false, err
	}
	return stored, true, nil
}
