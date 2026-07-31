package migrate

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestStage4SpatialMetadataRouteMatrixLive(t *testing.T) {
	sourceDSN := os.Getenv("DMTX_TEST_MYSQL_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MYSQL_CA")
	if sourceDSN == "" || targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MYSQL_DSN, DMTX_TEST_MYSQL_TARGET_DSN, " +
				"and DMTX_TEST_MYSQL_CA to run the Stage 4 spatial route matrix",
		)
	}
	registerMySQLCommonFixtureTLSNamed(t, caPath, "dmtx_test")
	sourceConfig := parseMySQLNativeTargetDSNForTLS(
		t,
		"source",
		sourceDSN,
		"dmtx_test",
	)
	targetConfig := parseMySQLNativeTargetDSNForTLS(
		t,
		"target",
		targetDSN,
		"dmtx_test",
	)
	if sourceConfig.DBName == targetConfig.DBName &&
		sourceConfig.Addr == targetConfig.Addr {
		t.Fatal("spatial route matrix requires distinct source and target databases")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	sourceDatabase := openMySQLNativeLiveDatabase(
		t,
		ctx,
		"spatial source",
		sourceDSN,
	)
	targetDatabase := openMySQLNativeLiveDatabase(
		t,
		ctx,
		"spatial target",
		targetDSN,
	)
	name := "dmtx_spatial_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupMySQLNativeTables(t, sourceDatabase, name)
	cleanupMySQLNativeTables(t, targetDatabase, name)
	createStage4MySQLSpatialSource(t, ctx, sourceDatabase, name)
	insertStage4MySQLSpatialRow(
		t,
		ctx,
		sourceDatabase,
		name,
		1,
		4326,
	)

	sourceMetadata, err := engine.InspectMySQLTable(
		ctx,
		sourceDatabase,
		sourceConfig.DBName,
		name,
	)
	if err != nil {
		t.Fatalf("inspect MySQL spatial source: %v", err)
	}
	assertStage4MySQLSpatialMetadata(t, sourceMetadata)

	migrationConfig := config.Config{
		Source: mysqlNativeTargetEndpoint(
			t,
			sourceConfig,
			caPath,
		),
		Target: mysqlNativeTargetEndpoint(
			t,
			targetConfig,
			caPath,
		),
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{name},
		},
	}
	result, err := MySQLToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf("migrate MySQL spatial table: %v", err)
	}
	if result.Tables != 1 ||
		result.Rows != 1 ||
		!result.Validated {
		t.Fatalf("spatial rebuild result = %+v", result)
	}
	targetMetadata, err := engine.InspectMySQLTable(
		ctx,
		targetDatabase,
		targetConfig.DBName,
		name,
	)
	if err != nil {
		t.Fatalf("reinspect MySQL spatial target: %v", err)
	}
	assertStage4MySQLSpatialMetadata(t, targetMetadata)
	if !reflect.DeepEqual(
		sourceMetadata.Columns,
		targetMetadata.Columns,
	) {
		t.Fatalf(
			"spatial catalog round-trip differs:\nsource=%#v\ntarget=%#v",
			sourceMetadata.Columns,
			targetMetadata.Columns,
		)
	}
	assertStage4MySQLSpatialRowsEqual(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		name,
	)

	if _, err := sourceDatabase.ExecContext(
		ctx,
		"UPDATE "+mySQLIdentifier(name)+
			" SET "+mySQLIdentifier("any_shape")+
			" = ST_GeomFromText(?), "+
			mySQLIdentifier("position")+
			" = ST_GeomFromText(?, 4326) WHERE "+
			mySQLIdentifier("id")+" = 1",
		"LINESTRING(2 2,3 3)",
		"POINT(7 8)",
	); err != nil {
		t.Fatalf("update MySQL spatial source: %v", err)
	}
	migrationConfig.Migration.TargetMode = "upsert"
	upserted, err := MySQLToMySQLWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf("upsert MySQL spatial table: %v", err)
	}
	if upserted.Tables != 1 ||
		upserted.Rows != 1 ||
		!upserted.Validated {
		t.Fatalf("spatial upsert result = %+v", upserted)
	}
	assertStage4MySQLSpatialRowsEqual(
		t,
		ctx,
		sourceDatabase,
		targetDatabase,
		name,
	)

	if _, err := targetDatabase.ExecContext(
		ctx,
		"DELETE FROM "+mySQLIdentifier(name),
	); err != nil {
		t.Fatalf("clear MySQL spatial target before catalog tamper: %v", err)
	}
	if _, err := targetDatabase.ExecContext(
		ctx,
		"ALTER TABLE "+mySQLIdentifier(name)+" MODIFY COLUMN "+
			mySQLIdentifier("position")+" POINT SRID 0 NOT NULL",
	); err != nil {
		t.Fatalf("tamper MySQL spatial target SRID: %v", err)
	}
	insertStage4MySQLSpatialRow(
		t,
		ctx,
		targetDatabase,
		name,
		99,
		0,
	)
	tampered, err := engine.InspectMySQLTable(
		ctx,
		targetDatabase,
		targetConfig.DBName,
		name,
	)
	if err != nil {
		t.Fatalf("reinspect tampered MySQL spatial target: %v", err)
	}
	position := stage4MySQLSpatialColumn(t, tampered, "position")
	if position.DeclaredType.Spatial.SRID == nil ||
		*position.DeclaredType.Spatial.SRID != 0 {
		t.Fatalf(
			"tampered target SRID = %#v, want explicit zero",
			position.DeclaredType.Spatial.SRID,
		)
	}

	observer := &mysqlNativePreflightObserver{}
	rejected, err := MySQLToMySQLWithObserver(
		ctx,
		migrationConfig,
		observer,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"retained target shape differs from the plan",
		) ||
		!strings.Contains(err.Error(), "declared type differs") {
		t.Fatalf(
			"tampered spatial rerun result = %+v, error = %v",
			rejected,
			err,
		)
	}
	assertMySQLNativePreflightDidNotMutate(t, rejected, observer)
	var rows int
	var retainedID int64
	if err := targetDatabase.QueryRowContext(
		ctx,
		"SELECT COUNT(*), MIN("+mySQLIdentifier("id")+
			") FROM "+mySQLIdentifier(name),
	).Scan(&rows, &retainedID); err != nil {
		t.Fatalf("read MySQL spatial tamper sentinel: %v", err)
	}
	if rows != 1 || retainedID != 99 {
		t.Fatalf(
			"spatial tamper sentinel rows = %d, id = %d",
			rows,
			retainedID,
		)
	}
}

func createStage4MySQLSpatialSource(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	name string,
) {
	t.Helper()
	statement := "CREATE TABLE " + mySQLIdentifier(name) + " (" +
		mySQLIdentifier("id") + " BIGINT NOT NULL, " +
		mySQLIdentifier("any_shape") + " GEOMETRY NOT NULL, " +
		mySQLIdentifier("position") + " POINT SRID 4326 NOT NULL, " +
		mySQLIdentifier("path") + " LINESTRING SRID 0 NOT NULL, " +
		mySQLIdentifier("area") + " POLYGON NOT NULL, " +
		mySQLIdentifier("points") + " MULTIPOINT NOT NULL, " +
		mySQLIdentifier("paths") + " MULTILINESTRING NOT NULL, " +
		mySQLIdentifier("areas") + " MULTIPOLYGON NOT NULL, " +
		mySQLIdentifier("collection") + " GEOMETRYCOLLECTION NOT NULL, " +
		"PRIMARY KEY (" + mySQLIdentifier("id") + ")) " +
		"ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 " +
		"COLLATE=utf8mb4_0900_bin ROW_FORMAT=DYNAMIC"
	if _, err := database.ExecContext(ctx, statement); err != nil {
		t.Fatalf("create MySQL spatial source: %v", err)
	}
}

func insertStage4MySQLSpatialRow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	name string,
	id int64,
	positionSRID int,
) {
	t.Helper()
	columns := []string{
		"id",
		"any_shape",
		"position",
		"path",
		"area",
		"points",
		"paths",
		"areas",
		"collection",
	}
	quoted := make([]string, len(columns))
	for index, column := range columns {
		quoted[index] = mySQLIdentifier(column)
	}
	statement := "INSERT INTO " + mySQLIdentifier(name) +
		" (" + strings.Join(quoted, ", ") + ") VALUES (" +
		"?, ST_GeomFromText(?), ST_GeomFromText(?, ?), " +
		"ST_GeomFromText(?, 0), ST_GeomFromText(?), " +
		"ST_GeomFromText(?), ST_GeomFromText(?), " +
		"ST_GeomFromText(?), ST_GeomFromText(?))"
	if _, err := database.ExecContext(
		ctx,
		statement,
		id,
		"LINESTRING(0 0,1 1)",
		"POINT(5 6)",
		positionSRID,
		"LINESTRING(0 0,2 2)",
		"POLYGON((0 0,0 2,2 2,2 0,0 0))",
		"MULTIPOINT((1 1),(2 2))",
		"MULTILINESTRING((0 0,1 1),(2 2,3 3))",
		"MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))",
		"GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))",
	); err != nil {
		t.Fatalf("insert MySQL spatial row: %v", err)
	}
}

func assertStage4MySQLSpatialMetadata(
	t *testing.T,
	table schema.Table,
) {
	t.Helper()
	expected := []struct {
		name    string
		subtype schema.SpatialSubtype
		srid    *uint32
	}{
		{name: "any_shape", subtype: schema.SpatialSubtypeGeometry},
		{
			name:    "position",
			subtype: schema.SpatialSubtypePoint,
			srid:    stage4SpatialSRIDPointer(4326),
		},
		{
			name:    "path",
			subtype: schema.SpatialSubtypeLineString,
			srid:    stage4SpatialSRIDPointer(0),
		},
		{name: "area", subtype: schema.SpatialSubtypePolygon},
		{name: "points", subtype: schema.SpatialSubtypeMultiPoint},
		{name: "paths", subtype: schema.SpatialSubtypeMultiLineString},
		{name: "areas", subtype: schema.SpatialSubtypeMultiPolygon},
		{
			name:    "collection",
			subtype: schema.SpatialSubtypeGeometryCollection,
		},
	}
	if len(table.Columns) != len(expected)+1 ||
		!table.Columns[0].PrimaryKey ||
		table.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf("MySQL spatial columns = %#v", table.Columns)
	}
	for _, want := range expected {
		column := stage4MySQLSpatialColumn(t, table, want.name)
		spatial := column.DeclaredType.Spatial
		if column.Type != string(want.subtype) ||
			column.DeclaredType.Base != mySQLSpatialCatalogBase(
				want.subtype,
			) ||
			spatial.Subtype != want.subtype ||
			!stage4SpatialSRIDsEqual(spatial.SRID, want.srid) {
			t.Fatalf(
				"MySQL spatial column %s = %#v",
				want.name,
				column,
			)
		}
	}
}

func stage4MySQLSpatialColumn(
	t *testing.T,
	table schema.Table,
	name string,
) schema.Column {
	t.Helper()
	for _, column := range table.Columns {
		if column.Name == name {
			if column.DeclaredType == nil ||
				column.DeclaredType.Spatial == nil {
				t.Fatalf("column %s lacks spatial metadata", name)
			}
			return column
		}
	}
	t.Fatalf("spatial column %s is absent", name)
	return schema.Column{}
}

func stage4SpatialSRIDPointer(value uint32) *uint32 {
	return &value
}

func stage4SpatialSRIDsEqual(left, right *uint32) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

type stage4MySQLSpatialRow struct {
	id     int64
	values []string
}

func assertStage4MySQLSpatialRowsEqual(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	name string,
) {
	t.Helper()
	sourceRows := readStage4MySQLSpatialRows(t, ctx, source, name)
	targetRows := readStage4MySQLSpatialRows(t, ctx, target, name)
	if !reflect.DeepEqual(sourceRows, targetRows) {
		t.Fatalf(
			"MySQL spatial rows differ:\nsource=%#v\ntarget=%#v",
			sourceRows,
			targetRows,
		)
	}
}

func readStage4MySQLSpatialRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	name string,
) []stage4MySQLSpatialRow {
	t.Helper()
	columns := []string{
		"any_shape",
		"position",
		"path",
		"area",
		"points",
		"paths",
		"areas",
		"collection",
	}
	projection := make([]string, len(columns))
	for index, column := range columns {
		quoted := mySQLIdentifier(column)
		projection[index] = "CONCAT(HEX(" + quoted +
			"), ':', ST_SRID(" + quoted +
			"), ':', ST_GeometryType(" + quoted + "))"
	}
	rows, err := database.QueryContext(
		ctx,
		"SELECT "+mySQLIdentifier("id")+", "+
			strings.Join(projection, ", ")+" FROM "+
			mySQLIdentifier(name)+" ORDER BY "+
			mySQLIdentifier("id"),
	)
	if err != nil {
		t.Fatalf("read MySQL spatial rows: %v", err)
	}
	defer rows.Close()
	result := make([]stage4MySQLSpatialRow, 0)
	for rows.Next() {
		row := stage4MySQLSpatialRow{
			values: make([]string, len(columns)),
		}
		destinations := make([]any, len(columns)+1)
		destinations[0] = &row.id
		for index := range row.values {
			destinations[index+1] = &row.values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			t.Fatalf("scan MySQL spatial row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate MySQL spatial rows: %v", err)
	}
	return result
}
