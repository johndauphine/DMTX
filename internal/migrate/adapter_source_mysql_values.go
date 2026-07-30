package migrate

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

// mysqlTemporalRows validates the raw text selected by mySQLReadProjection,
// then restores the time.Time representation used by the other source
// adapters. It rejects zero, partial-zero, and malformed values without
// depending on or naming a target engine.
type mysqlTemporalRows struct {
	adapterRows
	columns []mysqlTemporalColumn
}

type mysqlTemporalColumn struct {
	index     int
	name      string
	typ       string
	precision int
}

func wrapMySQLSourceRows(
	rows adapterRows,
	table schema.Table,
	columns []string,
) adapterRows {
	metadata := make(map[string]schema.Column, len(table.Columns))
	for _, column := range table.Columns {
		metadata[column.Name] = column
	}
	temporals := make([]mysqlTemporalColumn, 0)
	for index, name := range columns {
		column, ok := metadata[name]
		if !ok || !isMySQLTemporalType(column.Type) {
			continue
		}
		precision := 0
		if column.Type == "time" {
			precision = mysqlSourceTimePrecision(column)
		}
		temporals = append(temporals, mysqlTemporalColumn{
			index:     index,
			name:      name,
			typ:       column.Type,
			precision: precision,
		})
	}
	if len(temporals) == 0 {
		return rows
	}
	return &mysqlTemporalRows{
		adapterRows: rows,
		columns:     temporals,
	}
}

func (rows *mysqlTemporalRows) Scan(destinations ...any) error {
	if err := rows.adapterRows.Scan(destinations...); err != nil {
		return err
	}
	for _, column := range rows.columns {
		if column.index >= len(destinations) {
			return fmt.Errorf(
				"validate MySQL temporal column %s: scan destination is missing",
				column.name,
			)
		}
		destination, ok := destinations[column.index].(*any)
		if !ok {
			return fmt.Errorf(
				"validate MySQL temporal column %s: unsupported scan destination",
				column.name,
			)
		}
		normalized, ok := normalizeMySQLTemporal(
			column.typ,
			column.precision,
			*destination,
		)
		if !ok {
			return invalidMySQLTemporal(column)
		}
		*destination = normalized
	}
	return nil
}

func normalizeMySQLTemporal(
	columnType string,
	precision int,
	value any,
) (any, bool) {
	if value == nil {
		return nil, true
	}
	if columnType == "time" {
		return normalizeMySQLTime(precision, value)
	}
	if temporal, ok := value.(time.Time); ok {
		if temporal.IsZero() {
			return nil, false
		}
		return temporal, true
	}

	var text string
	switch temporal := value.(type) {
	case string:
		if !utf8.ValidString(temporal) {
			return nil, false
		}
		text = temporal
	case []byte:
		if !utf8.Valid(temporal) {
			return nil, false
		}
		text = string(temporal)
	default:
		return nil, false
	}
	return parseMySQLTemporal(columnType, text)
}

func normalizeMySQLTime(
	precision int,
	value any,
) (any, bool) {
	var text string
	switch temporal := value.(type) {
	case string:
		if !utf8.ValidString(temporal) {
			return nil, false
		}
		text = temporal
	case []byte:
		if !utf8.Valid(temporal) {
			return nil, false
		}
		text = string(temporal)
	default:
		return nil, false
	}
	if !validMySQLClockTime(text, precision) {
		return nil, false
	}
	return text, true
}

func validMySQLClockTime(value string, precision int) bool {
	if precision < 0 || precision > 6 {
		return false
	}
	clock := value
	if precision == 0 {
		if len(value) != len("00:00:00") {
			return false
		}
	} else {
		fractionOffset := len("00:00:00")
		if len(value) != fractionOffset+1+precision ||
			value[fractionOffset] != '.' ||
			!isMySQLDigits(value[fractionOffset+1:]) {
			return false
		}
		clock = value[:fractionOffset]
	}
	if clock[2] != ':' || clock[5] != ':' {
		return false
	}
	for _, index := range []int{0, 1, 3, 4, 6, 7} {
		if clock[index] < '0' || clock[index] > '9' {
			return false
		}
	}
	hour := int(clock[0]-'0')*10 + int(clock[1]-'0')
	minute := int(clock[3]-'0')*10 + int(clock[4]-'0')
	second := int(clock[6]-'0')*10 + int(clock[7]-'0')
	return hour <= 23 && minute <= 59 && second <= 59
}

func mysqlSourceTimePrecision(column schema.Column) int {
	if column.DeclaredType == nil ||
		column.DeclaredType.Base != "time" ||
		len(column.DeclaredType.Arguments) != 1 {
		return -1
	}
	precision := column.DeclaredType.Arguments[0]
	if precision < 0 || precision > 6 {
		return -1
	}
	return precision
}

func parseMySQLTemporal(
	columnType string,
	value string,
) (time.Time, bool) {
	switch columnType {
	case "date":
		if !validMySQLDateShape(value) {
			return time.Time{}, false
		}
		parsed, err := time.ParseInLocation(
			"2006-01-02",
			value,
			time.UTC,
		)
		if err != nil || parsed.Year() == 0 {
			return time.Time{}, false
		}
		return parsed, true
	case "datetime", "timestamp":
		if len(value) < 19 ||
			!validMySQLDateShape(value[:10]) ||
			value[10] != ' ' ||
			value[13] != ':' ||
			value[16] != ':' {
			return time.Time{}, false
		}
		for _, index := range []int{11, 12, 14, 15, 17, 18} {
			if value[index] < '0' || value[index] > '9' {
				return time.Time{}, false
			}
		}
		layout := "2006-01-02 15:04:05"
		if len(value) > 19 {
			fraction := value[19:]
			if len(fraction) < 2 ||
				len(fraction) > 7 ||
				fraction[0] != '.' ||
				!isMySQLDigits(fraction[1:]) {
				return time.Time{}, false
			}
			layout += "." + strings.Repeat("0", len(fraction)-1)
		}
		parsed, err := time.ParseInLocation(layout, value, time.UTC)
		if err != nil || parsed.Year() == 0 {
			return time.Time{}, false
		}
		return parsed, true
	default:
		return time.Time{}, false
	}
}

func validMySQLDateShape(value string) bool {
	if len(value) != 10 || value[4] != '-' || value[7] != '-' {
		return false
	}
	for _, index := range []int{0, 1, 2, 3, 5, 6, 8, 9} {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func isMySQLDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func invalidMySQLTemporal(column mysqlTemporalColumn) error {
	return fmt.Errorf(
		"MySQL source column %s contains an invalid %s value",
		column.name,
		column.typ,
	)
}

func isMySQLTemporalType(columnType string) bool {
	switch columnType {
	case "date", "time", "datetime", "timestamp":
		return true
	default:
		return false
	}
}
