package database

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/polyglottis/platform/content"
	"github.com/polyglottis/platform/database"
	"github.com/polyglottis/platform/user"
)

func history(tableName string) string {
	return tableName + "_history"
}

func version(fieldName string) string {
	return fieldName + "_version"
}

func versioning(tableName string) database.Columns {
	return database.Columns{{
		Field: "author",
		Type:  "text",
	}, {
		Field: "time",
		Type:  "integer",
	}, {
		Field: version(tableName),
		Type:  "integer",
	}, {
		Field: "editType",
		Type:  "text",
	}}
}

func addVersionedTable(schema database.Schema, table *database.Table) database.Schema {
	return append(schema, table, &database.Table{
		Name:       history(table.Name),
		Columns:    append(table.Columns, versioning(table.Name)...),
		PrimaryKey: append(table.PrimaryKey, version(table.Name)),
	})
}

func (tx *Tx) InsertVersioned(table string, author user.Name, values ...interface{}) error {
	// update main table
	_, err := tx.Exec(fmt.Sprintf("insert into %s values %s", table, database.QM(len(values))), values...)
	if err != nil {
		return err
	}

	// insert history entry
	historyValues := versionedValues(values, author, 0, content.EditNew)
	_, err = tx.Exec(fmt.Sprintf("insert into %s values %s", history(table), database.QM(len(historyValues))), historyValues...)
	return err
}

func versionedValues(values []interface{}, author user.Name, version int, t content.EditType) []interface{} {
	return append(values, string(author), time.Now().Unix(), version, string(t))
}

func (tx *Tx) InsertVersionedFlavor(author user.Name, f *content.Flavor) error {
	extractId := string(f.ExtractId)
	flavorId := int(f.Id)
	err := tx.InsertVersioned("flavors", author, extractId, flavorId, f.Summary, string(f.Type), string(f.Language), f.LanguageComment)
	if err != nil {
		return err
	}

	for bId, block := range f.Blocks {
		for uId, unit := range block {
			unit.BlockId = content.BlockId(bId + 1)
			unit.Id = content.UnitId(uId + 1)
			err = tx.InsertVersioned("units", author, extractId, flavorId, bId+1, uId+1, string(unit.ContentType), unit.Content)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type extractUpdate struct {
	// Order and field names must coincide with DB columns!
	ExtractType string
	Slug        string
	Metadata    []byte
}

type flavorUpdate struct {
	// Order and field names must coincide with DB columns!
	Summary         string
	FlavorType      string
	Language        string
	LanguageComment string
}

type unitUpdate struct {
	// Order and field names must coincide with DB columns!
	ContentType string
	Content     string
}

type primaryKey struct {
	// Order and field names must coincide with DB columns!
	ExtractId string
	FlavorId  int
	BlockId   int
	UnitId    int
}

func (pk *primaryKey) Sql() string {
	list := []string{"extractId", "flavorId", "blockId", "unitId"}
	switch {
	case pk.FlavorId == 0:
		list = list[:1]
	case pk.BlockId == 0 || pk.UnitId == 0:
		list = list[:2]
	}
	return strings.Join(list, "=? and ") + "=?"
}

func (pk *primaryKey) Values() []interface{} {
	list := []interface{}{pk.ExtractId, pk.FlavorId, pk.BlockId, pk.UnitId}
	switch {
	case pk.FlavorId == 0:
		return list[:1]
	case pk.BlockId == 0 || pk.UnitId == 0:
		return list[:2]
	}
	return list
}

func newExtractId(id content.ExtractId) *primaryKey {
	return &primaryKey{
		ExtractId: string(id),
	}
}

func newFlavorId(extractId content.ExtractId, flavorId content.FlavorId) *primaryKey {
	return &primaryKey{
		ExtractId: string(extractId),
		FlavorId:  int(flavorId),
	}
}

func newUnitId(extractId content.ExtractId, flavorId content.FlavorId, blockId content.BlockId, unitId content.UnitId) *primaryKey {
	return &primaryKey{
		ExtractId: string(extractId),
		FlavorId:  int(flavorId),
		BlockId:   int(blockId),
		UnitId:    int(unitId),
	}
}

func (tx *Tx) InsertOrUpdateVersioned(table string, author user.Name, id *primaryKey, kvPairs interface{}) error {
	curVersion, err := tx.LatestVersion(table, id)
	if err != nil {
		return err
	}

	// turn id and kvPairs into sql format
	v := reflect.ValueOf(kvPairs).Elem()
	t := v.Type()
	columns := make([]string, t.NumField())
	values := make([]interface{}, t.NumField(), t.NumField()+4)
	for i := range columns {
		columns[i] = t.Field(i).Name + "=?"
		values[i] = v.Field(i).Interface()
	}
	idValues := id.Values()

	// update main table
	insertValues := append(idValues, values...)
	if curVersion.Number == -1 {
		_, err = tx.Exec(fmt.Sprintf("insert into %s values %s", table, database.QM(len(insertValues))), insertValues...)
		if err != nil {
			return err
		}
	} else {
		updateValues := append(values, idValues...)
		_, err = tx.Exec(fmt.Sprintf("update %s set %s where %s", table, strings.Join(columns, ","), id.Sql()), updateValues...)
		if err != nil {
			return err
		}
	}

	// insert history entry
	editType := content.EditUpdate
	if curVersion.EditType == content.EditDelete {
		editType = content.EditNew
	}
	historyValues := versionedValues(insertValues, author, curVersion.Number+1, editType)
	_, err = tx.Exec(fmt.Sprintf("insert into %s values %s", history(table), database.QM(len(historyValues))), historyValues...)
	return err
}

func (tx *Tx) LatestVersion(table string, id *primaryKey) (*content.Version, error) {
	row := tx.QueryRow(fmt.Sprintf("select max(%s) from %s where %s",
		version(table), history(table), id.Sql()), id.Values()...)
	var v sql.NullInt64
	err := row.Scan(&v)
	if err != nil {
		return nil, err
	}

	if v.Valid {
		return tx.scanVersion(tx.QueryRow(fmt.Sprintf("select author, time, %s, editType from %s where %s and %s=?",
			version(table), history(table), id.Sql(), version(table)), append(id.Values(), v)...))
	} else {
		return &content.Version{
			Number:   -1,
			EditType: content.EditDelete,
		}, nil
	}
}

func (tx *Tx) scanVersion(s scanner) (*content.Version, error) {
	v := new(content.Version)
	var author, editType string
	var date int64
	err := s.Scan(&author, &date, &v.Number, &editType)
	if err != nil {
		return nil, err
	}
	v.Author = user.Name(author)
	v.Time = time.Unix(date, 0)
	v.EditType = content.EditType(editType)
	return v, nil
}
