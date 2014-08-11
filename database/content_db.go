// Package database defines the content database.
package database

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3" // driver import

	"github.com/polyglottis/platform/content"
	"github.com/polyglottis/platform/database"
	"github.com/polyglottis/platform/language"
	"github.com/polyglottis/platform/user"
	"github.com/polyglottis/rand"
)

var ExtractNotFound = errors.New("Extract not found")

var extractIdLen = 8

type DB struct {
	db          *database.DB
	extractLock *lock
}

type Tx struct {
	*database.Tx
}

func Open(file string) (*DB, error) {
	db, err := sql.Open("sqlite3", file)
	if err != nil {
		return nil, err
	}

	schema := database.Schema{}

	schema = addVersionedTable(schema, &database.Table{
		Name: "extracts",
		Columns: database.Columns{{
			Field:      "extractId",
			Type:       "text",
			Constraint: "not null",
		}, {
			Field: "extractType",
			Type:  "text",
		}, {
			Field: "slug",
			Type:  "text",
		}, {
			Field: "metadata",
			Type:  "text",
		}},
		PrimaryKey: []string{"extractId"},
	})

	schema = addVersionedTable(schema, &database.Table{
		Name: "flavors",
		Columns: database.Columns{{
			Field: "extractId",
			Type:  "text",
		}, {
			Field: "flavorId",
			Type:  "integer",
		}, {
			Field: "summary",
			Type:  "text",
		}, {
			Field: "flavorType",
			Type:  "text",
		}, {
			Field: "language",
			Type:  "text",
		}, {
			Field: "languageComment",
			Type:  "text",
		}},
		PrimaryKey: []string{"extractId", "flavorId"},
	})

	schema = addVersionedTable(schema, &database.Table{
		Name: "units",
		Columns: database.Columns{{
			Field: "extractId",
			Type:  "text",
		}, {
			Field: "flavorId",
			Type:  "integer",
		}, {
			Field: "blockId",
			Type:  "integer",
		}, {
			Field: "unitId",
			Type:  "integer",
		}, {
			Field: "contentType",
			Type:  "text",
		}, {
			Field: "content",
			Type:  "text",
		}},
		PrimaryKey: []string{"extractId", "flavorId", "blockId", "unitId"},
	})

	contentDB, err := database.Create(db, schema)
	if err != nil {
		return nil, err
	}

	return &DB{
		db:          contentDB,
		extractLock: newLock(),
	}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Begin() (*Tx, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{tx}, nil
}

func (db *DB) NewExtract(author user.Name, e *content.Extract) error {
	if e == nil {
		return fmt.Errorf("New Extract should not be nil")
	}
	if len(author) == 0 {
		return fmt.Errorf("Author name cannot be empty")
	}

	metadata, err := json.Marshal(e.Metadata)
	if err != nil {
		return err
	}

	var id content.ExtractId
	for i := 0; i < 10; i++ {
		var strId string
		strId, err = rand.Id(extractIdLen)
		if err != nil {
			continue
		}
		id = content.ExtractId(strId)

		err = db.withExtractLock_NoCheck(id, func() error {
			exists, err := db.ExtractHasExisted(id)
			if err != nil || exists {
				return err
			}

			tx, err := db.Begin()
			if err != nil {
				return err
			}

			err = tx.InsertVersioned("extracts", author, strId, string(e.Type), e.UrlSlug, metadata)
			if err != nil {
				tx.Rollback()
				return err
			}
			e.SetId(id)

			for fIdx, f := range e.Flavors {
				f.SetId(content.FlavorId(fIdx + 1))
				err = tx.InsertVersionedFlavor(author, f)
				if err != nil {
					tx.Rollback()
					return err
				}
			}
			return tx.Commit()
		})
		if err == nil {
			break
		}
	}

	return err
}

func (db *DB) NewFlavor(author user.Name, f *content.Flavor) error {
	if f == nil {
		return fmt.Errorf("New Flavor should not be nil")
	}
	if len(author) == 0 {
		return fmt.Errorf("Author name cannot be empty")
	}

	return db.withExtractLock(f.ExtractId, func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		max, err := tx.QueryInt("select max(flavorId) from flavors where extractId=?", string(f.ExtractId))
		if err != nil {
			tx.Rollback()
			return err
		}

		f.SetId(content.FlavorId(max + 1))
		err = tx.InsertVersionedFlavor(author, f)
		if err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

func (db *DB) ExtractHasExisted(id content.ExtractId) (bool, error) {
	return db.db.QueryNonZero("select count(1) from extracts_history where extractId=?", string(id))
}

func (db *DB) ExtractExists(id content.ExtractId) (bool, error) {
	return db.db.QueryNonZero("select count(1) from extracts where extractId=?", string(id))
}

func (db *DB) FlavorExists(extractId content.ExtractId, flavorId content.FlavorId) (bool, error) {
	return db.db.QueryNonZero("select count(1) from flavors where extractId=? and flavorId=?", string(extractId), int(flavorId))
}

func (db *DB) GetExtract(id content.ExtractId) (*content.Extract, error) {
	strId := string(id)
	e, err := db.scanExtract(db.db.QueryRow("select * from extracts where extractId=?", strId))
	switch {
	case err == sql.ErrNoRows:
		return nil, content.ErrNotFound
	case err != nil:
		return nil, err
	default:
	}

	rows, err := db.db.Query("select * from flavors where extractId=? order by extractId, flavorId", strId)
	if err != nil {
		return nil, err
	}
	e.Flavors, err = db.scanFlavors(rows)
	if err != nil {
		return nil, err
	}

	rows, err = db.db.Query("select * from units where extractId=? order by extractId, flavorId, blockId, unitId", strId)
	if err != nil {
		return nil, err
	}
	units, err := db.scanUnits(rows)
	if err != nil {
		return nil, err
	}

	// Group units and assign them to the right flavor.
	groupedUnits := db.groupSortedUnits(units)
	flavorMap := make(map[content.FlavorId]int)
	for i, f := range e.Flavors {
		flavorMap[f.Id] = i
	}
	for _, group := range groupedUnits {
		fId := group[0][0].FlavorId
		if i, ok := flavorMap[fId]; ok {
			e.Flavors[i].Blocks = group
		} else {
			log.Printf("ERROR: Units associated to missing flavor: %s/%d", strId, fId)
		}
	}

	return e, nil
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func (db *DB) scanExtract(s scanner) (*content.Extract, error) {
	e := new(content.Extract)
	var id, eType string
	var metadata []byte
	err := s.Scan(&id, &eType, &e.UrlSlug, &metadata)
	if err != nil {
		return nil, err
	}
	e.Id = content.ExtractId(id)
	e.Type = content.ExtractType(eType)
	err = json.Unmarshal(metadata, &e.Metadata)
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (db *DB) scanFlavors(rows *sql.Rows) ([]*content.Flavor, error) {
	flavors := make([]*content.Flavor, 0)
	for rows.Next() {
		f, err := db.scanFlavor(rows)
		if err != nil {
			return nil, err
		}
		flavors = append(flavors, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return flavors, nil
}

func (db *DB) scanFlavor(s scanner) (*content.Flavor, error) {
	f := new(content.Flavor)
	var eId, fType, lang string
	var fId int
	err := s.Scan(&eId, &fId, &f.Summary, &fType, &lang, &f.LanguageComment)
	if err != nil {
		return nil, err
	}
	f.ExtractId = content.ExtractId(eId)
	f.Id = content.FlavorId(fId)
	f.Type = content.FlavorType(fType)
	f.Language = language.Code(lang)
	return f, nil
}

func (db *DB) scanUnits(rows *sql.Rows) ([]*content.Unit, error) {
	units := make([]*content.Unit, 0)
	for rows.Next() {
		u, err := db.scanUnit(rows)
		if err != nil {
			return nil, err
		}
		units = append(units, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return units, nil
}

func (db *DB) scanUnit(s scanner) (*content.Unit, error) {
	u := new(content.Unit)
	var eId, cType string
	var fId, bId, uId int
	err := s.Scan(&eId, &fId, &bId, &uId, &cType, &u.Content)
	if err != nil {
		return nil, err
	}
	u.ExtractId = content.ExtractId(eId)
	u.FlavorId = content.FlavorId(fId)
	u.BlockId = content.BlockId(bId)
	u.Id = content.UnitId(uId)
	u.ContentType = content.ContentType(cType)
	return u, nil
}

// groupSortedUnits takes a sorted slice of units (sorted by FlavorId, BlockId, UnitId) for one extract,
// and returns the same units grouped by Flavor and Block
func (db *DB) groupSortedUnits(units []*content.Unit) []content.BlockSlice {
	groups := make([]content.BlockSlice, 0)
	lastFlavorId := content.FlavorId(-1)
	lastBlockId := content.BlockId(-1)
	for _, u := range units {
		if u.FlavorId != lastFlavorId {
			groups = append(groups, make(content.BlockSlice, 0))
			lastFlavorId = u.FlavorId
		}
		flavorIdx := len(groups) - 1
		if u.BlockId != lastBlockId {
			groups[flavorIdx] = append(groups[flavorIdx], make(content.UnitSlice, 0))
			lastBlockId = u.BlockId
		}
		blockIdx := len(groups[flavorIdx]) - 1
		groups[flavorIdx][blockIdx] = append(groups[flavorIdx][blockIdx], u)
	}
	return groups
}

func (db *DB) SlugToIdMap() (map[string]content.ExtractId, error) {
	rows, err := db.db.Query("select extractId, slug from extracts")
	if err != nil {
		return nil, err
	}
	m := make(map[string]content.ExtractId)
	for rows.Next() {
		var id, slug string
		err := rows.Scan(&id, &slug)
		if err != nil {
			return nil, err
		}
		if otherId, wasThere := m[slug]; wasThere {
			log.Println("Error: slug used by both %v and %v", id, otherId)
		}
		m[slug] = content.ExtractId(id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return m, nil
}

func (db *DB) ExtractList() ([]*content.Extract, error) {
	rows, err := db.db.Query("select extractId, extractType, slug from extracts")
	if err != nil {
		return nil, err
	}
	list := make([]*content.Extract, 0)
	for rows.Next() {
		var id, eType, slug string
		err := rows.Scan(&id, &eType, &slug)
		if err != nil {
			return nil, err
		}
		list = append(list, &content.Extract{
			Id:      content.ExtractId(id),
			Type:    content.ExtractType(eType),
			UrlSlug: slug,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return list, nil
}
