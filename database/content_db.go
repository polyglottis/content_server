// Package database defines the content database.
package database

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

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
			Field: "slug",
			Type:  "text",
		}, {
			Field: "extractType",
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
			Field: "language",
			Type:  "text",
		}, {
			Field: "flavorType",
			Type:  "text",
		}, {
			Field: "flavorId",
			Type:  "integer",
		}, {
			Field: "languageComment",
			Type:  "text",
		}, {
			Field: "summary",
			Type:  "text",
		}},
		PrimaryKey: []string{"extractId", "language", "flavorType", "flavorId"},
	})

	schema = addVersionedTable(schema, &database.Table{
		Name: "units",
		Columns: database.Columns{{
			Field: "extractId",
			Type:  "text",
		}, {
			Field: "language",
			Type:  "text",
		}, {
			Field: "flavorType",
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
		PrimaryKey: []string{"extractId", "language", "flavorType", "flavorId", "blockId", "unitId"},
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
	if !content.ValidExtractType(e.Type) {
		return content.ErrInvalidInput
	}
	if valid, _ := content.ValidSlug(e.UrlSlug); !valid {
		return content.ErrInvalidInput
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

			err = tx.InsertVersioned("extracts", author, strId, e.UrlSlug, string(e.Type), metadata)
			if err != nil {
				tx.Rollback()
				return err
			}
			e.SetId(id)

			for lang, fByType := range e.Flavors {
				for fType, flavors := range fByType {
					if !content.ValidFlavorType(fType) {
						tx.Rollback()
						return content.ErrInvalidInput
					}
					for fIdx, f := range flavors {
						f.SetLanguage(lang)
						f.SetType(fType)
						f.SetId(content.FlavorId(fIdx + 1))
						err = tx.InsertVersionedFlavor(author, f)
						if err != nil {
							tx.Rollback()
							return err
						}
					}
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
	if !content.ValidFlavorType(f.Type) {
		return fmt.Errorf("Invalid flavor type")
	}
	if len(f.Language) == 0 {
		return fmt.Errorf("Missing language field")
	}

	return db.withExtractLock(f.ExtractId, func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		var max sql.NullInt64
		err = tx.QueryRow("select max(flavorId) from flavors where extractId=? and language=? and flavorType=?",
			string(f.ExtractId), string(f.Language), string(f.Type)).Scan(&max)
		if err != nil {
			tx.Rollback()
			return err
		}
		var m int64
		if max.Valid {
			val, err := max.Value()
			if err != nil {
				tx.Rollback()
				return err
			}
			var ok bool
			if m, ok = val.(int64); !ok {
				tx.Rollback()
				return fmt.Errorf("Unable to parse integer value")
			}
		}

		f.SetId(content.FlavorId(m + 1))
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

func (db *DB) FlavorExists(extractId content.ExtractId, lang language.Code, flavorType content.FlavorType, flavorId content.FlavorId) (bool, error) {
	return db.db.QueryNonZero("select count(1) from flavors where extractId=? and language=? and flavorType=? and flavorId=?",
		string(extractId), string(lang), string(flavorType), int(flavorId))
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

	rows, err := db.db.Query("select * from flavors where extractId=? order by extractId, language, flavorType, flavorId", strId)
	if err != nil {
		return nil, err
	}
	e.Flavors, err = db.scanFlavorMap(rows)
	if err != nil {
		return nil, err
	}

	rows, err = db.db.Query("select * from units where extractId=? order by extractId, language, flavorType, flavorId, blockId, unitId", strId)
	if err != nil {
		return nil, err
	}
	units, err := db.scanUnits(rows)
	if err != nil {
		return nil, err
	}

	// Group units and assign them to the right flavor.
	groupedUnits := db.groupSortedUnits(units)
	for _, group := range groupedUnits {
		lang := group[0][0].Language
		if fByType, ok := e.Flavors[lang]; ok {
			fType := group[0][0].FlavorType
			if flavors, ok := fByType[fType]; ok {
				fId := group[0][0].FlavorId
				flavorMap := make(map[content.FlavorId]int)
				for i, f := range flavors {
					flavorMap[f.Id] = i
				}
				if i, ok := flavorMap[fId]; ok {
					flavors[i].Blocks = group
				} else {
					log.Printf("ERROR: Units associated to missing flavor: %s/%s/%s/%d", strId, string(lang), string(fType), fId)
				}
			} else {
				log.Printf("ERROR: Units associated to missing flavor: %s/%s/%s", strId, string(lang), string(fType))
			}
		} else {
			log.Printf("ERROR: Units associated to missing flavor: %s/%s", strId, string(lang))
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
	err := s.Scan(&id, &e.UrlSlug, &eType, &metadata)
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

func (db *DB) scanFlavorMap(rows *sql.Rows) (content.FlavorMap, error) {
	flavors, err := db.scanFlavors(rows)
	if err != nil {
		return nil, err
	}
	m := make(content.FlavorMap)
	for _, f := range flavors {
		if _, ok := m[f.Language]; !ok {
			m[f.Language] = make(content.FlavorByType)
		}
		m[f.Language][f.Type] = append(m[f.Language][f.Type], f)
	}
	return m, nil
}

func (db *DB) scanFlavor(s scanner) (*content.Flavor, error) {
	f := new(content.Flavor)
	var eId, lang, fType string
	var fId int
	err := s.Scan(&eId, &lang, &fType, &fId, &f.LanguageComment, &f.Summary)
	if err != nil {
		return nil, err
	}
	f.ExtractId = content.ExtractId(eId)
	f.Language = language.Code(lang)
	f.Type = content.FlavorType(fType)
	f.Id = content.FlavorId(fId)
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
	var eId, lang, fType, cType string
	var fId, bId, uId int
	err := s.Scan(&eId, &lang, &fType, &fId, &bId, &uId, &cType, &u.Content)
	if err != nil {
		return nil, err
	}
	u.ExtractId = content.ExtractId(eId)
	u.Language = language.Code(lang)
	u.FlavorType = content.FlavorType(fType)
	u.FlavorId = content.FlavorId(fId)
	u.BlockId = content.BlockId(bId)
	u.Id = content.UnitId(uId)
	u.ContentType = content.ContentType(cType)
	return u, nil
}

// groupSortedUnits takes a sorted slice of units (sorted by Language, FlavorType, FlavorId, BlockId, UnitId) for one extract,
// and returns the same units grouped by Language, FlavorType, FlavorId and Block
func (db *DB) groupSortedUnits(units []*content.Unit) []content.BlockSlice {
	groups := make([]content.BlockSlice, 0)
	var lastLanguage language.Code
	var lastFlavorType content.FlavorType
	lastFlavorId := content.FlavorId(-1)
	lastBlockId := content.BlockId(-1)
	for _, u := range units {
		if u.Language != lastLanguage || u.FlavorType != lastFlavorType || u.FlavorId != lastFlavorId {
			groups = append(groups, make(content.BlockSlice, 0))
			lastLanguage = u.Language
			lastFlavorType = u.FlavorType
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
		slug = strings.ToLower(slug)
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
