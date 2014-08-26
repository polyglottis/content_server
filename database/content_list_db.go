// Package database defines the content database.
package database

import (
	"database/sql"

	"github.com/polyglottis/platform/content"
	"github.com/polyglottis/platform/language"
)

func (db *DB) ExtractList() ([]*content.Extract, error) {
	rows, err := db.db.Query("select extractId, extractType, slug from extracts")
	if err != nil {
		return nil, err
	}
	return scanExtractList(rows)
}

func (db *DB) ExtractLanguages() ([]language.Code, error) {
	rows, err := db.db.Query("select distinct(language) from flavors")
	if err != nil {
		return nil, err
	}
	list := make([]language.Code, 0)
	for rows.Next() {
		var code string
		err := rows.Scan(&code)
		if err != nil {
			return nil, err
		}
		list = append(list, language.Code(code))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return list, nil
}

func scanExtractList(rows *sql.Rows) ([]*content.Extract, error) {
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

func (db *DB) ExtractListWithLanguage(lang language.Code) ([]*content.Extract, error) {
	rows, err := db.db.Query(
		"select distinct(e.extractId), e.extractType,e.slug from extracts e, flavors f where "+
			"e.extractId=f.extractId and f.language=?", string(lang))
	if err != nil {
		return nil, err
	}
	return scanExtractList(rows)
}

func (db *DB) ExtractListWithLanguages(langA, langB language.Code) ([]*content.Extract, error) {
	rows, err := db.db.Query(
		"select distinct(e.extractId), e.extractType,e.slug from extracts e, flavors f1, flavors f2 where "+
			"e.extractId=f1.extractId and e.extractId=f2.extractId and f1.language=? and f2.language=?;",
		string(langA), string(langB))
	if err != nil {
		return nil, err
	}
	return scanExtractList(rows)
}
