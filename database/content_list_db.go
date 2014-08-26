// Package database defines the content database.
package database

import (
	"github.com/polyglottis/platform/content"
	"github.com/polyglottis/platform/language"
)

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
