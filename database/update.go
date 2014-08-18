package database

import (
	"encoding/json"

	"github.com/polyglottis/platform/content"
	"github.com/polyglottis/platform/user"
)

func (db *DB) UpdateExtract(author user.Name, e *content.Extract) error {
	if !content.ValidExtractType(e.Type) {
		return content.ErrInvalidInput
	}

	metadata, err := json.Marshal(e.Metadata)
	if err != nil {
		return err
	}

	return db.withExtractLock(e.Id, func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		// get current slug, to make sure it stays the same as before
		var slug string
		err = tx.QueryRow("select slug from extracts where extractId=?", string(e.Id)).Scan(&slug)
		if err != nil {
			tx.Rollback()
			return err
		}

		err = tx.InsertOrUpdateVersioned("extracts", author, newExtractId(e.Id), &extractUpdate{
			Slug:        slug,
			ExtractType: string(e.Type),
			Metadata:    metadata,
		})
		if err != nil {
			tx.Rollback()
			return err
		}

		return tx.Commit()
	})
}

func (db *DB) UpdateFlavor(author user.Name, f *content.Flavor) error {
	return db.withFlavorLock(f.ExtractId, f.Language, f.Type, f.Id, func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		err = tx.InsertOrUpdateVersioned("flavors", author, newFlavorId(f.ExtractId, f.Language, f.Type, f.Id), &flavorUpdate{
			LanguageComment: f.LanguageComment,
			Summary:         f.Summary,
		})
		if err != nil {
			tx.Rollback()
			return err
		}

		return tx.Commit()
	})
}

func (db *DB) InsertOrUpdateUnits(author user.Name, units []*content.Unit) error {
	if len(units) == 0 {
		return nil
	}

	extractId := units[0].ExtractId
	lang := units[0].Language
	flavorType := units[0].FlavorType
	flavorId := units[0].FlavorId
	if len(lang) == 0 || !content.ValidFlavorType(flavorType) {
		return content.ErrInvalidInput
	}
	for _, u := range units {
		if u.ExtractId != extractId || u.Language != lang || u.FlavorType != flavorType || u.FlavorId != flavorId ||
			u.BlockId <= 0 || u.Id <= 0 ||
			(u.BlockId == 1 && u.Id != 1) { // block 1 can have at most one unit (title block)
			return content.ErrInvalidInput
		}
	}

	return db.withFlavorLock(extractId, lang, flavorType, flavorId, func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		for _, u := range units {
			err := tx.InsertOrUpdateVersioned("units", author, newUnitId(extractId, lang, flavorType, flavorId, u.BlockId, u.Id), &unitUpdate{
				ContentType: string(u.ContentType),
				Content:     u.Content,
			})
			if err != nil {
				tx.Rollback()
				return err
			}
		}
		return tx.Commit()
	})
}
