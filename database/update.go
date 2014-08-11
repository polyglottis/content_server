package database

import (
	"encoding/json"

	"github.com/polyglottis/platform/content"
	"github.com/polyglottis/platform/user"
)

func (db *DB) UpdateExtract(author user.Name, e *content.Extract) error {
	metadata, err := json.Marshal(e.Metadata)
	if err != nil {
		return err
	}

	return db.withExtractLock(e.Id, func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		err = tx.InsertOrUpdateVersioned("extracts", author, newExtractId(e.Id), &extractUpdate{
			ExtractType: string(e.Type),
			Slug:        e.UrlSlug,
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
	return db.withFlavorLock(f.ExtractId, f.Id, func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		err = tx.InsertOrUpdateVersioned("flavors", author, newFlavorId(f.ExtractId, f.Id), &flavorUpdate{
			Summary:         f.Summary,
			FlavorType:      string(f.Type),
			Language:        string(f.Language),
			LanguageComment: f.LanguageComment,
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
	flavorId := units[0].FlavorId
	for _, u := range units {
		if u.ExtractId != extractId || u.FlavorId != flavorId || u.BlockId <= 0 || u.Id <= 0 ||
			(u.BlockId == 1 && u.Id != 1) { // block 1 can have at most one unit (title block)
			return content.ErrInvalidInput
		}
	}

	return db.withFlavorLock(extractId, flavorId, func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		for _, u := range units {
			err := tx.InsertOrUpdateVersioned("units", author, newUnitId(extractId, flavorId, u.BlockId, u.Id), &unitUpdate{
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
