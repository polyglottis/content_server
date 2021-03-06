package database

import (
	"sync"

	"github.com/polyglottis/platform/content"
	"github.com/polyglottis/platform/language"
)

type lock struct {
	*sync.Mutex
}

func newLock() *lock {
	return &lock{
		Mutex: new(sync.Mutex),
	}
}

func (db *DB) withExtractLock(id content.ExtractId, todo func() error) error {
	exists, err := db.ExtractExists(id)
	if err != nil {
		return err
	} else if !exists {
		return content.ErrNotFound
	}
	return db.withExtractLock_NoCheck(id, todo)
}

func (db *DB) withExtractLock_NoCheck(id content.ExtractId, todo func() error) error {
	db.extractLock.Lock()
	defer db.extractLock.Unlock()
	return todo()
}

func (db *DB) withFlavorLock(extractId content.ExtractId, lang language.Code, flavorType content.FlavorType, flavorId content.FlavorId, todo func() error) error {
	exists, err := db.FlavorExists(extractId, lang, flavorType, flavorId)
	if err != nil {
		return err
	} else if !exists {
		return content.ErrNotFound
	}
	return db.withExtractLock_NoCheck(extractId, todo)
}
