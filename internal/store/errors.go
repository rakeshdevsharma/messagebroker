package store

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func wrapNotFound(err error) error {
	if isNoRows(err) {
		return ErrNotFound
	}
	return err
}
