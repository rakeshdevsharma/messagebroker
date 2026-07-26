package store

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const pgUniqueViolation = "23505"

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

func wrapNotFound(err error) error {
	if isNoRows(err) {
		return ErrNotFound
	}
	return err
}

func wrapAlreadyExists(err error) error {
	if isUniqueViolation(err) {
		return ErrAlreadyExists
	}
	return err
}
