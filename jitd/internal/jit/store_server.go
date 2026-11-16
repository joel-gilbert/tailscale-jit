package jit

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"
)

// openServer opens a Postgres or MySQL database named by a URL. Unless
// readOnly, it creates jitd's tables in an empty database and takes the
// writer lease, waiting up to leaseTTL for a jitd that stopped without
// releasing it.
func openServer(d *dialect, target string, readOnly bool) (*Store, error) {
	driver, dsn, err := driverDSN(d, target)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	s := &Store{db: db, dia: d, Path: RedactURL(target)}
	fail := func(err error) (*Store, error) {
		db.Close()
		return nil, fmt.Errorf("%s: %w", s.Path, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return fail(err)
	}
	if readOnly {
		err = s.checkServerVersion(ctx)
	} else {
		err = s.migrateServer(ctx)
		if err == nil {
			err = s.acquireLease()
		}
	}
	if err == nil {
		err = s.load()
	}
	if err != nil {
		if s.holder != "" {
			s.releaseLease()
		}
		return fail(err)
	}
	if !readOnly {
		s.lost = make(chan error, 1)
		s.stopWatch = make(chan struct{})
		go s.renewLease(s.stopWatch)
	}
	return s, nil
}

// serverVersion reads the schema version, or 0 for a database without
// jitd's tables.
func (s *Store) serverVersion(ctx context.Context) (int, error) {
	ok, err := s.dia.hasTable(ctx, s.db, "jitd_schema")
	if err != nil || !ok {
		return 0, err
	}
	var v int
	err = s.db.QueryRowContext(ctx, "SELECT version FROM jitd_schema").Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("jitd_schema is empty: the tables were not completely created; drop them and start jitd again")
	}
	return v, err
}

func (s *Store) checkServerVersion(ctx context.Context) error {
	v, err := s.serverVersion(ctx)
	if err == nil && v != schemaVersion {
		err = fmt.Errorf("the database is at schema %d; this jitd needs %d (start jitd once to upgrade it)", v, schemaVersion)
	}
	return err
}

// migrateServer creates jitd's tables in an empty database, or checks the
// version of existing ones. Server databases start at the current schema;
// later schema changes add steps here, as migrate does for SQLite.
func (s *Store) migrateServer(ctx context.Context) error {
	v, err := s.serverVersion(ctx)
	if err != nil {
		return err
	}
	switch {
	case v == schemaVersion:
		return nil
	case v > schemaVersion:
		return fmt.Errorf("the database was written by a newer jitd (schema %d; this jitd knows up to %d): run the newer jitd, or restore a backup", v, schemaVersion)
	case v > 0:
		return fmt.Errorf("unknown schema %d", v)
	}
	for _, t := range []string{"workflows", "requests", "settings", "jitd_lock"} {
		ok, err := s.dia.hasTable(ctx, s.db, t)
		if err != nil {
			return err
		}
		if ok {
			return fmt.Errorf("this is not a jitd database: it has a %s table but no jitd_schema; give jitd a database of its own", t)
		}
	}
	// Postgres creates the tables in one transaction. MySQL commits each
	// CREATE TABLE on its own, so a failure part way leaves some tables
	// behind, without the version row; the error above then says so.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, s.dia.schema); err != nil {
		return fmt.Errorf("creating the tables: %w", err)
	}
	if _, err := tx.ExecContext(ctx, s.dia.q("INSERT INTO jitd_schema (version) VALUES (?)"), schemaVersion); err != nil {
		return err
	}
	return tx.Commit()
}

// The writer lease. jitd keeps a working copy of everything in memory, so a
// second jitd writing to the same database would overwrite the first's
// changes. One row in jitd_lock names the jitd allowed to write and until
// when, by the database's clock. jitd renews it every leaseRenew while it
// runs, and every write transaction first checks (and extends) it, so a jitd
// that lost the lease cannot write. It is plain SQL, so it works through
// connection poolers and on databases without advisory locks.

// claimLease takes or extends the lease for s.holder if it is free, expired
// or already ours. It reports whether s.holder holds it afterwards.
func (s *Store) claimLease(ctx context.Context, x interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, onlyOurs bool) (bool, error) {
	ttl := leaseTTL.Milliseconds()
	q := "UPDATE jitd_lock SET holder = ?, expires_at = " + s.dia.nowMillis + " + ? WHERE id = 1 AND "
	args := []any{s.holder, ttl, s.holder}
	if onlyOurs {
		q += "holder = ? AND expires_at > " + s.dia.nowMillis
	} else {
		q += "(holder = ? OR expires_at <= " + s.dia.nowMillis + ")"
	}
	res, err := x.ExecContext(ctx, s.dia.q(q), args...)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// acquireLease takes the lease, waiting for one left by a jitd that stopped
// without releasing it (a crash, a killed pod) to expire.
func (s *Store) acquireLease() error {
	host, _ := os.Hostname()
	b := make([]byte, 6)
	rand.Read(b)
	s.holder = fmt.Sprintf("%s/%d/%s", host, os.Getpid(), hex.EncodeToString(b))
	deadline := time.Now().Add(leaseTTL + 5*time.Second)
	for {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		ok, err := s.claimLease(ctx, s.db, false)
		cancel()
		if err != nil {
			return fmt.Errorf("taking the writer lease: %w", err)
		}
		if ok {
			s.leaseOK = start.Add(leaseTTL - leaseTTL/4)
			return nil
		}
		if time.Now().After(deadline) {
			var other string
			s.db.QueryRow("SELECT holder FROM jitd_lock WHERE id = 1").Scan(&other)
			return fmt.Errorf("another jitd (%s) is using this database: run one jitd per database", other)
		}
		time.Sleep(time.Second)
	}
}

// fence checks, inside a write transaction, that this jitd still holds the
// lease, and extends it. SQLite needs no lease: the file has one user.
func (s *Store) fence(ctx context.Context, tx *sql.Tx) error {
	if s.holder == "" {
		return nil
	}
	start := time.Now()
	ok, err := s.claimLease(ctx, tx, true)
	if err != nil {
		return fmt.Errorf("checking the writer lease: %w", err)
	}
	if !ok {
		return s.loseLease(errors.New("this jitd no longer holds the database's writer lease (another jitd took over, or renewing failed for too long)"))
	}
	s.leaseOK = start.Add(leaseTTL - leaseTTL/4)
	return nil
}

// execFenced runs one statement in a fenced transaction. Callers hold s.mu.
func (s *Store) execFenced(query string, args ...any) error {
	if s.broken != nil {
		return s.broken
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.fence(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return tx.Commit()
}

// renewLease extends the lease every leaseRenew. If it cannot (the database
// is unreachable) until the lease may have expired, or finds it taken, the
// store stops saving and LockLost reports it.
func (s *Store) renewLease(stop <-chan struct{}) {
	t := time.NewTicker(leaseRenew)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		s.mu.Lock()
		if s.stopWatch == nil || s.broken != nil {
			s.mu.Unlock()
			return
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), leaseRenew)
		ok, err := s.claimLease(ctx, s.db, true)
		cancel()
		switch {
		case err == nil && ok:
			s.leaseOK = start.Add(leaseTTL - leaseTTL/4)
		case err == nil:
			s.loseLease(errors.New("another jitd took over the database's writer lease"))
		case time.Now().After(s.leaseOK):
			s.loseLease(fmt.Errorf("could not renew the database's writer lease in time: %w", err))
		}
		done := s.broken != nil
		s.mu.Unlock()
		if done {
			return
		}
	}
}

// loseLease stops the store from saving and reports why. Callers hold s.mu.
func (s *Store) loseLease(err error) error {
	if s.broken == nil {
		s.broken = err
		select {
		case s.lost <- err:
		default:
		}
	}
	return s.broken
}

// releaseLease gives the lease up, best effort. Callers hold s.mu, or own s.
func (s *Store) releaseLease() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.db.ExecContext(ctx, s.dia.q("UPDATE jitd_lock SET holder = '', expires_at = 0 WHERE id = 1 AND holder = ?"), s.holder)
}

// LockLost delivers an error if a server database's writer lease is lost.
// jitd should then exit and start again. It never fires for SQLite.
func (s *Store) LockLost() <-chan error { return s.lost }

// Dialect names the kind of database: "sqlite", "postgres" or "mysql".
func (s *Store) Dialect() string { return s.dia.name }
