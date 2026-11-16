package jit

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Server database tests run against real servers, named by
// JITD_TEST_POSTGRES_URL and JITD_TEST_MYSQL_URL, and are skipped without
// them. Each test drops jitd's tables first, so point them at a scratch
// database. For example:
//
//	docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=jitd postgres:17
//	docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=pw -e MYSQL_DATABASE=jitd mysql:8.4
//	JITD_TEST_POSTGRES_URL='postgres://postgres:pw@localhost:5432/jitd?sslmode=disable' \
//	JITD_TEST_MYSQL_URL='mysql://root:pw@localhost:3306/jitd' go test ./jitd/internal/jit -run Server
func serverURLs(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}
	for name, env := range map[string]string{"postgres": "JITD_TEST_POSTGRES_URL", "mysql": "JITD_TEST_MYSQL_URL"} {
		if v := os.Getenv(env); v != "" {
			out[name] = v
		}
	}
	if len(out) == 0 {
		t.Skip("set JITD_TEST_POSTGRES_URL and/or JITD_TEST_MYSQL_URL to test database servers")
	}
	return out
}

// eachServer runs fn against each configured server, on a database with
// none of jitd's tables.
func eachServer(t *testing.T, fn func(t *testing.T, url string)) {
	for name, url := range serverURLs(t) {
		t.Run(name, func(t *testing.T) {
			dropServerTables(t, url)
			fn(t, url)
		})
	}
}

func rawServerDB(t *testing.T, url string) (*sql.DB, *dialect) {
	t.Helper()
	d := serverDialect(url)
	driver, dsn, err := driverDSN(d, url)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, d
}

func dropServerTables(t *testing.T, url string) {
	t.Helper()
	db, _ := rawServerDB(t, url)
	for _, tbl := range []string{"workflows", "requests", "settings", "jitd_schema", "jitd_lock", "other"} {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatal(err)
		}
	}
}

func TestServerStoreRoundTrip(t *testing.T) {
	eachServer(t, func(t *testing.T, url string) {
		s := openStore(t, url)
		if s.Dialect() == "sqlite" || strings.Contains(s.Path, "pw@") {
			t.Errorf("dialect %s, path %q (password must be hidden)", s.Dialect(), s.Path)
		}
		created := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
		mustUpdate(t, s, func(d *Data) error {
			d.Workflows["wf_1"] = &Workflow{ID: "wf_1", Name: "one ünïcødé ✓", MaxDuration: DefaultMaxDuration}
			d.Workflows["wf_2"] = &Workflow{ID: "wf_2", Name: "two"}
			d.Requests["req_1"] = &AccessRequest{ID: "req_1", WorkflowID: "wf_1", Status: StatusPending, CreatedAt: created,
				Requester: Identity{Login: "alice@example.com"}, Reason: strings.Repeat("long reason ", 2000)}
			d.Requests["req_2"] = &AccessRequest{ID: "req_2", WorkflowID: "wf_1", Status: StatusPending}
			return nil
		})
		// Update one row, delete two, leave one alone.
		mustUpdate(t, s, func(d *Data) error {
			d.Requests["req_1"].Status = StatusApproved
			delete(d.Requests, "req_2")
			delete(d.Workflows, "wf_2")
			return nil
		})
		s.Close()

		s2 := openStore(t, url)
		s2.View(func(d *Data) {
			if len(d.Workflows) != 1 || len(d.Requests) != 1 {
				t.Fatalf("reloaded %d flows, %d requests; want 1, 1", len(d.Workflows), len(d.Requests))
			}
			r := d.Requests["req_1"]
			if d.Workflows["wf_1"].Name != "one ünïcødé ✓" || r.Status != StatusApproved || !r.CreatedAt.Equal(created) || len(r.Reason) != 24000 {
				t.Errorf("reloaded: flow %q, request %+v", d.Workflows["wf_1"].Name, r.Status)
			}
		})
		// The columns for ad-hoc queries are filled in.
		var status, requester string
		if err := s2.db.QueryRow(s2.dia.q("SELECT status, requester FROM requests WHERE id = ?"), "req_1").Scan(&status, &requester); err != nil {
			t.Fatal(err)
		}
		if status != "approved" || requester != "alice@example.com" {
			t.Errorf("columns: %s %s", status, requester)
		}
	})
}

func TestServerStoreSettings(t *testing.T) {
	eachServer(t, func(t *testing.T, url string) {
		s := openStore(t, url)
		type slack struct{ Team string }
		if ok, err := s.GetSetting("slack", &slack{}); ok || err != nil {
			t.Fatalf("missing setting: %v %v", ok, err)
		}
		for _, team := range []string{"T1", "T2"} { // insert, then replace
			if err := s.PutSetting("slack", slack{Team: team}); err != nil {
				t.Fatal(err)
			}
		}
		var got slack
		if ok, err := s.GetSetting("slack", &got); !ok || err != nil || got.Team != "T2" {
			t.Fatalf("setting = %+v %v %v", got, ok, err)
		}
		if err := s.DeleteSetting("slack"); err != nil {
			t.Fatal(err)
		}
		if ok, _ := s.GetSetting("slack", &got); ok {
			t.Error("setting not deleted")
		}
	})
}

func TestServerStoreUpdateErrorRollsBack(t *testing.T) {
	eachServer(t, func(t *testing.T, url string) {
		s := openStore(t, url)
		mustUpdate(t, s, func(d *Data) error {
			d.Workflows["wf_1"] = &Workflow{ID: "wf_1", Name: "one"}
			return nil
		})
		// A row the database refuses (a key too long for MySQL's column, or
		// a nil request) fails the update and leaves the working copy as it
		// was.
		err := s.Update(func(d *Data) error {
			d.Workflows["wf_1"].Name = "changed"
			d.Requests["req_x"] = nil
			return nil
		})
		if err == nil {
			t.Fatal("update with a nil request succeeded")
		}
		s.View(func(d *Data) {
			if d.Workflows["wf_1"].Name != "one" || len(d.Requests) != 0 {
				t.Errorf("working copy not rolled back: %+v", d.Workflows["wf_1"])
			}
		})
	})
}

// fastLease shortens the writer lease for a test.
func fastLease(t *testing.T) {
	ttl, renew := leaseTTL, leaseRenew
	leaseTTL, leaseRenew = time.Second, 100*time.Millisecond
	t.Cleanup(func() { leaseTTL, leaseRenew = ttl, renew })
}

func TestServerStoreOneWriter(t *testing.T) {
	fastLease(t)
	eachServer(t, func(t *testing.T, url string) {
		s := openStore(t, url)
		// The first jitd keeps renewing, so a second one gives up after
		// waiting out a lease.
		start := time.Now()
		if _, err := Open(url); err == nil || !strings.Contains(err.Error(), "another jitd") {
			t.Fatalf("second writer: %v", err)
		}
		if waited := time.Since(start); waited < leaseTTL {
			t.Errorf("second writer gave up after %v, before the lease could expire", waited)
		}
		// Reading (jitd -policy-snippet) works alongside the writer.
		ro, err := OpenReadOnly(url)
		if err != nil {
			t.Fatalf("read-only open: %v", err)
		}
		ro.Close()
		// A clean stop releases the lease at once.
		s.Close()
		start = time.Now()
		s2, err := Open(url)
		if err != nil {
			t.Fatalf("after the first closed: %v", err)
		}
		if time.Since(start) > leaseTTL/2 {
			t.Errorf("waited %v for a released lease", time.Since(start))
		}
		s2.Close()
	})
}

func TestServerStoreTakesOverAfterCrash(t *testing.T) {
	fastLease(t)
	eachServer(t, func(t *testing.T, url string) {
		s := openStore(t, url)
		// A crash: renewing stops and nothing releases the lease.
		s.mu.Lock()
		close(s.stopWatch)
		s.stopWatch = nil
		s.mu.Unlock()
		s2, err := Open(url) // waits for the lease to expire
		if err != nil {
			t.Fatalf("taking over: %v", err)
		}
		defer s2.Close()
		mustUpdate(t, s2, func(d *Data) error { d.Workflows["wf_1"] = &Workflow{ID: "wf_1"}; return nil })
		// The crashed one, had it come back, could not write.
		err = s.Update(func(d *Data) error { d.Workflows["wf_2"] = &Workflow{ID: "wf_2"}; return nil })
		if err == nil || !strings.Contains(err.Error(), "writer lease") {
			t.Errorf("old holder wrote after losing the lease: %v", err)
		}
	})
}

func TestServerStoreRefusesOtherTables(t *testing.T) {
	eachServer(t, func(t *testing.T, url string) {
		db, _ := rawServerDB(t, url)
		if _, err := db.Exec("CREATE TABLE workflows (x INTEGER)"); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(url); err == nil || !strings.Contains(err.Error(), "not a jitd database") {
			t.Errorf("foreign table: %v", err)
		}
		if _, err := OpenReadOnly(url); err == nil {
			t.Error("read-only open of a database without jitd's tables succeeded")
		}
	})
}

func TestServerStoreRefusesNewerSchema(t *testing.T) {
	eachServer(t, func(t *testing.T, url string) {
		openStore(t, url).Close()
		db, _ := rawServerDB(t, url)
		if _, err := db.Exec("UPDATE jitd_schema SET version = 99"); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(url); err == nil || !strings.Contains(err.Error(), "newer jitd") {
			t.Errorf("newer schema: %v", err)
		}
	})
}

func TestServerStoreLockLost(t *testing.T) {
	fastLease(t)
	eachServer(t, func(t *testing.T, url string) {
		s := openStore(t, url)
		// Someone takes the lease (as a second jitd would after this one
		// stalled for longer than a lease).
		db, d := rawServerDB(t, url)
		if _, err := db.Exec(d.q("UPDATE jitd_lock SET holder = ?, expires_at = "+d.nowMillis+" + 60000"), "someone else"); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-s.LockLost():
			if !strings.Contains(err.Error(), "took over") {
				t.Errorf("lost: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("losing the lease was not noticed")
		}
		if err := s.Update(func(d *Data) error { d.Workflows["wf_1"] = &Workflow{ID: "wf_1"}; return nil }); err == nil {
			t.Error("the store kept saving after losing the lease")
		}
		if err := s.PutSetting("x", 1); err == nil {
			t.Error("settings kept saving after losing the lease")
		}
	})
}

func TestDatabaseURLs(t *testing.T) {
	for _, tc := range []struct {
		in     string
		server bool
	}{
		{"jit.db", false}, {"/data/jit.db", false}, {"old.json", false}, {"file:jit.db", false},
		{"postgres://u:p@h/db", true}, {"postgresql://h/db", true}, {"MySQL://u@h/db", true}, {"sqlite://x", false},
	} {
		if got := IsDatabaseURL(tc.in); got != tc.server {
			t.Errorf("IsDatabaseURL(%q) = %v", tc.in, got)
		}
	}
	if got := RedactURL("postgres://jitd:s3cret@db:5432/jitd?sslmode=require"); strings.Contains(got, "s3cret") || !strings.Contains(got, "jitd:xxxxx@db") {
		t.Errorf("RedactURL = %q", got)
	}
	_, dsn, err := driverDSN(mysqlDialect, "mysql://jitd:pw@db/jitd?tls=true")
	if err != nil || dsn != "jitd:pw@tcp(db:3306)/jitd?clientFoundRows=true&multiStatements=true&tls=true" {
		t.Errorf("mysql DSN = %q, %v", dsn, err)
	}
	_, dsn, err = driverDSN(mysqlDialect, "mysql://jitd:pw@/jitd?socket=/cloudsql/p:r:i")
	if err != nil || !strings.Contains(dsn, "unix(/cloudsql/p:r:i)/jitd") {
		t.Errorf("mysql socket DSN = %q, %v", dsn, err)
	}
	if _, _, err := driverDSN(mysqlDialect, "mysql://jitd:pw@db"); err == nil {
		t.Error("mysql URL without a database accepted")
	}
	if got := postgresDialect.q("SELECT ? WHERE a = ? AND b = ?"); got != "SELECT $1 WHERE a = $2 AND b = $3" {
		t.Errorf("postgres placeholders: %q", got)
	}
}

var scratchN atomic.Int64

// scratchServerURL creates an empty schema (Postgres) or database (MySQL)
// on the server base names, dropped when the test ends, and returns a URL
// for it. Tests that open several stores each get their own.
func scratchServerURL(t *testing.T, base string) string {
	t.Helper()
	name := fmt.Sprintf("jitd_test_%d_%d", os.Getpid(), scratchN.Add(1))
	db, d := rawServerDB(t, base)
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	drop := "DROP DATABASE IF EXISTS " + name
	if d.name == "postgres" {
		drop = "DROP SCHEMA IF EXISTS " + name + " CASCADE"
		if _, err := db.Exec("CREATE SCHEMA " + name); err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("search_path", name)
		u.RawQuery = q.Encode()
	} else {
		if _, err := db.Exec("CREATE DATABASE " + name); err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + name
	}
	t.Cleanup(func() {
		// A fresh connection: the test's stores are closed by now.
		cdb, _ := rawServerDB(t, base)
		cdb.Exec(drop)
	})
	return u.String()
}
