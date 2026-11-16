package jit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver
)

// dialect is what differs between the databases jitd can keep its data in:
// SQLite (a file, the default), and Postgres or MySQL (a server, named by a
// URL). Everything else in Store is shared SQL.
type dialect struct {
	name   string // "sqlite", "postgres", "mysql"
	server bool   // a database server rather than a file

	// schema creates the current tables on an empty server database.
	schema string
	// nowMillis is the database's clock, in milliseconds since the epoch.
	// The writer lease uses it, so jitd hosts' clocks don't matter.
	nowMillis string
	// upsertWorkflow and upsertRequest insert or replace one row; written
	// with ? placeholders, in the order Store.write passes them.
	upsertWorkflow, upsertRequest, upsertSetting string
}

var (
	sqliteDialect = &dialect{
		name: "sqlite",
		upsertWorkflow: `INSERT INTO workflows (id, name, data) VALUES (?, ?, ?)
			ON CONFLICT (id) DO UPDATE SET name = excluded.name, data = excluded.data`,
		upsertRequest: `INSERT INTO requests (id, workflow_id, requester, status, created_at, data)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT (id) DO UPDATE SET workflow_id = excluded.workflow_id, requester = excluded.requester,
				status = excluded.status, created_at = excluded.created_at, data = excluded.data`,
		upsertSetting: `INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
	}

	// Postgres also covers CockroachDB, AlloyDB, Cloud SQL, RDS, Neon and
	// anything else that speaks its protocol.
	postgresDialect = &dialect{
		name: "postgres", server: true,
		schema: `
CREATE TABLE workflows (
	id   TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	data TEXT NOT NULL
);
CREATE TABLE requests (
	id          TEXT PRIMARY KEY,
	workflow_id TEXT NOT NULL,
	requester   TEXT NOT NULL,
	status      TEXT NOT NULL,
	created_at  TEXT NOT NULL,
	data        TEXT NOT NULL
);
CREATE INDEX requests_status ON requests(status);
CREATE INDEX requests_workflow ON requests(workflow_id);
CREATE INDEX requests_created ON requests(created_at);
CREATE TABLE settings (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE jitd_schema (version INTEGER NOT NULL);
CREATE TABLE jitd_lock (
	id         INTEGER PRIMARY KEY,
	holder     TEXT NOT NULL,
	expires_at BIGINT NOT NULL
);
INSERT INTO jitd_lock (id, holder, expires_at) VALUES (1, '', 0);
`,
		nowMillis:      `(extract(epoch from now()) * 1000)::BIGINT`,
		upsertWorkflow: sqliteDialect.upsertWorkflow,
		upsertRequest:  sqliteDialect.upsertRequest,
		upsertSetting:  sqliteDialect.upsertSetting,
	}

	// MySQL also covers MariaDB, and Cloud SQL, RDS and Aurora for MySQL.
	// Keys are VARCHAR (MySQL cannot index TEXT), with room to spare: ids
	// are 20 characters.
	mysqlDialect = &dialect{
		name: "mysql", server: true,
		schema: `
CREATE TABLE workflows (
	id   VARCHAR(64) NOT NULL PRIMARY KEY,
	name TEXT NOT NULL,
	data LONGTEXT NOT NULL
) DEFAULT CHARSET=utf8mb4;
CREATE TABLE requests (
	id          VARCHAR(64) NOT NULL PRIMARY KEY,
	workflow_id VARCHAR(64) NOT NULL,
	requester   VARCHAR(320) NOT NULL,
	status      VARCHAR(32) NOT NULL,
	created_at  VARCHAR(40) NOT NULL,
	data        LONGTEXT NOT NULL,
	INDEX requests_status (status),
	INDEX requests_workflow (workflow_id),
	INDEX requests_created (created_at)
) DEFAULT CHARSET=utf8mb4;
CREATE TABLE settings (
	` + "`key`" + ` VARCHAR(191) NOT NULL PRIMARY KEY,
	value LONGTEXT NOT NULL
) DEFAULT CHARSET=utf8mb4;
CREATE TABLE jitd_schema (version INTEGER NOT NULL);
CREATE TABLE jitd_lock (
	id         INTEGER NOT NULL PRIMARY KEY,
	holder     VARCHAR(255) NOT NULL,
	expires_at BIGINT NOT NULL
);
INSERT INTO jitd_lock (id, holder, expires_at) VALUES (1, '', 0);
`,
		nowMillis: `CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)`,
		upsertWorkflow: `INSERT INTO workflows (id, name, data) VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE name = VALUES(name), data = VALUES(data)`,
		upsertRequest: `INSERT INTO requests (id, workflow_id, requester, status, created_at, data)
			VALUES (?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE workflow_id = VALUES(workflow_id), requester = VALUES(requester),
				status = VALUES(status), created_at = VALUES(created_at), data = VALUES(data)`,
		upsertSetting: "INSERT INTO settings (`key`, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)",
	}
)

// q rewrites a query written with ? placeholders for d. Only Postgres needs
// it ($1, $2, ...). jitd's queries have no ? inside string literals.
func (d *dialect) q(query string) string {
	if d.name != "postgres" {
		return query
	}
	var b strings.Builder
	n := 0
	for _, c := range query {
		if c == '?' {
			n++
			b.WriteString("$" + strconv.Itoa(n))
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

// settingKey is the settings table's key column, which MySQL needs quoted.
func (d *dialect) settingKey() string {
	if d.name == "mysql" {
		return "`key`"
	}
	return "key"
}

// IsDatabaseURL reports whether target names a database server (a
// postgres://, postgresql:// or mysql:// URL) rather than a SQLite file.
func IsDatabaseURL(target string) bool {
	return serverDialect(target) != nil
}

func serverDialect(target string) *dialect {
	scheme, _, ok := strings.Cut(target, "://")
	if !ok {
		return nil
	}
	switch strings.ToLower(scheme) {
	case "postgres", "postgresql":
		return postgresDialect
	case "mysql":
		return mysqlDialect
	}
	return nil
}

// RedactURL returns target with its password replaced, for logs.
func RedactURL(target string) string {
	u, err := url.Parse(target)
	if err != nil || u.User == nil {
		return target
	}
	if _, ok := u.User.Password(); ok {
		u.User = url.UserPassword(u.User.Username(), "xxxxx")
	}
	return u.String()
}

// driverDSN turns a jitd database URL into what the driver takes.
func driverDSN(d *dialect, target string) (driver, dsn string, err error) {
	if d.name == "postgres" {
		return "pgx", target, nil
	}
	// mysql://user:pass@host:port/db?param=value, in the driver's own form
	// user:pass@tcp(host:port)/db?param=value. A host starting with "/" is a
	// Unix socket (Cloud SQL's connector, for one): mysql://user:pass@/db?socket=/path.
	u, err := url.Parse(target)
	if err != nil {
		return "", "", fmt.Errorf("database URL: %w", err)
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		return "", "", errors.New("database URL: name the database, as in mysql://user:password@host:3306/jitd")
	}
	cfg := mysql.NewConfig()
	cfg.DBName = db
	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Passwd, _ = u.User.Password()
	}
	q := u.Query()
	if sock := q.Get("socket"); sock != "" {
		cfg.Net, cfg.Addr = "unix", sock
		q.Del("socket")
	} else {
		cfg.Net, cfg.Addr = "tcp", u.Host
		if u.Port() == "" && u.Host != "" {
			cfg.Addr = u.Host + ":3306"
		}
	}
	cfg.MultiStatements = true // for the schema
	cfg.ClientFoundRows = true // an UPDATE reports the rows it matched, even if unchanged
	cfg.ParseTime = false
	cfg.Params = map[string]string{}
	for k, v := range q {
		if len(v) > 0 {
			cfg.Params[k] = v[0]
		}
	}
	return "mysql", cfg.FormatDSN(), nil
}

// hasTable reports whether the server database has a table called name, in
// the schema jitd uses.
func (d *dialect) hasTable(ctx context.Context, db *sql.DB, name string) (bool, error) {
	q := `SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?`
	if d.name == "mysql" {
		q = `SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`
	}
	var n int
	err := db.QueryRowContext(ctx, d.q(q), name).Scan(&n)
	return n > 0, err
}

// The writer lease: a jitd on a database server holds it for leaseTTL and
// renews it every leaseRenew. Variables for tests.
var (
	leaseTTL   = 30 * time.Second
	leaseRenew = 10 * time.Second
)
