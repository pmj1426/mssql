package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/microsoft/go-mssqldb/integratedauth/krb5"
	"github.com/scorify/schema"
)

type Schema struct {
	Server    string `key:"target"`
	Port      int    `key:"port" default:"3306"`
	KDCServer string `key:"kdcserver"`
	KRBPath   string `key:"krbpath"`
	Username  string `key:"username"`
	Password  string `key:"password"`
	Database  string `key:"database"`
	Query     string `key:"query"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.Server == "" {
		return fmt.Errorf("server is required; got %q", conf.Server)
	}

	if conf.Port <= 0 || conf.Port > 65535 {
		return fmt.Errorf("port is invalid; got %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username is required; got %q", conf.Username)
	}

	if conf.Password == "" {
		return fmt.Errorf("password is required; got %q", conf.Password)
	}

	if conf.Database == "" {
		return fmt.Errorf("database is required; got %q", conf.Database)
	}

	if conf.KRBPath == "" {
		return fmt.Errorf("domain is required; got %q", conf.KRBPath)
	}
	//need port as part of it
	if conf.KDCServer == "" {
		return fmt.Errorf("KDC Server is required; got %q", conf.KDCServer)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("context deadline is not set")
	}

	AdoConnStr := fmt.Sprintf(
		"authenticator=krb5;server=%v,%v;database=%v;user id=%v;password=%v;krb5-realm=%v;krb5-configfile=%v;connection timeout=%v",
		conf.Server,
		conf.Port,
		conf.Database,
		conf.Username,
		conf.Password,
		conf.KDCServer,
		conf.KRBPath,
		int(math.Floor(time.Until(deadline).Seconds())),
	)

	conn, err := sql.Open("sqlserver", AdoConnStr)
	if err != nil {
		return fmt.Errorf("failed to open mssql connection: %v", err)
	}
	defer conn.Close()

	conn.SetMaxIdleConns(0)
	conn.SetMaxOpenConns(1)

	err = conn.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping mssql server: %w", err)
	}

	if conf.Query != "" {
		rows, err := db.QueryContext(execCtx, conf.Query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		defer rows.Close()

		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return fmt.Errorf("query failed while reading rows: %w", err)
			}
			return fmt.Errorf("no rows returned from query: %q", conf.Query)
		}
	}
	return nil
}
