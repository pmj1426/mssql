package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
	_ "github.com/microsoft/go-mssqldb/integratedauth/krb5"
	"github.com/scorify/schema"
)

type Schema struct {
	Server         string `key:"target"`
	Port           int    `key:"port" default:"1433"`
	Domain         string `key:"domain"`
	Username       string `key:"username"`
	Password       string `key:"password"`
	Database       string `key:"database"`
	Krb5ConfigFile string `key:"krb5_config_file"`
	Query          string `key:"query"`
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

	if conf.Domain == "" {
		return fmt.Errorf("domain is required; got %q", conf.Domain)
	}

	if conf.Krb5ConfigFile == "" {
		return fmt.Errorf("krb5_config_file is required; got %q", conf.Krb5ConfigFile)
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

	u := &url.URL{
		Scheme: "sqlserver",
		Host:   fmt.Sprintf("%s:%d", conf.Server, conf.Port),
		User:   url.UserPassword(conf.Username, conf.Password),
	}

	query := u.Query()
	query.Set("database", conf.Database)
	query.Set("authenticator", "krb5")
	query.Set("krb5-configfile", conf.Krb5ConfigFile)
	if conf.Domain != "" && !strings.Contains(conf.Username, "@") {
		query.Set("krb5-realm", conf.Domain)
	}
	u.RawQuery = query.Encode()

	connector, err := mssql.NewConnector(u.String())
	if err != nil {
		return fmt.Errorf("failed to build mssql connector: %w", err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	execCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(1)

	err = db.PingContext(execCtx)
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
