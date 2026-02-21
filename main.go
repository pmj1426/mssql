package mssql

import (
	"context"
	"database/sql"
	"fmt"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/scorify/schema"
)

type Schema struct {
	Server    string `key:"target"`
	Port      int    `key:"port" default:"3306"`
	KDCServer string `key:"kdcserver"`
	//KDCPort   int    `key:"kdcport" default:"88"`
	Domain   string `key:"domain"`
	Username string `key:"username"`
	Password string `key:"password"`
	Database string `key:"database"`
	Query    string `key:"query"`
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

	if conf.KDCServer == "" {
		return fmt.Errorf("KDC Server is required; got %q", conf.KDCServer)
	}

	//if conf.KDCPort <= 0 || conf.KDCPort > 65535 {
	//	return fmt.Errorf("port is invalid; got %d", conf.KDCPort)
	//}

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

	krbConf := fmt.Sprintf(`
[libdefaults]
 default_realm = %s
 dns_lookup_kdc = false
[realms]
 %s = { kdc = %s }
`, conf.Domain, conf.Domain, conf.KDCServer)

	cfg, err := config.NewFromString(krbConf)
	if err != nil {
		return err
	}

	cl := client.NewWithPassword(conf.Username, conf.Domain, conf.Password, cfg)
	if err := cl.Login(); err != nil {
		return fmt.Errorf("Kerberos login failed: %v", err)
	}

	conn, err := mssql.NewKerberosConnector(
		fmt.Sprintf("sqlserver://%s:%d?database=master", conf.Server, conf.Port),
		cl,
	)

	db := sql.OpenDB(conn)
	defer db.Close()

	ctx, cancel := context.WithDeadline(
		context.Background(),
		deadline,
	)
	defer cancel()

	conn.SetMaxIdleConns(-1)
	conn.SetMaxOpenConns(1)

	err = conn.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping mssql server: %w", err)
	}

	if conf.Query != "" {
		rows, err := conn.QueryContext(ctx, conf.Query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		defer rows.Close()

		if !rows.Next() {
			return fmt.Errorf("no rows returned from query: %q", conf.Query)
		}
	}
	defer cl.Destroy()
	return nil
}
