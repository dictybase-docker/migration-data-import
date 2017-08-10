package main

import (
	"database/sql"
	"fmt"

	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx/stdlib"
	"gopkg.in/mgutz/dat.v1/sqlx-runner"
	"gopkg.in/urfave/cli.v1"
)

func afterConnect(conn *pgx.Conn) error {
	_, err := conn.Prepare("getOrganism", `
		SELECT organism_id FROM organism WHERE genus=$1 and species=$2
	`)
	if err != nil {
		return nil
	}
	_, err = conn.Prepare("createOrganism", `
		INSERT INTO organism(genus,species,abbreviation) VALUES($1,$2,$3)
	`)
	if err != nil {
		return err
	}
	return nil
}

func getConnConfig(c *cli.Context) (pgx.ConnConfig, error) {
	dsn := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		c.GlobalString("chado-user"), c.GlobalString("chado-pass"), c.GlobalString("pghost"),
		c.GlobalString("pgport"), c.GlobalString("chado-db"),
	)
	connConfig, err := pgx.ParseDSN(dsn)
	if err != nil {
		return pgx.ConnConfig{}, err
	}
	return connConfig, nil
}

func getConnection(c *cli.Context) (*pgx.Conn, error) {
	config, err := getConnConfig(c)
	if err != nil {
		return &pgx.Conn{}, err
	}
	return pgx.Connect(config)
}

func getPostgresDsn(c *cli.Context) string {
	return fmt.Sprintf("dbi:Pg:host=%s;port=%s;database=%s", c.GlobalString("pghost"),
		c.GlobalString("pgport"), c.GlobalString("chado-db"))
}

func getConnPoolConfig(c *cli.Context) (pgx.ConnPoolConfig, error) {
	dsn := fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		c.GlobalString("chado-user"), c.GlobalString("chado-pass"), c.GlobalString("pghost"),
		c.GlobalString("pgport"), c.GlobalString("chado-db"),
	)
	connConfig, err := pgx.ParseDSN(dsn)
	if err != nil {
		return pgx.ConnPoolConfig{}, err
	}
	return pgx.ConnPoolConfig{
		ConnConfig:     connConfig,
		MaxConnections: 3,
		AfterConnect:   afterConnect,
	}, nil
}

func getConnPool(c *cli.Context) (*pgx.ConnPool, error) {
	connConfig, err := getConnPoolConfig(c)
	if err != nil {
		return &pgx.ConnPool{}, err
	}
	return pgx.NewConnPool(connConfig)
}

func sendNotification(c *cli.Context, channel, payload string) error {
	connConfig, err := getConnConfig(c)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(connConfig)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Exec("SELECT pg_notify($1, $2)", channel, payload)
	if err != nil {
		return err
	}
	return nil
}

func sendNotificationWithConn(conn *pgx.Conn, channel, payload string) error {
	_, err := conn.Exec("SELECT pg_notify($1, $2)", channel, payload)
	if err != nil {
		return err
	}
	return nil
}

func getPgWrapper(c *cli.Context) (*runner.DB, error) {
	var dbh *runner.DB
	h, err := getPgxDbHandler(c)
	if err != nil {
		return dbh, err
	}
	return runner.NewDB(h, "postgres"), nil
}

func getPgxDbHandler(c *cli.Context) (*sql.DB, error) {
	cStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.GlobalString("chado-user"),
		c.GlobalString("chado-pass"),
		c.GlobalString("pghost"),
		c.GlobalString("pgport"),
		c.GlobalString("chado-db"),
	)
	return sql.Open("pgx", cStr)
}
