package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"gopkg.in/jackc/pgx.v2"
	"gopkg.in/urfave/cli.v1"

	"github.com/sirupsen/logrus"
)

type Response struct {
	Organism string
	Error    error
}

type Feedback struct {
	Error error
	Done  bool
	Count int
}

func validateOrganism(c *cli.Context) error {
	if err := validateArgs(c); err != nil {
		return err
	}
	return nil
}

func validateOrganismPlus(c *cli.Context) error {
	if err := validateArgs(c); err != nil {
		return err
	}
	if err := validateS3Args(c); err != nil {
		return err
	}
	return nil
}

func OrganismPlusAction(c *cli.Context) error {
	log := getLogger(c)
	if !definedPostgres(c) || !definedChadoUser(c) {
		log.WithFields(logrus.Fields{
			"type": "organism-plus-loader",
			"kind": "no database information",
		}).Error("could not load organism data")
		return cli.NewExitError("Could not load organism data", 2)
	}

	// database connection
	connPoolConfig, err := getConnPoolConfig(c)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	connPool, err := pgx.NewConnPool(connPoolConfig)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	defer connPool.Close()
	// reading from the file
	filename, err := fetchRemoteFile(c, "organism")
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	reader, err := os.Open(filename)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("error opening file %s %s", filename, err.Error()), 2)
	}
	defer reader.Close()

	// load organisms
	rch := make(chan Response, 20)
	fch := make(chan Feedback)
	go extractOrganisms(reader, rch)
	go loadExtraOrganisms(connPool, rch, fch)
	fb := <-fch
	if fb.Error != nil {
		return cli.NewExitError(fb.Error.Error(), 2)
	}
	log.WithFields(logrus.Fields{
		"type": "organism-plus-loader",
		"kind": "loading-success",
	}).Info("extra organism data for stocks loaded successfully")
	return nil
}

func OrganismAction(c *cli.Context) error {
	log := getLogger(c)
	dsn := getPostgresDsn(c)
	mi, err := exec.LookPath("modware-import")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "binary-lookup",
			"name": "modware-import",
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	cmdline := []string{
		"organism2chado",
		"--log_level",
		"debug",
		"--dsn",
		dsn,
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
	}
	out, err := exec.Command(mi, cmdline...).CombinedOutput()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":        "organism-loader",
			"kind":        "loading-issue",
			"status":      string(out),
			"commandline": strings.Join(cmdline, " "),
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.WithFields(logrus.Fields{
		"type":        "organism-loader",
		"kind":        "loading-success",
		"commandline": strings.Join(cmdline, " "),
	}).Info("organism data loaded successfully")
	return nil
}

func loadExtraOrganisms(conn *pgx.ConnPool, rch <-chan Response, fch chan<- Feedback) {
	fb := Feedback{}
	tx, err := conn.Begin()
	if err != nil {
		fb.Error = err
	} else {
		count := 0
		defer tx.Rollback()
		for r := range rch {
			if r.Error != nil {
				fb.Error = r.Error
				break
			}
			genus, species := splitOrganism(r.Organism)
			var id int
			err := tx.QueryRow("getOrganism", genus, species).Scan(&id)
			switch err {
			case nil:
				continue
			case pgx.ErrNoRows:
				abbrev := makeOrganismAbbrev(genus, species)
				if _, err := tx.Exec("createOrganism", genus, species, abbrev); err != nil {
					fb.Error = r.Error
					break
				}
				count = count + 1
			default:
				fb.Error = fmt.Errorf("unknown error with sql query %s", err)
				break
			}
		}
		fb.Count = count
	}
	if fb.Error == nil {
		err = tx.Commit()
		if err != nil {
			fb.Error = err
		}
	}
	fb.Done = true
	fch <- fb
	close(fch)
}

func splitOrganism(organism string) (string, string) {
	sl := strings.Split(organism, " ")
	return sl[0], sl[1]
}

func makeOrganismAbbrev(genus, species string) string {
	return fmt.Sprintf("%s.%s", genus[:1], species)
}

func extractOrganisms(rd io.Reader, rch chan<- Response) {
	cache := make(map[string]int)
	resp := Response{}
	scanner := bufio.NewScanner(rd)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		sl := strings.Split(scanner.Text(), "\t")
		if _, ok := cache[sl[2]]; ok {
			continue
		}
		resp.Organism = sl[2]
		rch <- resp
		cache[sl[2]] = 1
	}
	if err := scanner.Err(); err != nil {
		resp.Error = err
		rch <- resp
	}
	close(rch)
}
