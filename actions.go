package main

import (
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/codegangsta/cli.v1"
)

func OrganismAction(c *cli.Context) {
	if hasEtcd(c) {
		if err := waitForEtcd(c.GlobalString("key-watch"), c); err != nil {
			log.WithFields(log.Fields{
				"type": "organism-loader",
				"key":  c.GlobalString("key-watch"),
				"kind": "etcd-wait",
			}).Fatal(err)
		}
		log.WithFields(log.Fields{
			"type": "organism-loader",
			"key":  c.GlobalString("key-watch"),
			"kind": "etcd-wait",
		}).Info("wait for key is over")
	}

	if definedPostgres(c) && definedChadoUser(c) {
		dsn := getPostgresDsn(c)
		mi, err := exec.LookPath("modware-import")
		if err != nil {
			log.WithFields(log.Fields{
				"type": "binary-lookup",
				"name": "modware-import",
			}).Fatal(err)
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
			log.WithFields(log.Fields{
				"type":        "organism-loader",
				"kind":        "loading-issue",
				"status":      string(out),
				"commandline": strings.Join(cmdline, " "),
			}).Fatal(err)
		}
		log.WithFields(log.Fields{
			"type":        "organism-loader",
			"kind":        "loading-success",
			"commandline": strings.Join(cmdline, " "),
		}).Info("organism data loaded successfully")

		if hasEtcd(c) {
			if err := registerWithEtcd(c.String("key-register"), c); err != nil {
				log.WithFields(log.Fields{
					"type": "organism-loader",
					"kind": "etcd-register",
					"key":  c.String("key-register"),
				}).Fatal(err)
			}
			log.WithFields(log.Fields{
				"type": "organism-loader",
				"kind": "etcd-register",
				"key":  c.String("key-register"),
			}).Info("register with etcd")
		}
	} else {
		log.WithFields(log.Fields{
			"type": "organism-loader",
			"kind": "no database information",
		}).Warn("could not load organism data")
	}
}

func OntologiesAction(c *cli.Context) {
	if hasEtcd(c) {
		// watch for download key
		log.WithFields(log.Fields{
			"type": "ontology-loader",
			"key":  c.String("key-download"),
			"kind": "etcd-wait",
		}).Info("going to wait for key")

		if err := waitForEtcd(c.String("key-download"), c); err != nil {
			log.WithFields(log.Fields{
				"type": "ontology-loader",
				"key":  c.String("key-download"),
				"kind": "etcd-wait",
			}).Fatal(err)
		}
		log.WithFields(log.Fields{
			"type": "ontology-loader",
			"key":  c.String("key-download"),
			"kind": "etcd-wait",
		}).Info("wait for key is over")

		// watch for sqitch deploy key
		log.WithFields(log.Fields{
			"type": "ontology-loader",
			"key":  c.GlobalString("key-watch"),
			"kind": "etcd-wait",
		}).Info("going to wait for key")
		if err := waitForEtcd(c.GlobalString("key-watch"), c); err != nil {
			log.WithFields(log.Fields{
				"type": "ontology-loader",
				"key":  c.GlobalString("key-watch"),
				"kind": "etcd-wait",
			}).Fatal(err)
		}
		log.WithFields(log.Fields{
			"type": "ontology-loader",
			"key":  c.GlobalString("key-watch"),
			"kind": "etcd-wait",
		}).Info("wait for key is over")
	}

	if definedPostgres(c) && definedChadoUser(c) {
		dsn := getPostgresDsn(c)
		ml, err := exec.LookPath("modware-load")
		if err != nil {
			log.WithFields(log.Fields{
				"type": "binary-lookup",
				"name": "modware-load",
			}).Fatal(err)
		}
		// load cv_property.obo for versioning
		pcmd := []string{
			"adhocobo2chado",
			"--dsn",
			dsn,
			"--user",
			c.GlobalString("chado-user"),
			"--password",
			c.GlobalString("chado-pass"),
			"--log_level",
			"debug",
			"--include_metadata",
			"--input",
			filepath.Join(c.String("folder"), "cv_property.obo"),
		}
		out, err := exec.Command(ml, pcmd...).CombinedOutput()
		if err != nil {
			log.WithFields(log.Fields{
				"type":        "adhocobo2chado-loader",
				"kind":        "loading-issue",
				"status":      string(out),
				"file":        "cv_property.obo",
				"commandline": strings.Join(pcmd, " "),
			}).Fatal(err)
		}
		log.WithFields(log.Fields{
			"type":        "adhocobo2chado-loader",
			"kind":        "loading-success",
			"status":      string(out),
			"file":        "cv_property.obo",
			"commandline": strings.Join(pcmd, " "),
		}).Info("ontology loaded successfully")

		// Now read all obo files from the directory
		// and load them one by one
		dir, err := ioutil.ReadDir(c.String("folder"))
		if err != nil {
			log.WithFields(log.Fields{
				"type":      "read ontology directory",
				"kind":      "error reading",
				"directory": c.String("folder"),
			}).Fatal(err)
		}
		for _, e := range dir {
			if !e.IsDir() && strings.HasSuffix(e.Name(), "obo") {
				if e.Name() == "cv_property.obo" {
					continue
				}
				ocmd := []string{
					"obo2chado",
					"--dsn",
					dsn,
					"--user",
					c.GlobalString("chado-user"),
					"--password",
					c.GlobalString("chado-pass"),
					"--log_level",
					"debug",
					"--input",
					filepath.Join(c.String("folder"), e.Name()),
				}
				out, err := exec.Command(ml, ocmd...).CombinedOutput()
				if err != nil {
					log.WithFields(log.Fields{
						"type":        "obo2chado-loader",
						"kind":        "loading-issue",
						"status":      string(out),
						"file":        e.Name(),
						"commandline": strings.Join(ocmd, " "),
					}).Fatal(err)
				}
				log.WithFields(log.Fields{
					"type":        "obo2chado-loader",
					"kind":        "loading-success",
					"status":      string(out),
					"file":        e.Name(),
					"commandline": strings.Join(ocmd, " "),
				}).Info("ontology loaded successfully")
			}
		}

	} else {
		log.WithFields(log.Fields{
			"type": "ontology-loader",
			"kind": "no database information",
		}).Warn("could not load ontologies")
	}

}

func GenomesAction(c *cli.Context) {

}

func GenomeAnnoAction(c *cli.Context) {

}

func LiteratureAction(c *cli.Context) {

}

func ScAction(c *cli.Context) {

}
