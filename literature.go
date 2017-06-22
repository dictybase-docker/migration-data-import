package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/sirupsen/logrus"

	cli "gopkg.in/urfave/cli.v1"
)

type cmdFeedback struct {
	Error  error
	Output []byte
	SubCmd string
}

func LiteratureAction(c *cli.Context) error {
	log := getLogger(c)
	mi, err := exec.LookPath("modware-import")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "binary-lookup",
			"name": "modware-import",
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}

	// fetch the literature archive file(from cloud storage)
	filename, err := fetchRemoteFile(c, "literature")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "remote-get",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("unable to fetch remote file %s ", err), 2)
	}
	tmpDir, err := ioutil.TempDir(os.TempDir(), "litreature")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "temp-dir",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("unable to create temp directory %s", err), 2)
	}
	err = untar(filename, tmpDir)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "untar",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("error in untarring file %s", err), 2)
	}
	litcmd, err := makeLitImportCmd(c)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "command-string",
			"name": "bibtex2chado",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("error generating command line %s", err), 2)
	}
	files, err := listFiles(tmpDir)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "dir-lookup",
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}

	ch := make(chan cmdFeedback, len(files))
	for _, f := range files {
		runLitCmd(mi, append(litcmd, f), ch)
	}
	for i := 0; i < len(files); i++ {
		fback := <-ch
		if fback.Error != nil {
			log.WithFields(logrus.Fields{
				"type":    mi,
				"output":  string(fback.Output),
				"command": fback.SubCmd,
			}).Error(err)
			return cli.NewExitError(fmt.Sprintf("Error type %s in loading %s", fback.Error.Error(), string(fback.Output)), 2)
		}
		log.WithFields(logrus.Fields{
			"type":    mi,
			"output":  string(fback.Output),
			"command": fback.SubCmd,
		}).Info("loading success")
	}
	return nil
}

func runLitCmd(cmd string, subCmd []string, wch chan<- cmdFeedback) {
	fb := cmdFeedback{}
	out, err := exec.Command(cmd, subCmd...).CombinedOutput()
	if err != nil {
		fb.Error = err
		fb.Output = out
	} else {
		fb.Output = out
	}
	fb.SubCmd = strings.Join(subCmd, " ")
	wch <- fb
}

func makeLitImportCmd(c *cli.Context) ([]string, error) {
	cmd := []string{
		"bibtex2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
		"--use_extended_layout",
	}

	if c.GlobalBool("use-log-file") {
		logf, err := getLogFileName(c, "bibtex2chado")
		if err != nil {
			return cmd, err
		}
		cmd = append(cmd, "--logfile", logf)
	}
	return append(cmd, "--input"), nil
}
