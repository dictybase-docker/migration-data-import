package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/urfave/cli.v1"
)

var strainData []string = []string{
	"characteristics",
	"publications",
	"inventory",
	"props",
	"parent",
	"genotype",
}

var plasmidData []string = []string{
	"publications",
	"props",
	"inventory",
	"images",
}

type cmdFunc func(*cli.Context, string, string, *logrus.Logger) error

func ScAction(c *cli.Context) error {
	log := getLogger(c)
	mi, err := exec.LookPath("modware-import")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "binary-lookup",
			"name": "modware-import",
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.Debugf("successfully looked up command %s", mi)

	filename, err := fetchRemoteFile(c, "dsc")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "remote-get",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("unable to fetch remote file %s ", err), 2)
	}
	log.Infof("retrieved the remote file %s", filename)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "dsc")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "temp-dir",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("unable to create temp directory %s", err), 2)
	}
	log.Debugf("create a temp folder %s", tmpDir)

	err = untar(filename, tmpDir)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "untar",
			"name": "input",
		}).Error(err)
		return cli.NewExitError(fmt.Sprintf("error in untarring file %s", err), 2)
	}
	log.Debugf("untar file %s in %s temp folder", filename, tmpDir)

	allfuncs := []cmdFunc{runStrainImport, runPlasmidImport, runStrainPlasmidImport}
	for _, cf := range allfuncs {
		if err := cf(c, tmpDir, mi, log); err != nil {
			return cli.NewExitError(err.Error(), 2)
		}
	}
	return nil
}

func runStrainImport(c *cli.Context, tmpDir string, mainCmd string, log *logrus.Logger) error {
	cmd := makeStrainImportCmd(c, tmpDir)
	for i, data := range strainData {
		rcmd := make([]string, len(cmd))
		copy(rcmd, cmd)
		rcmd = append(rcmd, data)
		if i == 0 && c.Bool("prune") {
			rcmd = append(rcmd, "--prune")
		}
		if c.GlobalBool("use-logfile") {
			logf, err := getLogFileName(c, data)
			if err != nil {
				log.WithFields(logrus.Fields{
					"type": "logfile",
					"name": "name-generation",
				}).Error(err)
				return cli.NewExitError(err.Error(), 2)
			}
			log.Debugf("logfile %s for data %s", logf, data)
			rcmd = append(rcmd, "--logfile", logf)
		}
		out, err := exec.Command(mainCmd, rcmd...).CombinedOutput()
		if err != nil {
			log.WithFields(logrus.Fields{
				"type":        "modware-import",
				"name":        "dictystrain2chado",
				"status":      string(out),
				"data":        data,
				"commandline": strings.Join(rcmd, " "),
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Infof("successfully ran command %s", strings.Join(rcmd, " "))
	}
	pcmd := make([]string, len(cmd))
	copy(pcmd, cmd)
	pcmd = append(pcmd, "phenotype", "--dsc_phenotypes", filepath.Join(tmpDir, "DSC_phenotypes_import.tsv"))
	if c.GlobalBool("use-log-file") {
		logf, err := getLogFileName(c, "phenotype")
		if err != nil {
			log.WithFields(logrus.Fields{
				"type": "logfile",
				"name": "name-generation",
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Debugf("logfile %s for data phenotype", logf)
		pcmd = append(pcmd, "--logfile", logf)
	}
	out, err := exec.Command(mainCmd, pcmd...).CombinedOutput()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":        "modware-import",
			"name":        "dictystrain2chado",
			"data":        "phenotype",
			"status":      string(out),
			"commandline": strings.Join(pcmd, " "),
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.Infof("successfully ran command %s", strings.Join(pcmd, " "))
	return nil
}

func runStrainPlasmidImport(c *cli.Context, tmpDir string, mainCmd string, log *logrus.Logger) error {
	cmd := makeStrainImportCmd(c, tmpDir)
	spcmd := make([]string, len(cmd))
	copy(spcmd, cmd)
	spcmd = append(spcmd, "plasmid")
	out, err := exec.Command(mainCmd, spcmd...).CombinedOutput()
	if c.GlobalBool("use-logfile") {
		logf, err := getLogFileName(c, "strain-plasmid")
		if err != nil {
			log.WithFields(logrus.Fields{
				"type": "logfile",
				"name": "name-generation",
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Debugf("logfile %s for data %s", logf, "plasmid")
		spcmd = append(spcmd, "--logfile", logf)
	}
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":        "modware-import",
			"name":        "dictystrain2chado",
			"data":        "plasmid",
			"status":      string(out),
			"commandline": strings.Join(spcmd, " "),
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.Infof("successfully ran command %s", strings.Join(spcmd, " "))
	return nil
}

func runPlasmidImport(c *cli.Context, tmpDir string, mainCmd string, log *logrus.Logger) error {
	cmd := makePlasmidImportCmd(c, tmpDir)
	for i, data := range plasmidData {
		rcmd := make([]string, len(cmd))
		copy(rcmd, cmd)
		rcmd = append(rcmd, data)
		if i == 0 && c.Bool("prune") {
			rcmd = append(rcmd, "--prune")
		}
		if c.GlobalBool("use-logfile") {
			logf, err := getLogFileName(c, data)
			if err != nil {
				log.WithFields(logrus.Fields{
					"type": "logfile",
					"name": "name-generation",
				}).Error(err)
				return cli.NewExitError(err.Error(), 2)
			}
			log.Debugf("logfile %s for data %s", logf, data)
			rcmd = append(rcmd, "--logfile", logf)
		}
		out, err := exec.Command(mainCmd, rcmd...).CombinedOutput()
		if err != nil {
			log.WithFields(logrus.Fields{
				"type":        "modware-import",
				"name":        "dictyplasmid2chado",
				"status":      string(out),
				"data":        data,
				"commandline": strings.Join(rcmd, " "),
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Infof("successfully ran command %s", strings.Join(rcmd, " "))
	}
	scmd := make([]string, len(cmd))
	copy(scmd, cmd)
	scmd = append(scmd, "sequence", "--seq_data_dir", filepath.Join(tmpDir, "formatted_sequence"))
	if c.GlobalBool("use-logfile") {
		logf, err := getLogFileName(c, "sequence")
		if err != nil {
			log.WithFields(logrus.Fields{
				"type": "logfile",
				"name": "name-generation",
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.Debugf("logfile %s for data %s", logf, "sequence")
		scmd = append(scmd, "--logfile", logf)
	}
	out, err := exec.Command(mainCmd, scmd...).CombinedOutput()
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":        "modware-import",
			"name":        "plasmid2chado",
			"data":        "sequence",
			"status":      string(out),
			"commandline": strings.Join(scmd, " "),
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	log.Infof("successfully ran command %s", strings.Join(scmd, " "))
	return nil
}

func makePlasmidImportCmd(c *cli.Context, folder string) []string {
	return []string{
		"dictyplasmid2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
		"--dir",
		folder,
		"--data",
	}
}

func makeStrainImportCmd(c *cli.Context, folder string) []string {
	return []string{
		"dictystrain2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
		"--dir",
		folder,
		"--data",
	}
}
