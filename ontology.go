package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"

	"gopkg.in/jackc/pgx.v2"
	"gopkg.in/urfave/cli.v1"

	"github.com/Sirupsen/logrus"
	"github.com/google/go-github/github"
)

const (
	repository   = "migration-data"
	roRepository = "obo-relations"
	owner        = "dictyBase"
	basePath     = "ontologies"
	roBasePath   = "subsets"
	purlBase     = "http://purl.obolibrary.org/obo"
)

type Obo struct {
	Ontologies []string `json:"ontologies"`
}

type OntoFile struct {
	Content string
	Error   error
	Name    string
}

type contentFn func(string, chan<- *OntoFile)

func validateOnto(c *cli.Context) error {
	if err := validateArgs(c); err != nil {
		return err
	}
	if len(c.StringSlice("obo")) == 0 {
		cli.NewExitError("no obo file given", 2)
	}
	return nil
}

func ontoAction(c *cli.Context) error {
	log := getLogger(c)
	conn, err := getConnection(c)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	defer conn.Close()
	// Check if cvprop is loaded
	cvp, err := iscvPropLoaded(conn)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	// download obo files
	dir, err := oboDownload(cvp, c)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	ml, err := exec.LookPath("modware-load")
	if err != nil {
		log.WithFields(logrus.Fields{
			"type": "binary-lookup",
			"name": "modware-load",
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	// load cv_property.obo for versioning
	if !cvp {
		acmd, err := makeadhocOboCmd(c)
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("unable to generate command %s", err), 2)
		}
		pcmd := append(acmd, filepath.Join(dir, "cv_property.obo"))
		out, err := exec.Command(ml, pcmd...).CombinedOutput()
		if err != nil {
			log.WithFields(logrus.Fields{
				"type":        "adhocobo2chado-loader",
				"kind":        "loading-issue",
				"status":      string(out),
				"file":        "cv_property.obo",
				"commandline": strings.Join(pcmd, " "),
			}).Error(err)
			return cli.NewExitError(err.Error(), 2)
		}
		log.WithFields(logrus.Fields{
			"type":        "adhocobo2chado-loader",
			"kind":        "loading-success",
			"status":      string(out),
			"file":        "cv_property.obo",
			"commandline": strings.Join(pcmd, " "),
		}).Info("ontology loaded successfully")
	}

	// Now the other obo files
	reader, err := ioutil.ReadDir(dir)
	if err != nil {
		log.WithFields(logrus.Fields{
			"type":      "read ontology directory",
			"kind":      "error reading",
			"directory": dir,
		}).Error(err)
		return cli.NewExitError(err.Error(), 2)
	}
	obocmd, err := makeOboCmd(c)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("unable to generate command %s", err), 2)
	}
	for _, obo := range reader {
		if obo.IsDir() {
			continue
		}
		pcmd := append(obocmd, filepath.Join(dir, obo.Name()))
		out, err := exec.Command(ml, pcmd...).CombinedOutput()
		if err != nil {
			log.WithFields(logrus.Fields{
				"type":        "obo2chado loader",
				"kind":        "loading-issue",
				"status":      string(out),
				"file":        obo.Name(),
				"commandline": strings.Join(pcmd, " "),
			}).Error(err)
			continue
		}
		log.WithFields(logrus.Fields{
			"type":        "obo2chado loader",
			"kind":        "loading-success",
			"status":      string(out),
			"file":        obo.Name(),
			"commandline": strings.Join(pcmd, " "),
		}).Info("ontology loaded successfully")
	}
	return nil
}

func oboDownload(cvp bool, c *cli.Context) (string, error) {
	allObos := make([]string, 0)
	if cvp {
		allObos = append(allObos, c.StringSlice("obo")...)
	} else {
		allObos = append(allObos, "cv_property")
		allObos = append(allObos, c.StringSlice("obo")...)
	}
	dir, err := ioutil.TempDir("", "obo")
	if err != nil {
		return dir, err
	}
	//defer os.RemoveAll(dir)
	var fn contentFn
	switch {
	case c.Bool("purl"):
		fn = purlContent
	case c.Bool("github"):
		fn = githubContent
	default:
		return dir, fmt.Errorf("either of %s or %s download source has to be selected", "purl", "github")
	}
	ch := make(chan *OntoFile, len(allObos))
	for _, n := range allObos {
		if n == "cv_property" {
			go githubContent(fmt.Sprintf("%s.obo", n), ch)
			continue
		}
		go fn(fmt.Sprintf("%s.obo", n), ch)
	}
	for i := 0; i < len(allObos); i++ {
		file := <-ch
		if file.Error != nil {
			return dir, file.Error
		}
		err := ioutil.WriteFile(
			filepath.Join(dir, file.Name),
			[]byte(file.Content),
			0644,
		)
		if err != nil {
			return dir, err
		}
	}
	return dir, nil
}

func purlContent(name string, ch chan<- *OntoFile) {
	url := fmt.Sprintf("%s/%s", purlBase, name)
	res, err := http.Get(url)
	if err != nil {
		ch <- &OntoFile{Error: err, Name: name}
	} else {
		ct, err := ioutil.ReadAll(res.Body)
		if err != nil {
			ch <- &OntoFile{Error: err, Name: name}
		} else {
			ch <- &OntoFile{Name: name, Content: string(ct)}
		}
	}
}

func getRepository(name string) string {
	if name == "ro-chado.obo" {
		return roRepository
	}
	return repository
}

func getRepoPath(name string) string {
	if name == "ro-chado.obo" {
		return fmt.Sprintf("%s/%s", roBasePath, name)
	}
	return fmt.Sprintf("%s/%s", basePath, name)
}

func githubContent(name string, ch chan<- *OntoFile) {
	client := github.NewClient(nil)
	ct, _, _, err := client.Repositories.GetContents(
		context.Background(),
		owner,
		getRepository(name),
		getRepoPath(name),
		nil,
	)
	if err != nil {
		ch <- &OntoFile{Error: err}
	} else {
		data, err := ct.GetContent()
		if err != nil {
			ch <- &OntoFile{Error: err}
		} else {
			ch <- &OntoFile{Content: data, Name: name}
		}
	}
}

func iscvPropLoaded(conn *pgx.Conn) (bool, error) {
	var id int
	err := conn.QueryRow("SELECT cv_id FROM cv WHERE cv.name=$1", "cv_property").Scan(&id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func makeadhocOboCmd(c *cli.Context) ([]string, error) {
	cmd := []string{
		"adhocobo2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
		"--include_metadata",
	}
	if c.GlobalBool("use-log-file") {
		logf, err := getLogFileName(c, "adhocobo2chado")
		if err != nil {
			return cmd, err
		}
		cmd = append(cmd, "--logfile", logf)
	}
	return append(cmd, "--input"), nil
}

func makeOboCmd(c *cli.Context) ([]string, error) {
	cmd := []string{
		"obo2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
	}
	if c.GlobalBool("use-log-file") {
		logf, err := getLogFileName(c, "obo2chado")
		if err != nil {
			return cmd, err
		}
		cmd = append(cmd, "--logfile", logf)
	}
	return append(cmd, "--input"), nil
}
