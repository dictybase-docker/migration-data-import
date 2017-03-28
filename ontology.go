package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gopkg.in/jackc/pgx.v2"
	"gopkg.in/urfave/cli.v1"

	"github.com/Sirupsen/logrus"
	"github.com/google/go-github/github"
)

const (
	repository = "migration-data"
	owner      = "dictyBase"
	basePath   = "ontology"
	purlBase   = "http://purl.obolibrary.org/obo"
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
	if len(c.StringSlice("obo")) == 0 {
		cli.NewExitError("no obo file given", 2)
	}
	return nil
}

func ontoAction(c *cli.Context) error {
	log := getLogger(c)
	cont, err := json.Marshal(
		&Obo{Ontologies: c.StringSlice("obo")},
	)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
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
	if cvp {
		pcmd := append(makeadhocOboCmd(c), filepath.Join(dir, "cv_property.obo"))
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
	obocmd := makeOboCmd(c)
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
	err = sendNotificationWithConn(conn, c.String("notify-channel"), string(cont))
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	log.WithFields(logrus.Fields{
		"type":    "postgresql notification",
		"channel": c.String("notify-channel"),
	}).Info("send")
	return nil
}

func oboDownload(cvp bool, c *cli.Context) (string, error) {
	var allObos []string
	if cvp {
		allObos = c.StringSlice("obo")
	} else {
		allObos[0] = "cv_property"
		allObos = append(allObos, c.StringSlice("obo")...)
	}
	dir, err := ioutil.TempDir("", "obo")
	if err != nil {
		return dir, err
	}
	defer os.RemoveAll(dir)
	var fn contentFn
	switch {
	case c.Bool("purl"):
		fn = purlContent
	case c.Bool("github"):
		fn = githubContent
	default:
		return dir, fmt.Errorf("either of %s or %s download source has to be selected", "purl", "github")
	}
	ch := make(chan *OntoFile)
	for _, n := range allObos {
		obo := bytes.NewBufferString(n)
		_, err := obo.WriteString("obo")
		if err != nil {
			return dir, err
		}
		fn(obo.String(), ch)
	}
	for i := 0; i < len(c.StringSlice("obo")); i++ {
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
	res, err := http.Get(fmt.Sprintf("%s/%s", purlBase, name))
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

func githubContent(name string, ch chan<- *OntoFile) {
	client := github.NewClient(nil)
	ct, _, _, err := client.Repositories.GetContents(
		context.Background(),
		owner,
		repository,
		fmt.Sprintf("%s/%s", basePath, name),
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

func makeadhocOboCmd(c *cli.Context) []string {
	return []string{
		"adhocobo2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
		"--log_level",
		"debug",
		"--include_metadata",
		"--input",
	}
}

func makeOboCmd(c *cli.Context) []string {
	return []string{
		"ob2chado",
		"--dsn",
		getPostgresDsn(c),
		"--user",
		c.GlobalString("chado-user"),
		"--password",
		c.GlobalString("chado-pass"),
		"--log_level",
		"debug",
		"--input",
	}
}
