package main

import (
	"fmt"
	"io/ioutil"
	"os"

	cli "gopkg.in/urfave/cli.v1"
)

func validateLiterature(c *cli.Context) error {
	if err := validateArgs(c); err != nil {
		return err
	}
	if err := validateS3Args(c); err != nil {
		return err
	}
	return nil
}

func LiteratureAction(c *cli.Context) error {
	// fetch the literature archive file(from cloud storage)
	filename, err := fetchRemoteFile(c, "organism")
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("unable to fetch remote file %s ", err), 2)
	}
	tmpf, err := ioutil.TempDir(os.TempDir(), "organism")
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("unable to create temp directory %s", err), 2)
	}
	err = untar(filename, tmpf)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("error in untarring file %s", err), 2)
	}
	return nil
}
