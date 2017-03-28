package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/urfave/cli.v1"
)

func ScAction(c *cli.Context) error {
	s3Client, err := getS3Client(c)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	// create the input folder/path if it does not exist
	err = os.MkdirAll(c.String("input"), 0775)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	tmpf, err := ioutil.TempFile("/tmp", "dsc")
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	defer os.Remove(tmpf.Name())
	if err := s3Client.FGetObject(c.GlobalString("s3-bucket"), c.String("remote-path"), tmpf.Name()); err != nil {
		return cli.NewExitError(fmt.Sprintf("Unable to fget the object %s", err.Error()), 2)
	}
	if err := untar(tmpf.Name(), c.String("input")); err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	//if !definedPostgres(c) || !definedChadoUser(c) {
	//log.WithFields(log.Fields{
	//"type": "stock-loader",
	//"kind": "no database information",
	//}).Error("could not load stock center data")
	//return cli.NewExitError("could not load stock center data", 2)
	//}
	//mi, err := exec.LookPath("modware-import")
	//if err != nil {
	//log.WithFields(log.Fields{
	//"type": "binary-lookup",
	//"name": "modware-import",
	//}).Error(err)
	//return cli.NewExitError(err.Error(), 2)
	//}
	return nil
}
