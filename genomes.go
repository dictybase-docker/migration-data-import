package main

import (
	"gopkg.in/urfave/cli.v1"
)

func GenomesAction(c *cli.Context) error {
	//if hasEtcd(c) {
	//key := c.String("key-watch")
	//if err := waitForEtcd(key, c); err != nil {
	//log.WithFields(log.Fields{
	//"type": "genome-loader",
	//"key":  key,
	//"kind": "etcd-wait",
	//}).Error(err)
	//return cli.NewExitError(err.Error(), 2)
	//}
	//log.WithFields(log.Fields{
	//"type": "genome-loader",
	//"key":  key,
	//"kind": "etcd-wait",
	//}).Info("wait for key is over")
	//}
	//if !definedPostgres(c) {
	//log.WithFields(log.Fields{
	//"type": "genome-loader",
	//"kind": "command-options",
	//}).Error("postgres options are not defined")
	//return cli.NewExitError("postgres options are not defined", 2)
	//}
	//if !definedChadoUser(c) {
	//log.WithFields(log.Fields{
	//"type": "genome-loader",
	//"kind": "command-options",
	//}).Error("chado database options are not defined")
	//return cli.NewExitError("chado database options are not defined", 2)
	//}

	//dsn := getPostgresDsn(c)
	//ml, err := exec.LookPath("modware-load")
	//if err != nil {
	//log.WithFields(log.Fields{
	//"type": "binary-lookup",
	//"name": "modware-load",
	//}).Error(err)
	//return cli.NewExitError(err.Error(), 2)
	//}
	//// basic command line options for all genomes
	//gcmd := []string{
	//"adhocobo2chado",
	//"--dsn",
	//dsn,
	//"--user",
	//c.GlobalString("chado-user"),
	//"--password",
	//c.GlobalString("chado-pass"),
	//"--log_level",
	//"debug",
	//"--include_metadata",
	//"--input",
	//filepath.Join(c.String("folder"), "cv_property.obo"),
	//}
	return nil
}

func GenomeAnnoAction(c *cli.Context) error {
	return nil

}
