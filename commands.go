package main

import (
	"fmt"
	"os"
	"regexp"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"gopkg.in/codegangsta/cli.v1"
)

func main() {
	app := cli.NewApp()
	app.Name = "import"
	app.Usage = "cli for various import subcommands"
	app.Version = "1.0.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "etcd-host",
			EnvVar: "ETCD_CLIENT_SERVICE_HOST",
			Usage:  "ip address of etcd instance",
		},
		cli.StringFlag{
			Name:   "etcd-port",
			EnvVar: "ETCD_CLIENT_SERVICE_PORT",
			Usage:  "port number of etcd instance",
		},
		cli.StringFlag{
			Name:   "chado-pass",
			EnvVar: "CHADO_PASS",
			Usage:  "chado database password",
		},
		cli.StringFlag{
			Name:   "chado-db",
			EnvVar: "CHADO_DB",
			Usage:  "chado database name",
		},
		cli.StringFlag{
			Name:   "chado-user",
			EnvVar: "CHADO_USER",
			Usage:  "chado database user",
		},
		cli.StringFlag{
			Name:   "pghost",
			EnvVar: "POSTGRES_SERVICE_HOST",
			Usage:  "postgresql host",
		},
		cli.StringFlag{
			Name:   "pgport",
			EnvVar: "POSTGRES_SERVICE_PORT",
			Usage:  "postgresql port",
		},
		cli.StringFlag{
			Name:  "key-watch",
			Usage: "key to watch before start loading",
			Value: "/migration/sqitch",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:   "organism",
			Usage:  "Import organism",
			Action: OrganismAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "key-register",
					Usage: "key to register after loading organism",
					Value: "/migration/organism",
				},
			},
		},
		{
			Name:   "ontologies",
			Usage:  "Import all ontologies",
			Action: OntologiesAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "folder",
					Usage: "data folder",
					Value: "/data/ontology",
				},
				cli.StringFlag{
					Name:  "key-register",
					Usage: "key to register after loading ontologies",
					Value: "/migration/ontology",
				},
				cli.StringFlag{
					Name:  "key-download",
					Usage: "key to watch for download of ontologies",
					Value: "/migration/download",
				},
			},
		},
		{
			Name:   "genomes",
			Usage:  "Import all genomes",
			Action: GenomesAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "folder",
					Usage: "data folder",
					Value: "/data/stockcenter",
				},
			},
		},
		{
			Name:   "genome-annotations",
			Usage:  "Import all genome annotations",
			Action: GenomeAnnoAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "folder",
					Usage: "data folder",
					Value: "/data/stockcenter",
				},
			},
		},
		{
			Name:   "literature",
			Usage:  "Import literature",
			Action: LiteratureAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "folder",
					Usage: "data folder",
					Value: "/data/stockcenter",
				},
			},
		},
		{
			Name:   "stock-center",
			Usage:  "Import all data related to stock center",
			Action: ScAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "folder",
					Usage: "data folder",
					Value: "/data/stockcenter",
				},
			},
		},
	}
	app.Run(os.Args)
}

func hasEtcd(c *cli.Context) bool {
	if len(c.GlobalString("etcd-host")) > 1 && len(c.GlobalString("etcd-port")) > 1 {
		return true
	}
	return false
}

func getEtcdURL(c *cli.Context) string {
	return "http://" + c.GlobalString("etcd-host") + ":" + c.GlobalString("etcd-port")
}

func getEtcdAPIHandler(c *cli.Context) (client.KeysAPI, error) {
	cfg := client.Config{
		Endpoints: []string{getEtcdURL(c)},
		Transport: client.DefaultTransport,
	}
	cl, err := client.New(cfg)
	if err != nil {
		return nil, err
	}
	return client.NewKeysAPI(cl), nil
}

func waitForEtcd(key string, c *cli.Context) error {
	api, err := getEtcdAPIHandler(c)
	if err != nil {
		return err
	}
	_, err = api.Get(context.Background(), key, nil)
	if err != nil {
		if m, _ := regexp.MatchString("100", err.Error()); m {
			// key is not present have to watch it
			w := api.Watcher(c.String("key-watch"), nil)
			_, err := w.Next(context.Background())
			if err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	}
	// key is already present
	return nil
}

func registerWithEtcd(key string, c *cli.Context) error {
	api, err := getEtcdAPIHandler(c)
	if err != nil {
		return err
	}
	_, err = api.Create(context.Background(), key, "complete")
	if err != nil {
		return err
	}
	return nil
}

func definedPostgres(c *cli.Context) bool {
	if len(c.GlobalString("pghost")) > 1 && len(c.GlobalString("pgport")) > 1 {
		return true
	}
	return false
}

func definedChadoUser(c *cli.Context) bool {
	if len(c.GlobalString("chado-user")) > 1 && len(c.GlobalString("chado-db")) > 1 && len(c.GlobalString("chado-pass")) > 1 {
		return true
	}
	return false
}

func getPostgresDsn(c *cli.Context) string {
	return fmt.Sprintf("dbi:Pg:host=%s;port=%s;database=%s", c.GlobalString("pghost"),
		c.GlobalString("pgport"), c.GlobalString("chado-db"))
}
