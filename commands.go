package main

import (
	"os"

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
