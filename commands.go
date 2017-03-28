package main

import (
	"os"

	"gopkg.in/urfave/cli.v1"
)

func main() {
	app := cli.NewApp()
	app.Name = "import"
	app.Usage = "cli for various import subcommands"
	app.Version = "1.0.0"
	app.Flags = []cli.Flag{
		cli.StringSliceFlag{
			Name:  "hooks",
			Usage: "hook names for sending log in addition to stderr",
			Value: &cli.StringSlice{},
		},
		cli.StringFlag{
			Name:   "slack-channel",
			EnvVar: "SLACK_CHANNEL",
			Usage:  "Slack channel where the log will be posted",
		},
		cli.StringFlag{
			Name:   "slack-url",
			EnvVar: "SLACK_URL",
			Usage:  "Slack webhook url[required if slack channel is provided]",
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
			Name:  "s3-server",
			Usage: "S3 server endpoint",
			Value: "storage.googleapis.com",
		},
		cli.StringFlag{
			Name:  "s3-bucket",
			Usage: "S3 bucket where the import data is kept",
			Value: "dictybase",
		},
		cli.StringFlag{
			Name:   "access-key, akey",
			EnvVar: "S3_ACCESS_KEY",
			Usage:  "access key for S3 server, required based on command run",
		},
		cli.StringFlag{
			Name:   "secret-key, skey",
			EnvVar: "S3_SECRET_KEY",
			Usage:  "secret key for S3 server, required based on command run",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:   "organism",
			Usage:  "Import organism",
			Action: OrganismAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "notify-channel",
					Usage: "The postgresql channel to send notification after successful completion of loading",
					Value: "organism-plus",
				},
				cli.StringFlag{
					Name:  "payload",
					Usage: "The payload for notification",
					Value: "loaded",
				},
			},
		},
		{
			Name:   "organism-plus",
			Usage:  "Import additional organisms tied to stocks in stock center",
			Action: OrganismPlusAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "input, i",
					Usage: "full path where the data for import will be available",
					Value: "/data/organism",
				},
				cli.StringFlag{
					Name:  "remote-path, rp",
					Usage: "full path(relative to the bucket) of s3 object which will be download",
					Value: "import/strain_strain.tsv",
				},
				cli.StringFlag{
					Name:  "notify-channel",
					Usage: "The postgresql channel to send notification after successful completion of loading",
					Value: "organism-plus",
				},
				cli.StringFlag{
					Name:  "payload",
					Usage: "The payload for notification",
					Value: "loaded",
				},
				cli.StringFlag{
					Name:  "listen-channel",
					Usage: "The postgresql channel to listen before start loading",
					Value: "organism",
				},
			},
		},
		{
			Name:   "onto",
			Usage:  "Import one or more ontologies",
			Action: ontoAction,
			Flags: []cli.Flag{
				cli.BoolFlag{
					Name:  "github, gh",
					Usage: "Flag to retrieve all ontology files from dictybase github repo",
				},
				cli.BoolFlag{
					Name:  "purl",
					Usage: "Flag to retrieve all ontology files using purl url",
				},
				cli.StringSliceFlag{
					Name:  "obo",
					Usage: "Name on ontologies to load",
					Value: &cli.StringSlice{},
				},
				cli.StringFlag{
					Name:  "notify-channel",
					Usage: "The postgresql channel to send notification after successful completion of loading",
					Value: "ontology",
				},
				cli.StringFlag{
					Name:  "payload",
					Usage: "The payload for notification",
					Value: "loaded",
				},
			},
			Before: validateOnto,
		},
		{
			Name:   "genomes",
			Usage:  "Import all genomes",
			Action: GenomesAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "folder",
					Usage: "data folder",
					Value: "/data/gff3",
				},
				cli.StringFlag{
					Name:  "key-watch",
					Usage: "key to watch before loading genomes",
					Value: "/migration/ontology",
				},
				cli.StringFlag{
					Name:  "key-register",
					Usage: "key to register after loading genomes",
					Value: "/migration/genomes",
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
					Name:  "input, i",
					Usage: "full path where the data for import will be available",
					Value: "/data/stockcenter",
				},
				cli.StringFlag{
					Name:  "remote-path, rp",
					Usage: "full path(relative to the bucket) of s3 object which will be download",
					Value: "import/stockcenter.tar.gz",
				},
			},
		},
	}
	app.Before = validateArgs
	app.Run(os.Args)
}
