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
			EnvVar: "CHADO_BACKEND_SERVICE_HOST",
			Usage:  "postgresql host",
		},
		cli.StringFlag{
			Name:   "pgport",
			EnvVar: "CHADO_BACKEND_SERVICE_PORT",
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
		cli.BoolFlag{
			Name:   "use-logfile",
			EnvVar: "USE_LOG_FILE",
			Usage:  "Instead of stderr, write the script(s) log to a file",
		},
		cli.StringFlag{
			Name:   "remote-log-path",
			EnvVar: "REMOTE_LOG_PATH",
			Value:  "log/import-log.zip",
			Usage:  "full path(relative to the bucket) of s3 object where all import log will be uploaded",
		},
		cli.StringFlag{
			Name:   "local-log-path",
			EnvVar: "LOCAL_LOG_PATH",
			Value:  "/log",
			Usage:  "local log folder",
		},
		cli.BoolFlag{
			Name:   "upload-logfile",
			EnvVar: "UPLOAD_LOG_FILE",
			Usage:  "upload all log files(compressed) to a s3 bucket",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:   "organism",
			Usage:  "Import organism",
			Action: OrganismAction,
			Before: validateOrganism,
		},
		{
			Name:   "organism-plus",
			Usage:  "Import additional organisms tied to stocks in stock center",
			Action: OrganismPlusAction,
			Before: validateOrganismPlus,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "remote-path, rp",
					Usage: "full path(relative to the bucket) of s3 object which will be download",
					Value: "import/strain_strain.tsv",
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
			Before: validateLiterature,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "remote-path, rp",
					Usage: "full path(relative to the bucket) of s3 object which will be download",
					Value: "import/literature.tar.gz",
				},
			},
		},
		{
			Name:   "stock-center",
			Usage:  "Import all data related to stock center",
			Action: ScAction,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "remote-path, rp",
					Usage: "full path(relative to the bucket) of s3 object which will be download",
					Value: "import/stockcenter.tar.gz",
				},
			},
		},
	}
	app.Run(os.Args)
}
