# Data import
This is a source repository for [docker](http://docker.io) image to run
all data import tasks of dictybase overhaul project.

## Usage

`Build`

```docker build --rm=true -t dictybase/migration-data-import .```

`Run`

### Command line

```
docker run --rm dictybase/migration-data-import app -h


NAME:
   import - cli for various import subcommands

USAGE:
   import [global options] command [command options] [arguments...]

VERSION:
   1.0.0

COMMANDS:
   organism		Import organism
   ontologies		Import all ontologies
   genomes		Import all genomes
   genome-annotations	Import all genome annotations
   literature		Import literature
   stock-center		Import all data related to stock center
   help, h		Shows a list of commands or help for one command
   
GLOBAL OPTIONS:
   --etcd-host 				ip address of etcd instance [$ETCD_CLIENT_SERVICE_HOST]
   --etcd-port 				port number of etcd instance [$ETCD_CLIENT_SERVICE_PORT]
   --chado-pass 			chado database password [$CHADO_PASS]
   --chado-db 				chado database name [$CHADO_DB]
   --chado-user 			chado database user [$CHADO_USER]
   --pghost 				postgresql host [$POSTGRES_SERVICE_HOST]
   --pgport 				postgresql port [$POSTGRES_SERVICE_PORT]
   --key-watch '/migration/sqitch'	key to watch before start loading
   --help, -h				show help
   --version, -v			print the version
   
```
