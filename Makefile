load-organism:
	app organism

load-organism-plus: load-organism
	app organism-plus

load-so:
	app --use-logfile onto --purl  --obo so 

load-ro:
	app --use-logfile onto --gh  --obo ro-chado 

load-dicty-ontologies:
	app --use-logfile onto --gh --obo dictyBase_literature_topic --obo dicty_anatomy --obo dicty_assay \
		--obo dicty_environment --obo dicty_genetic_modification --obo dicty_mutagenesis_method \
		--obo dicty_phenotypes --obo dicty_plasmid_inventory --obo dicty_plasmid_keywords \
		--obo dicty_storage_condition --obo dicty_strain_characteristics --obo dicty_strain_inventory

load-literature:
	app --use-logfile literature 

load-stocks:
	app --use-logfile stock-center

load-users:
	app --use-logfile users

tag-inventory:
	app --use-logfile tag-inventory

load-orders:
	app --use-logfile stock-center-orders

generate-plasmid-prefix:
	app --use-logfile plasmid-prefix

generate-bacterial-strain:
	app --use-logfile bacterial-strain

load-annotation-assignments:
	app --use-logfile annotation-assignments

load-dsc: load-stocks load-users load-orders tag-inventory generate-plasmid-prefix generate-bacterial-strain load-annotation-assignments

upload-log:
	app upload-log

load-ontologies: load-so load-ro load-dicty-ontologies

load-all: load-organism-plus load-ontologies load-literature load-dsc upload-log

