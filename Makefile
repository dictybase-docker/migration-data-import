load-organism:
	app organism
load-organism-plus: load-organism
	app organism-plus
load-so:
	app onto --purl --obo so 
load-ro:
	app onto --gh --obo ro-chado 
load-dicty-ontologies:
	app onto --gh --obo dictyBase_literature --obo dicty_anatomy --obo dicty_assay \
		--obo dicty_environment --obo dicty_genetic_modifiction --obo dicty_mutagenesis_method \
		--obo dicty_phenotypes --obo dicty_plasmid_inventory --obo dicty_plasmid_keywords \ 
		--obo dicty_storage_condition --obo dicty_strain_characteristics --obo dicty_strain_inventory 

load-literature:
	app literature

load-ontologies: load-so load-ro load-dicty-ontologies

load-all: load-organism-plus load-ontologies load-liteature

