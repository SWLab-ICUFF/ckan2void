# CKAN to VoID

Overview
--------
Despite the fact that extensive list of open datasets are available in catalogues, most of the data publishers still connects their datasets to other popular datasets, such as DBpedia5, Freebase 6 and Geonames7. Although the linkage with popular datasets would allow us to explore external resources, it would fail to cover highly specialized information. Catalogues of linked data describe the content of datasets in terms of the update periodicity, authors, SPARQL endpoints, linksets with other datasets, amongst others, as recommended by W3C VoID Vocabulary. However, catalogues by themselves do not provide any explicit information to help the URI linkage process.

Searching techniques can rank available datasets <i>S<sub>i</sub></i> according to the probability that it will be possible to define links between URIs of <i>S<sub>i</sub></i> and a given dataset <i>T</i> to be published, so that most of the links, if not all, could be found by inspecting the most relevant datasets in the ranking.

This dataset provides dataset descriptions using the VoID vocabulary and linkage references for supporting the evaluation of searching techniques for dataset linkage. The descriptions contain linksets, classes, properties and topic categories harvested from the Datahub catalogue, dataset dumps and void files and were enriched with topic categories from DBpedia. The enrichments used the DBpedia Spotlight to detect entities in textual literals and to retrieve the classification categories of each entity.

Requirements
------------


Installing
----------


Usage Example
-------------