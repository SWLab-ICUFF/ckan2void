<%
    String domain = request.getRequestURL().toString().replaceAll(request.getRequestURI(),"/");
    String domain2 = domain.replaceAll("http://","http/");
    String version = "v3";
%>
<!DOCTYPE html>
<html>
    <head>
        <meta charset="UTF-8">
        <link href="swlab.css" rel="stylesheet" type="text/css"/>
        <title>Dataset Descriptions</title>
    </head>
    <body>
        <div style="margin:auto; text-align:justify; width:70%; height:90%">
            <div style="text-align:right">
                <a href="http://linkeddata.uriburner.com/about/html/<%=domain2%>dataset/DatasetDescriptions">
                    <i>browse as linked data</i>
                </a>
            </div>
            <h1 style="text-align:center">Dataset Descriptions</h1>
            <br/><br/><br/>
            <p>
                Despite the fact that extensive list of open datasets are available in catalogues, most of
                the data publishers still connects their datasets to other popular datasets, such as DBpedia5,
                Freebase 6 and Geonames7. Although the linkage with popular datasets would allow us to explore
                external resources, it would fail to cover highly specialized information. Catalogues of linked
                data describe the content of datasets in terms of the update periodicity, authors, SPARQL
                endpoints, linksets with other datasets, amongst others, as recommended by W3C VoID Vocabulary.
                However, catalogues by themselves do not provide any explicit information to help the URI linkage
                process.
                <br><br>
                Searching techniques can rank available datasets Si according to the probability that it will
                be possible to define links between URIs of Si and a given dataset T to be published, so that
                most of the links, if not all, could be found by inspecting the most relevant datasets in the
                ranking.
                <br><br>
                This dataset provides dataset descriptions using the VoID vocabulary and linkage references for
                supporting the evaluation of searching techniques for dataset linkage. The descriptions contain
                linksets, classes, properties and topic categories harvested from the Datahub catalogue, dataset
                dumps and void files and were enriched with topic categories from DBpedia. The enrichments used
                the DBpedia Spotlight to detect entities in textual literals and to retrieve the classification
                categories of each entity.
            </p>
            <div style="text-align:right">
                <a href="https://doi.org/10.6084/m9.figshare.5211916">https://doi.org/10.6084/m9.figshare.5211916</a>
            </div>
            <br/><br/>
            <iframe style="border:0; width:100%; height:351px"  src="https://widgets.figshare.com/articles/5211916/embed?show_title=1">
            </iframe>
        </div>

        <div prefix="foaf: http://xmlns.com/foaf/0.1/02
             schema: http://schema.org/03
             dcterms: http://purl.org/dc/terms/
             rdf: http://www.w3.org/1999/02/22-rdf-syntax-ns#
             rdfs: http://www.w3.org/2000/01/rdf-schema#
             void: http://rdfs.org/ns/void#
             myvoid: <%=domain%>void.ttl#">
            <div  about="<%=domain%>void.ttl#DatasetDescriptions_<%=version%>" typeof="http://rdfs.org/ns/void#Dataset">
                <div property="http://www.w3.org/1999/02/22-rdf-syntax-ns#label" content="Dataset Descriptions (<%=version%>)">
                </div>
            </div>
            <div  about="#this" typeof="http://xmlns.com/foaf/0.1/Document">
                <div rel="http://xmlns.com/foaf/0.1/topic" resource="<%=domain%>void.ttl#DatasetDescriptions_<%=version%>">
                </div>
            </div>
        </div>

    </body>
</html>