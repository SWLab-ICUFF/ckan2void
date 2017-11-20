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
        <title>Dataset Descriptions (latest version)</title>
    </head>
    <body>
        <div style="margin:auto; text-align:justify; width:70%; height:90%">
            <div style="text-align:right">
                <a href="http://linkeddata.uriburner.com/about/html/<%=domain2%>dataset/DatasetDescriptions/">
                    <i>browse as linked data</i>
                </a>
            </div>
            <h1 style="text-align:center">Dataset Descriptions (latest version)</h1>
            <br/><br/><br/>
            <p>
                This repository contains descriptions of Linked Data datasets using VoID vocabulary. 
				The descriptions include Linksets, classes, properties and topic categories and mashes up data
				from DataHub, dataset dumps, VoID files and DBpedia. The DBpedia Spotlight allowed the 
				recognition of named entities in literal values. Each entity is associated with a list of topic 
				categories through the predicate <i>dcterms:subject</i> and each topic category is subsumed by others 
				through the predicate <i>skos:broader</i>. A category <i>c</i> is a topic category of a dataset iff there 
				exists a property path <i>{e dcterms:subject/skos:broader* c.}</i> from a named entity <i>e</i> of the 
				dataset to <i>c</i> in DBpedia.
            </p>
			<div style="text-align:right">
				<a href="http://linkeddata.uriburner.com/about/html/<%=domain2%>void.ttl%01DatasetDescriptions">void</a>,
				<a href="http://swlab.ic.uff.br/fuseki/dataset.html?tab=query&ds=/DatasetDescriptions">sparql</a>, 
				<a href="https://doi.org/10.6084/m9.figshare.5211916">doi</a>
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
            <div  about="<%=domain%>void.ttl#DatasetDescriptions" typeof="http://rdfs.org/ns/void#Dataset">
                <div property="http://www.w3.org/1999/02/22-rdf-syntax-ns#label" content="Dataset Descriptions">
                </div>
            </div>
            <div  about="#this" typeof="http://xmlns.com/foaf/0.1/Document">
                <div rel="http://xmlns.com/foaf/0.1/topic" resource="<%=domain%>void.ttl#DatasetDescriptions">
                </div>
            </div>
        </div>

    </body>
</html>