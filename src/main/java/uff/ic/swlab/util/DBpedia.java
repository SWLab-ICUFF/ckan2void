package uff.ic.swlab.util;

import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;

public class DBpedia {

    public static void main(String[] args) {
        annotate("Rio de Janeiro is the wonderful city of Brazil.");
    }

    public static void annotate(String text) {
        String backupUrl = "http://model.dbpedia-spotlight.org/en/annotate";
        HttpClient httpclient = HttpClients.createDefault();
        try {
            URIBuilder builder = new URIBuilder(backupUrl);
            builder.setParameter("text", text).setParameter("confidence", "0.8");
            HttpGet request = new HttpGet(builder.build());
            request.addHeader("Accept", "application/json");

            HttpResponse response = httpclient.execute(request);
            int statuscode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (entity != null && statuscode == 200)
                try (final InputStream instream = entity.getContent()) {
                    System.out.println(IOUtils.toString(instream, "utf-8"));
                }
            else
                System.out.println("Backup request failed.");
        } catch (Throwable e) {
            System.out.println("Backup request failed.");
        }
    }

}
