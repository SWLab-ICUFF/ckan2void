package draft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import org.apache.commons.lang3.StringUtils;
import uff.ic.swlab.datasetcrawler.util.Config;

public class NewClass2 {

    public static void main(String[] args) throws IOException, URISyntaxException {
        String urlString1 = "http://swlab.ic.uff.br";
        String urlString2 = "http://Swlab.ic.uff.br:80/default.jsp";
        //System.out.println(sameAs(urlString1, urlString2));
        //System.out.println(normalizeURL("http://Swlab.ic.uff.br/a/default.jsp"));
        System.out.println((new URL(urlString2)).getContent().getClass().toGenericString());
    }

    private static String getContent(String urlString) throws MalformedURLException, IOException, URISyntaxException {
        URLConnection conn = (new URL(normalizeURL(urlString))).openConnection();
        conn.setConnectTimeout(Config.HTTP_CONNECT_TIMEOUT);
        conn.setReadTimeout(Config.HTTP_READ_TIMEOUT);

        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));) {
            StringBuilder response = new StringBuilder();
            String inputLine;
            while ((inputLine = in.readLine()) != null)
                response.append(inputLine);
            return response.toString();
        }
    }

    public static String normalizeURL(String urlString) throws MalformedURLException, URISyntaxException {
        URL url = new URL(urlString);
        URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost().toLowerCase(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
        String canonical = uri.toString();
        return canonical;
    }

    public static boolean sameAs(String urlString1, String urlString2) throws UnknownHostException, MalformedURLException, URISyntaxException, IOException {
        URL url1 = new URL(urlString1);
        URL url2 = new URL(urlString2);
        InetAddress address1 = InetAddress.getByName(url1.getHost());
        InetAddress address2 = InetAddress.getByName(url2.getHost());
        if (!normalizeURL(urlString1).equals(normalizeURL(urlString2))) {
            if (address1.getHostAddress().equals(address2.getHostAddress()))
                if (StringUtils.getJaroWinklerDistance(urlString1, urlString2) > 0.9)
                    return true;
            return false;
        }
        return true;
    }
}
