package uff.ic.swlab.ckancrawler.helper;

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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import uff.ic.swlab.ckancrawler.Config;
import uff.ic.swlab.ckancrawler.util.Executor;

public abstract class URLHelper {

    private static String getContent(String url) throws MalformedURLException, IOException, URISyntaxException {
        URLConnection conn = (new URL(normalize(url))).openConnection();
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

    public static String normalize(String url) throws MalformedURLException, URISyntaxException {
        URL url_ = new URL(url.trim());
        URI uri_ = new URI(url_.getProtocol(), url_.getUserInfo(), url_.getHost().toLowerCase(), url_.getPort(), url_.getPath(), url_.getQuery(), url_.getRef());
        return uri_.toString();
    }

    public static boolean sameAs(String url1, String url2) throws UnknownHostException, MalformedURLException, URISyntaxException, IOException {
        URL url1_ = new URL(url1);
        URL url2_ = new URL(url2);
        InetAddress address1 = InetAddress.getByName(url1_.getHost());
        InetAddress address2 = InetAddress.getByName(url2_.getHost());
        if (!normalize(url1).equals(normalize(url2))) {
            if (address1.getHostAddress().equals(address2.getHostAddress()))
                if (StringUtils.getJaroWinklerDistance(url1, url2) > 0.9)
                    return true;
            return false;
        }
        return true;
    }

    public static boolean isHTML(String url) throws MalformedURLException, IOException, InterruptedException, ExecutionException, TimeoutException {
        Callable<Boolean> task = () -> {
            String contentType;
            URLConnection conn = (new URL(url)).openConnection();
            conn.setConnectTimeout(Config.HTTP_CONNECT_TIMEOUT);
            conn.setReadTimeout(Config.HTTP_READ_TIMEOUT);
            contentType = conn.getContentType().toLowerCase();
            conn.getInputStream().close();

            if (contentType.contains("html"))
                return true;
            else
                return false;
        };
        return Executor.execute(task, Config.MODEL_READ_TIMEOUT);
    }

}
