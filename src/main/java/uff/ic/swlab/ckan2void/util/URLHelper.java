package uff.ic.swlab.ckan2void.util;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class URLHelper {

    public static String normalize(String url) throws MalformedURLException, URISyntaxException {
        URL url_ = new URL(url.trim());
        URI uri_ = new URI(url_.getProtocol(), url_.getUserInfo(), url_.getHost().toLowerCase(), url_.getPort(), url_.getPath(), url_.getQuery(), url_.getRef());
        return uri_.toString();
    }

    public static boolean isHTML(String url) throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
        Callable<Boolean> task = () -> {
            String contentType;
            URLConnection conn = (new URL(url)).openConnection();
            conn.setConnectTimeout(Config.getInsatnce().httpConnectTimeout());
            conn.setReadTimeout(Config.getInsatnce().httpReadTimeout());
            contentType = conn.getContentType().toLowerCase();
            conn.getInputStream().close();

            if (contentType.contains("html"))
                return true;
            else
                return false;
        };
        return Executor.execute(task, "Ask if " + url + " is html", Config.getInsatnce().httpAccessTimeout());
    }

//    private static String getContent(String url) throws MalformedURLException, IOException, URISyntaxException, InterruptedException, TimeoutException, ExecutionException {
//        Callable<String> task = () -> {
//            URLConnection conn = (new URL(normalize(url))).openConnection();
//            conn.setConnectTimeout(Config.HTTP_CONNECT_TIMEOUT);
//            conn.setReadTimeout(Config.HTTP_READ_TIMEOUT);
//
//            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));) {
//                StringBuilder response = new StringBuilder();
//                String inputLine;
//                while ((inputLine = in.readLine()) != null)
//                    response.append(inputLine);
//                return response.toString();
//            }
//        };
//        return Executor.execute(task, "Get content of " + url, Config.HTTP_ACCESS_TIMEOUT);
//    }
//    public static boolean sameAs(String url1, String url2) throws UnknownHostException, MalformedURLException, URISyntaxException, IOException {
//        URL url1_ = new URL(url1);
//        URL url2_ = new URL(url2);
//        InetAddress address1 = InetAddress.getByName(url1_.getHost());
//        InetAddress address2 = InetAddress.getByName(url2_.getHost());
//        if (!normalize(url1).equals(normalize(url2))) {
//            if (address1.getHostAddress().equals(address2.getHostAddress()))
//                if (StringUtils.getJaroWinklerDistance(url1, url2) > 0.9)
//                    return true;
//            return false;
//        }
//        return true;
//    }
}
