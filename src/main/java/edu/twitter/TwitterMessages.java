package edu.twitter;

import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class TwitterMessages {

    private static final String CONSUMER_KEY = "GK7FHT4mUHHDrzYpcwNIe4UjS",
            CONSUMER_SECRET = "RiMfiTwX9zBvirziJdnZ2QbXIO4AIV1e3LLFvH0lSpbpVd5JRu",
            TOKEN = "65xlee5LXCGHwNqRfkm7O1AVrLCktmwJJBrM4rQ0",
            SECRET = "uDb1sDmgCrAxfc7qaUvh5TfMjgc7kHKF7rOfhxKhRkCi4",
            ENDPOINT_AUTHENTICATION = "https://api.twitter.com/oauth2/token",
            ENDPOINT_USER_TIMELINE = "https://api.twitter.com/1.1/statuses/user_timeline.json",
            ENDPOINT_TWEET_SEARCH = "https://api.twitter.com/1.1/search/tweets.json";
    private static final SimpleDateFormat TWITTER_SDF =
            new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
    private final Logger logger = LoggerFactory.getLogger(TwitterMessages.class.getName());

    public TwitterMessages() {
    }

    public static void main(String[] args) {
        new TwitterMessages().run();
    }

    // Encodes the consumer key and secret to create the basic authorization key
    private static String encodeKeys(String consumerKey, String consumerSecret) {
        try {
            String encodedConsumerKey = URLEncoder.encode(consumerKey, "UTF-8");
            String encodedConsumerSecret = URLEncoder.encode(consumerSecret, "UTF-8");
            String fullKey = encodedConsumerKey + ":" + encodedConsumerSecret;
            byte[] encodedBytes = Base64.encodeBase64(fullKey.getBytes());
            return new String(encodedBytes);
        } catch (UnsupportedEncodingException e) {
            return new String();
        }
    }

    // Constructs the request for requesting a bearer token and returns that token as a string
    private static String requestBearerToken() throws IOException {
        HttpsURLConnection connection = null;
        String encodedCredentials = encodeKeys(CONSUMER_KEY, CONSUMER_SECRET);

        try {
            URL url = new URL(ENDPOINT_AUTHENTICATION);
            connection = (HttpsURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Host", "api.twitter.com");
            connection.setRequestProperty("User-Agent", "TwitterDataSource");
            connection.setRequestProperty("Authorization", "Basic " + encodedCredentials);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
            connection.setRequestProperty("Content-Length", "29");
            connection.setUseCaches(false);
            writeRequest(connection, "grant_type=client_credentials");

            // Parse the JSON response into a JSON mapped object to fetch fields from.
            JSONObject obj = (JSONObject) JSONValue.parse(readResponse(connection));
            if (obj != null) {
                String tokenType = (String) obj.get("token_type");
                String token = (String) obj.get("access_token");
                return ((tokenType.equals("bearer")) && (token != null)) ? token : "";
            }
            return new String();
        } catch (MalformedURLException e) {
            throw new IOException("Invalid endpoint URL specified.", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static JSONArray fetchTimelineTweet(String bearerToken) throws IOException {
        HttpsURLConnection connection = null;

        try {
            URL url = new URL(ENDPOINT_USER_TIMELINE + "?screen_name=israelloranca&count=20");
            connection = (HttpsURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Host", "api.twitter.com");
            connection.setRequestProperty("User-Agent", "TwitterDataSource");
            connection.setRequestProperty("Authorization", "Bearer " + bearerToken);
            connection.setUseCaches(false);

            // Parse the JSON response into a JSON mapped object to fetch fields from.
            return (JSONArray) JSONValue.parse(readResponse(connection));
        } catch (MalformedURLException e) {
            throw new IOException("Invalid endpoint URL specified.", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static JSONArray getTweets(String bearerToken) throws IOException {
        HttpsURLConnection connection = null;

        try {
            URL url = new URL(ENDPOINT_TWEET_SEARCH + "?q=atleti&result_type=recent&lang=es&count=100");
            connection = (HttpsURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Host", "api.twitter.com");
            connection.setRequestProperty("User-Agent", "TwitterDataSource");
            connection.setRequestProperty("Authorization", "Bearer " + bearerToken);
            connection.setUseCaches(false);
            JSONObject resp = (JSONObject) JSONValue.parse(readResponse(connection));
            if (resp != null) {
                JSONArray JSONArray = (JSONArray) resp.get("statuses");
                return JSONArray;
            }
            return null;
        } catch (MalformedURLException e) {
            throw new IOException("Invalid endpoint URL specified.", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static boolean writeRequest(HttpURLConnection connection, String textBody) throws IOException {
        BufferedWriter wr = null;
        try {
            wr = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()));
            wr.write(textBody);
            wr.flush();
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (wr != null) {
                wr.close();
            }
        }
    }

    private static String readResponse(HttpURLConnection connection) throws IOException {
        BufferedReader br = null;
        try {
            StringBuilder str = new StringBuilder();
            br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line = "";
            while ((line = br.readLine()) != null) {
                str.append(line + System.getProperty("line.separator"));
            }
            return str.toString();
        } catch (IOException e) {
            return "";
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    public void run() {
        try {
            logger.info("Here we go!!");
            String encodedKeys = encodeKeys(CONSUMER_KEY, CONSUMER_SECRET);
            logger.info("EncodedKeys: " + encodedKeys);
            String bearerToken = requestBearerToken();
            logger.info("BearerTokn: " + bearerToken);
            // JSONArray msgArr = fetchTimelineTweet(bearerToken);
            JSONArray msgArr = getTweets(bearerToken);
            if (msgArr != null) {
                for (int i = 0; i < msgArr.size(); ++i) {
                    JSONObject json = (JSONObject) msgArr.get(i);
                    String msg = json.get("text").toString();
                    Date date = TWITTER_SDF.parse(json.get("created_at").toString());
                    System.out.println("Message: " + date + " - " + msg);
                }
            }
        } catch (Exception ex) {
            logger.error("Exception: " + ex.getMessage());
        }
    }
}
