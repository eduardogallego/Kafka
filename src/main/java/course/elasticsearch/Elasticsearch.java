package course.elasticsearch;

import edu.config.ConfigSt;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Elasticsearch {

    private static final ConfigSt CONFIG = ConfigSt.getInstance();
    private static final int ES_PORT = Integer.parseInt(CONFIG.getConfig("elasticsearch_port"));
    private static final Logger LOGGER = LoggerFactory.getLogger(Elasticsearch.class.getName());
    private static final String ES_HOSTNAME = CONFIG.getConfig("elasticsearch_hostname"),
            ES_SCHEME = CONFIG.getConfig("elasticsearch_scheme");

    public static void main(String[] args) {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost(ES_HOSTNAME, ES_PORT, ES_SCHEME)))) {


        } catch (Exception ex) {
            LOGGER.error("Exception: " + ex.getMessage(), ex);
        }
    }
}
