package io.pravega.anomalydetection.event.pipeline;

import io.pravega.anomalydetection.event.AppConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.net.URL;

public class ElasticSetup {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSetup.class);

    private static final String VISUALIZATION_ID = "ec05f200-6bfd-11e7-80d1-4b492e6a1316";
    private static final String ELASTIC_PORT = "9200";

    private final AppConfiguration appConfiguration;
    private final HttpClient httpClient;
    private final AppConfiguration.ElasticSearch elasticConfig;

    public ElasticSetup(AppConfiguration appConfiguration) {
        this.appConfiguration = appConfiguration;
        this.httpClient = new DefaultHttpClient();
        this.elasticConfig  = appConfiguration.getPipeline().getElasticSearch();
    }

    public void run() throws Exception {
        LOG.info("Deleting old elastic search index");
        httpDelete("/"+ appConfiguration.getPipeline().getElasticSearch().getIndex(), 200, 404);

        LOG.info("Creating elastic Search Index");
        String indexBody = getTemplate("anomaly-elastic-index.json", Collections.singletonMap("type", elasticConfig.getType()));
        httpPut("/"+ elasticConfig.getIndex(), indexBody, 200);

        LOG.info("Creating Kibana Index Pattern");
        String kibanaIndex = getTemplate("anomaly-kibana-index-pattern.json", Collections.singletonMap("index", elasticConfig.getIndex()));
        httpPut("/.kibana/index-pattern/"+elasticConfig.getIndex(), kibanaIndex, 200, 201);

        LOG.info("Creating Kibana Search");
        String kibanaSearch = getTemplate("anomaly-kibana-search.json", Collections.singletonMap("index", elasticConfig.getIndex()));
        httpPut("/.kibana/search/anomalies", kibanaSearch, 200, 201);

        LOG.info("Creating Kibana Visualisation");
        String visualizationBody = getTemplate("anomaly-kibana-visualization.json", Collections.singletonMap("index", elasticConfig.getIndex()));
        httpPut("/.kibana/visualization/"+VISUALIZATION_ID, visualizationBody, 200, 201);
    }

    private void httpDelete(String path, int... successCodes) throws Exception {
        httpRequest(new HttpDelete(getTargetUrl(path)), successCodes);
    }

    private void httpPut(String path, String body, int... successCodes) throws Exception {
        HttpPut httpPut = new HttpPut(getTargetUrl(path));
        httpPut.setEntity(new StringEntity(body));

        httpRequest(httpPut, successCodes);
    }

    private void httpRequest(HttpUriRequest httpMethod, int... successCodes) throws Exception {
        HttpResponse response = httpClient.execute(httpMethod);

        int responseCode = response.getStatusLine().getStatusCode();
        String responseBody = "";

        if( response.getEntity() != null ) {
            responseBody = EntityUtils.toString(response.getEntity());
        }

        for (int code : successCodes) {
            if (code == responseCode) {
                return;
            }
        }

        throw new IllegalStateException("Http failure :"+responseCode+"  "+responseBody);
    }

    private String getTargetUrl(String path) {
        return "http://"+
            elasticConfig.getHost()+":"+
            ELASTIC_PORT+
            path;
    }

    private String getTemplate(String file, Map<String, String> values) throws Exception {
        URL url = getClass().getClassLoader().getResource(file);
        if (url == null) {
            throw new IllegalStateException("Template file "+file+" not found");
        }

        String body = IOUtils.toString(url.openStream());
        for (Map.Entry<String, String> value : values.entrySet()) {
            body = body.replace("@@"+value.getKey()+"@@", value.getValue());
        }

        return body;
    }
}
