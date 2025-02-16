package org.springframework.batch.item.data;

import java.io.IOException;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchClientAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringJUnitConfig
@ImportAutoConfiguration({ ElasticsearchRestClientAutoConfiguration.class, ElasticsearchClientAutoConfiguration.class })
class Elasticsearch7IntegrationTest {

	private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:7.17.27";

	@Container
	@ServiceConnection
	static ElasticsearchContainer container = new ElasticsearchContainer(IMAGE_NAME);

	@Test
	void testSetup(@Autowired RestClient restClient) throws IOException {
		Response response = restClient.performRequest(new Request("GET", "/_cluster/health"));
		assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
	}

}
