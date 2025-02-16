package org.springframework.batch.item.data;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.testcontainers.OpensearchContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.batch.item.data.TestDatabase.OPENSEARCH_1;

@Testcontainers
@SpringJUnitConfig
class Opensearch1IntegrationTest {

	@Container
	static OpensearchContainer<?> container = new OpensearchContainer<>(OPENSEARCH_1.getImageName());

	@Test
	void testSetup(@Autowired RestClient restClient) throws IOException {
		Response response = restClient.performRequest(new Request("GET", "/_cluster/health"));
		assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
	}

	@TestConfiguration
	static class RestClientConfiguration {

		@Bean
		public RestClient restClient() {
			return RestClient.builder(HttpHost.create(container.getHttpHostAddress())).build();
		}

	}

}
