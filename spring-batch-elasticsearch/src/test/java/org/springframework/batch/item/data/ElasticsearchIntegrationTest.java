package org.springframework.batch.item.data;

import java.io.IOException;

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchClientAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.RestClientBuilderCustomizer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringJUnitConfig
@ImportAutoConfiguration({ ElasticsearchRestClientAutoConfiguration.class, ElasticsearchClientAutoConfiguration.class })
@TestPropertySource(properties = { "spring.elasticsearch.username=elastic", "spring.elasticsearch.password=changeme" })
class ElasticsearchIntegrationTest {

	private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:8.17.2";

	@Container
	static ElasticsearchContainer container = new ElasticsearchContainer(IMAGE_NAME);

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		registry.add("spring.elasticsearch.uris", () -> "https://" + container.getHttpHostAddress());
	}

	@Test
	void testSetup(@Autowired RestClient restClient) throws IOException {
		Response response = restClient.performRequest(new Request("GET", "/_cluster/health"));
		assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
	}

	@TestConfiguration
	static class Ssl {

		@Bean
		public RestClientBuilderCustomizer customizer() {
			return new RestClientBuilderCustomizer() {
				@Override
				public void customize(RestClientBuilder builder) {

				}

				@Override
				public void customize(HttpAsyncClientBuilder builder) {
					builder.setSSLContext(container.createSslContextFromCa());
				}
			};
		}

	}

}
