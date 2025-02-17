package org.springframework.batch.item.data;

import java.io.IOException;
import java.util.stream.IntStream;

import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchClientAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.RestClientBuilderCustomizer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.batch.item.data.TestDatabase.ELASTICSEARCH_8;

@Testcontainers
@SpringBatchTest
@ImportAutoConfiguration({ ElasticsearchRestClientAutoConfiguration.class, ElasticsearchClientAutoConfiguration.class,
		ElasticsearchDataAutoConfiguration.class })
@TestPropertySource(properties = { "spring.elasticsearch.username=elastic", "spring.elasticsearch.password=changeme" })
class Elasticsearch8IntegrationTest {

	@Container
	static ElasticsearchContainer container = new ElasticsearchContainer(ELASTICSEARCH_8.getImageName());

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		registry.add("spring.elasticsearch.uris", () -> "https://" + container.getHttpHostAddress());
	}

	@Test
	void testSetup(@Autowired RestClient restClient) throws IOException {
		Response response = restClient.performRequest(new Request("GET", "/_cluster/health"));
		assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
	}

	@Test
	void testSpringData(@Autowired JobLauncherTestUtils jobLauncherTestUtils) throws Exception {
		JobExecution jobExecution = jobLauncherTestUtils.launchJob();

		assertThat(jobExecution).isNotNull();
		assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
	}

	@TestConfiguration
	static class JobConfiguration {

		@Bean
		ResourcelessJobRepository resourcelessJobRepository() {
			return new ResourcelessJobRepository();
		}

		@Bean
		TaskExecutorJobLauncher jobLauncher(JobRepository jobRepository) {
			var jobLauncher = new TaskExecutorJobLauncher();
			jobLauncher.setJobRepository(jobRepository);
			jobLauncher.setTaskExecutor(new SyncTaskExecutor());
			return jobLauncher;
		}

		@Bean
		Job job(JobRepository jobRepository, Step step) {
			return new JobBuilder("job", jobRepository).start(step).build();
		}

		@Bean
		Step step(JobRepository jobRepository, ItemProcessor<Integer, IndexQuery> processor,
				ElasticsearchItemWriter writer) {
			return new StepBuilder("step", jobRepository)
				.<Integer, IndexQuery>chunk(10, new ResourcelessTransactionManager())
				.reader(new ListItemReader<>(IntStream.range(0, 100).boxed().toList()))
				.processor(processor)
				.writer(writer)
				.build();
		}

		@Bean
		ItemProcessor<Integer, IndexQuery> itemProcessor() {
			return i -> {
				var item = new TestItem(String.valueOf(i));
				return new IndexQueryBuilder().withObject(item).build();
			};
		}

		@Bean
		ElasticsearchItemWriter elasticsearchItemWriter(ElasticsearchOperations elasticsearchOperations) {
			var indexCoordinates = elasticsearchOperations.getIndexCoordinatesFor(TestItem.class);
			return new ElasticsearchItemWriter(elasticsearchOperations, indexCoordinates);
		}

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

	@Document(indexName = "test-index")
	static class TestItem {

		@Id
		private String id;

		private String name;

		TestItem(String name) {
			this.name = name;
		}

		String getId() {
			return id;
		}

		String getName() {
			return id;
		}

	}

}
