/*
 * Copyright 2002-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.item.data;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.quality.Strictness.LENIENT;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.DocumentOperations;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

@MockitoSettings(strictness = LENIENT)
public class ElasticsearchItemWriterTest {

	@Document(indexName = "test_index")
	public class DummyDocument {

		private String id;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

	}

	private ElasticsearchItemWriter writer;

	@Mock
	private DocumentOperations documentOperations;

	private TransactionTemplate transactionTemplate;

	private DummyDocument dummyDocument;

	@BeforeEach
	public void setUp() throws Exception {
		transactionTemplate = new TransactionTemplate(new ResourcelessTransactionManager());
		writer = new ElasticsearchItemWriter(documentOperations, null);
		writer.afterPropertiesSet();
		dummyDocument = new DummyDocument();
	}

	@Test
	void shouldFailAssertion() {
		assertThatIllegalStateException()
			.isThrownBy(() -> new ElasticsearchItemWriter(null, null).afterPropertiesSet());
	}

	@Test
	void shouldNotWriteWhenNoTransactionIsActiveAndNoItem() throws Exception {

		writer.write(new Chunk<>());
		verifyNoInteractions(documentOperations);

		writer.write(new Chunk<>(emptyList()));
		verifyNoInteractions(documentOperations);
	}

	@Test
	void shouldWriteItemWhenNoTransactionIsActive() throws Exception {

		IndexQueryBuilder builder = new IndexQueryBuilder();
		builder.withObject(dummyDocument);

		IndexQuery item = builder.build();
		Chunk<IndexQuery> items = Chunk.of(item);

		writer.write(items);

		verify(documentOperations).index(item, null);
	}

	@Test
	void shouldWriteItemWhenInTransaction() {

		IndexQueryBuilder builder = new IndexQueryBuilder();
		builder.withObject(dummyDocument);

		IndexQuery item = builder.build();
		Chunk<IndexQuery> items = Chunk.of(item);

		transactionTemplate.execute((TransactionCallback<Void>) status -> {
			try {
				writer.write(items);
			}
			catch (Exception e) {
				fail("An error occurred while writing", e);
			}
			return null;
		});

		verify(documentOperations).index(item, null);
	}

	@Test
	void shouldNotWriteItemWhenTransactionFails() {

		IndexQueryBuilder builder = new IndexQueryBuilder();
		builder.withObject(dummyDocument);

		IndexQuery item = builder.build();
		Chunk<IndexQuery> items = Chunk.of(item);

		try {
			transactionTemplate.execute((TransactionCallback<Void>) status -> {
				try {
					writer.write(items);
				}
				catch (Exception ignore) {
					fail("unexpected error occurred");
				}
				throw new RuntimeException("rollback");
			});
		}
		catch (RuntimeException re) {
			// ignore
		}
		catch (Throwable t) {
			fail("Unexpected error occurred");
		}

		verifyNoInteractions(documentOperations);
	}

	@Test
	void shouldNotWriteItemWhenTransactionIsReadOnly() {

		IndexQueryBuilder builder = new IndexQueryBuilder();
		builder.withObject(dummyDocument);

		IndexQuery item = builder.build();
		Chunk<IndexQuery> items = Chunk.of(item);

		try {

			transactionTemplate.setReadOnly(true);
			transactionTemplate.execute((TransactionCallback<Void>) status -> {
				try {
					writer.write(items);
				}
				catch (Exception ignore) {
					Assertions.fail("unexpected error occurred");
				}
				throw new RuntimeException("rollback");
			});
		}
		catch (RuntimeException re) {
			// ignore
		}
		catch (Throwable t) {
			fail("Unexpected error occurred");
		}

		verifyNoInteractions(documentOperations);
	}

	@Test
	void shouldRemoveItemWhenNoTransactionIsActive() throws Exception {

		writer.setDelete(true);

		dummyDocument.setId("123456");

		IndexQueryBuilder builder = new IndexQueryBuilder();
		builder.withId(dummyDocument.getId());
		builder.withObject(dummyDocument);
		IndexQuery item = builder.build();

		Chunk<IndexQuery> items = Chunk.of(item);

		writer.write(items);

		verify(documentOperations).delete("123456", DummyDocument.class);
	}

	@Test
	void shouldRemoveItemWhenInTransaction() {

		writer.setDelete(true);

		dummyDocument.setId("123456");

		IndexQueryBuilder builder = new IndexQueryBuilder();
		builder.withId(dummyDocument.getId());
		builder.withObject(dummyDocument);
		IndexQuery item = builder.build();

		Chunk<IndexQuery> items = Chunk.of(item);

		transactionTemplate.execute((TransactionCallback<Void>) status -> {
			try {
				writer.write(items);
			}
			catch (Exception e) {
				fail("An error occurred while writing: " + e.getMessage());
			}

			return null;
		});

		verify(documentOperations).delete("123456", DummyDocument.class);
	}

}