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

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.SearchHitsImpl;
import org.springframework.data.elasticsearch.core.SearchOperations;
import org.springframework.data.elasticsearch.core.query.BaseQuery;
import org.springframework.data.elasticsearch.core.query.Query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

@MockitoSettings(strictness = LENIENT)
class ElasticsearchItemReaderTest {

	private ElasticsearchItemReader<String> reader;

	@Mock
	private SearchOperations searchOperations;

	private Query query = new BaseQuery();

	@BeforeEach
	void setUp() throws Exception {
		query = new BaseQuery();
		reader = new ElasticsearchItemReader<>(searchOperations, query, String.class);
		reader.afterPropertiesSet();
	}

	@Test
	void shouldFailAssertionOnNullSearchOperations() {
		assertThatIllegalStateException()
			.isThrownBy(() -> new ElasticsearchItemReader<>(null, null, null).afterPropertiesSet())
			.withMessage("A SearchOperations implementation is required.");
	}

	@Test
	void shouldFailAssertionOnNullQuery() {
		assertThatIllegalStateException()
			.isThrownBy(() -> new ElasticsearchItemReader<>(searchOperations, null, null).afterPropertiesSet())
			.withMessage("A query is required.");
	}

	@Test
	void shouldFailAssertionOnNullTargetType() {
		assertThatIllegalStateException()
			.isThrownBy(() -> new ElasticsearchItemReader<>(searchOperations, query, null).afterPropertiesSet())
			.withMessage("A target type to convert the input into is required.");
	}

	@Test
	void shouldQueryForList() {
		var searchHits = buildSearchHits("1", "2", "3");
		when(searchOperations.search(query, String.class)).thenReturn(searchHits);

		var actual = reader.doPageRead();

		var asList = StreamSupport.stream(((Iterable<String>) (() -> actual)).spliterator(), false).toList();
		assertThat(asList).containsExactly("1", "2", "3");
		verify(searchOperations).search(query, String.class);
	}

	private <T> SearchHit<T> buildSearchHit(T content) {
		return new SearchHit<T>(null, null, null, 0f, null, null, null, null, null, null, content);
	}

	private <T> SearchHits<T> buildSearchHits(T... content) {
		var searchHits = Stream.of(content).map(this::buildSearchHit).toList();
		return new SearchHitsImpl<T>(content.length, null, 0f, null, null, null, searchHits, null, null, null);
	}

}