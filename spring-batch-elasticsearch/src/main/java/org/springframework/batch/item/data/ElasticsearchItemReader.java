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

import static org.slf4j.LoggerFactory.getLogger;
import static org.springframework.util.Assert.state;
import static org.springframework.util.ClassUtils.getShortName;

import java.util.Iterator;

import org.slf4j.Logger;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchOperations;
import org.springframework.data.elasticsearch.core.query.Query;

/**
 * <p>
 * Restartable {@link ItemReader} that reads documents from Elasticsearch via a paging
 * technique.
 * </p>
 *
 * <p>
 * It executes the query object {@link SearchQuery} to retrieve the requested documents.
 * The query is executed using paged requests specified in the
 * {@link org.springframework.data.elasticsearch.core.query.AbstractQuery#setPageable(Pageable pageable)}.
 * Additional pages are requested as needed to provide data when the {@link #read()}
 * method is called.
 * </p>
 *
 * <p>
 * The implementation is thread-safe between calls to {@link #open(ExecutionContext)}, but
 * remember to use <code>saveState=false</code> if used in a multi-threaded client (no
 * restart available).
 * </p>
 *
 * @author Hasnain Javed
 * @since 3.x.x
 */
public class ElasticsearchItemReader<T> extends AbstractPaginatedDataItemReader<T> implements InitializingBean {

	private final Logger logger;

	private final SearchOperations searchOperations;

	private final Query query;

	private final Class<? extends T> targetType;

	public ElasticsearchItemReader(SearchOperations searchOperations, Query query, Class<? extends T> targetType) {
		setName(getShortName(getClass()));
		logger = getLogger(getClass());
		this.searchOperations = searchOperations;
		this.query = query;
		this.targetType = targetType;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		state(searchOperations != null, "A SearchOperations implementation is required.");
		state(query != null, "A query is required.");
		state(targetType != null, "A target type to convert the input into is required.");
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Iterator<T> doPageRead() {

		logger.debug("executing query {}", query.toString());

		return (Iterator<T>) searchOperations.search(query, targetType).map(SearchHit::getContent).iterator();
	}

}