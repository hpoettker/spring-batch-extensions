package org.springframework.batch.item.data;

import org.testcontainers.utility.DockerImageName;

enum TestDatabase {

	ELASTICSEARCH_7("docker.elastic.co/elasticsearch/elasticsearch:7.17.27"),
	ELASTICSEARCH_8("docker.elastic.co/elasticsearch/elasticsearch:8.17.2"),
	OPENSEARCH_1("opensearchproject/opensearch:1.3.20"), //
	OPENSEARCH_2("opensearchproject/opensearch:2.19.0");

	private final DockerImageName imageName;

	TestDatabase(String imageName) {
		this.imageName = DockerImageName.parse(imageName);
	}

	public DockerImageName getImageName() {
		return imageName;
	}

}
