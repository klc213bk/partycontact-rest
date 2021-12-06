package com.transglobe.streamingetl.partycontact.rest.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.partycontact.rest.bean.URLConstants;

public class TopicUtils {
	static final Logger LOG = LoggerFactory.getLogger(TopicUtils.class);

	public static Set<String> listTopics() throws Exception {
		String response = HttpUtils.restService(URLConstants.URL_LIST_TOPIC, "GET");
		
		LOG.info(">>>>>>>>>>>> response={} ", response);

		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonNode = mapper.readTree(response);
		String topicStr = jsonNode.get("topics").asText();
		List<String> topicList = mapper.readValue(topicStr, new TypeReference<List<String>>() {});
		LOG.info(">>>>>>>>>>>> topics={} ", String.join(",", topicList));
		
		return new HashSet<>(topicList);
	}
	public static void createTopic(String topic) throws Exception {
		
		String createUrl = String.format(URLConstants.URL_CREATE_TOPIC, topic);
		LOG.info(">>>>>>>>>>>> createUrl={} ", createUrl);
		String response = HttpUtils.restService(createUrl, "POST");
		
		LOG.info(">>>>>>>>>>>> response={} ", response);


	}
	public static void deleteTopic(String topic) throws Exception {
		
		String deleteUrl = String.format(URLConstants.URL_DELETE_TOPIC, topic);
		LOG.info(">>>>>>>>>>>> deleteUrl={} ", deleteUrl);
		
		String response = HttpUtils.restService(deleteUrl, "POST");
		
		LOG.info(">>>>>>>>>>>> response={} ", response);


	}
}
