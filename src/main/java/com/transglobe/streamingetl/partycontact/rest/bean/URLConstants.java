package com.transglobe.streamingetl.partycontact.rest.bean;

public class URLConstants {
	// Topic
	public static String URL_LIST_TOPIC = "http://localhost:9101/kafka/listTopics";
	public static String URL_DELETE_TOPIC = "http://localhost:9101/kafka/deleteTopic/%s";
	public static String URL_CREATE_TOPIC = "http://localhost:9101/kafka/createTopic/%s";

	// Logminer connector
	public static String URL_APPLY_LOGMINER_SYNC = "http://localhost:9102/logminer/applyLogminerSync";

}
