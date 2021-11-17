package com.transglobe.streamingetl.partycontact.rest.bean;

public class KafkaConsumerState {
	public static String RUNNING = "RUNNING";
	public static String STOPPED = "STOPPED";
	public static String ERROR = "ERROR";
	
	private String state;
	
	public KafkaConsumerState() {}
	
	public KafkaConsumerState(String state) {
		this.state = state;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}
}
