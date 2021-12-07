package com.transglobe.streamingetl.partycontact.rest.bean;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class TMHealth {
	private Timestamp heartbeatTime;
	private Long currentScn;
	private Long logminerScn;
	private Timestamp logminerReceiveed;
	private Timestamp consumerReceived;
	private String consumerClient;
	public String getConsumerClient() {
		return consumerClient;
	}
	public void setConsumerClient(String consumerClient) {
		this.consumerClient = consumerClient;
	}
	private BigDecimal cpuUtilValue;
	public Timestamp getHeartbeatTime() {
		return heartbeatTime;
	}
	public void setHeartbeatTime(Timestamp heartbeatTime) {
		this.heartbeatTime = heartbeatTime;
	}
	public Long getCurrentScn() {
		return currentScn;
	}
	public void setCurrentScn(Long currentScn) {
		this.currentScn = currentScn;
	}
	public Long getLogminerScn() {
		return logminerScn;
	}
	public void setLogminerScn(Long logminerScn) {
		this.logminerScn = logminerScn;
	}
	public Timestamp getLogminerReceiveed() {
		return logminerReceiveed;
	}
	public void setLogminerReceiveed(Timestamp logminerReceiveed) {
		this.logminerReceiveed = logminerReceiveed;
	}
	public Timestamp getConsumerReceived() {
		return consumerReceived;
	}
	public void setConsumerReceived(Timestamp consumerReceived) {
		this.consumerReceived = consumerReceived;
	}
	public BigDecimal getCpuUtilValue() {
		return cpuUtilValue;
	}
	public void setCpuUtilValue(BigDecimal cpuUtilValue) {
		this.cpuUtilValue = cpuUtilValue;
	}
	
	
}
