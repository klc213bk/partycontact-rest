package com.transglobe.streamingetl.partycontact.rest.bean;

public enum PartyContactTopicEnum {
	TOPIC_T_POLICY_HOLDER("EBAOPRD1.LS_EBAO.T_POLICY_HOLDER"),
	TOPIC_T_INSURED_LIST("EBAOPRD1.LS_EBAO.T_INSURED_LIST"),
	TOPIC_T_CONTRACT_BENE("EBAOPRD1.LS_EBAO.T_CONTRACT_BENE"),
	TOPIC_T_POLICY_HOLDER_LOG("EBAOPRD1.LS_EBAO.T_POLICY_HOLDER_LOG"),
	TOPIC_T_INSURED_LIST_LOG("EBAOPRD1.LS_EBAO.T_INSURED_LIST_LOG"),
	TOPIC_T_CONTRACT_BENE_LOG("EBAOPRD1.LS_EBAO.T_CONTRACT_BENE_LOG"),
	TOPIC_T_ADDRESS("EBAOPRD1.LS_EBAO.T_ADDRESS"),
	DDL("EBAOPRD1.LS_EBAO._GENERIC_DDL");
	
			
	private String topic;
	

	PartyContactTopicEnum(String topic) {
		this.topic = topic;
	}


	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		this.topic = topic;
	}

}
