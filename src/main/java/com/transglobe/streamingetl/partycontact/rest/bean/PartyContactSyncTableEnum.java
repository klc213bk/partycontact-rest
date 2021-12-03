package com.transglobe.streamingetl.partycontact.rest.bean;

public enum PartyContactSyncTableEnum {
	T_POLICY_HOLDER("T_POLICY_HOLDER"),
	T_INSURED_LIST("T_INSURED_LIST"),
	T_CONTRACT_BENE("T_CONTRACT_BENE"),
	T_POLICY_HOLDER_LOG("T_POLICY_HOLDER_LOG"),
	T_INSURED_LIST_LOG("T_INSURED_LIST_LOG"),
	T_CONTRACT_BENE_LOG("T_CONTRACT_BENE_LOG"),
	T_ADDRESS("T_ADDRESS");

	private String tableName;
	
	PartyContactSyncTableEnum(String tableName) {
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	
}
