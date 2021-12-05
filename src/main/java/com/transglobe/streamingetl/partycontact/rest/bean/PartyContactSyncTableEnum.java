package com.transglobe.streamingetl.partycontact.rest.bean;

public enum PartyContactSyncTableEnum {
	SYNC_TABLE_T_POLICY_HOLDER("LS_EBAO.T_POLICY_HOLDER"),
	SYNC_TABLE_T_INSURED_LIST("LS_EBAO.T_INSURED_LIST"),
	SYNC_TABLE_T_CONTRACT_BENE("LS_EBAO.T_CONTRACT_BENE"),
	SYNC_TABLE_T_POLICY_HOLDER_LOG("LS_EBAO.T_POLICY_HOLDER_LOG"),
	SYNC_TABLE_T_INSURED_LIST_LOG("LS_EBAO.T_INSURED_LIST_LOG"),
	SYNC_TABLE_T_CONTRACT_BENE_LOG("LS_EBAO.T_CONTRACT_BENE_LOG"),
	SYNC_TABLE_T_ADDRESS("LS_EBAO.T_ADDRESS");

	private String syncTableName;

	PartyContactSyncTableEnum(String syncTableName) {
		this.syncTableName = syncTableName;	
	}

	public String getSyncTableName() {
		return syncTableName;
	}

	public void setSyncTableName(String syncTableName) {
		this.syncTableName = syncTableName;
	}

}
