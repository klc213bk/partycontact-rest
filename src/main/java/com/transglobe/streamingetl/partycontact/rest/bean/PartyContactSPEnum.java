package com.transglobe.streamingetl.partycontact.rest.bean;

public enum PartyContactSPEnum {
	GET_STREAMING_ETL_STATE("GET_STREAMING_ETL_STATE", "createSP-GET_STREAMING_ETL_STATE.sql");
	;
	
	private String spName;
	private String scriptFile;

	PartyContactSPEnum(String spName, String scriptFile) {
		this.spName = spName;
		this.scriptFile = scriptFile;
	}

	public String getSpName() {
		return spName;
	}

	public void setSpName(String spName) {
		this.spName = spName;
	}

	public String getScriptFile() {
		return scriptFile;
	}

	public void setScriptFile(String scriptFile) {
		this.scriptFile = scriptFile;
	}
	
	
}
