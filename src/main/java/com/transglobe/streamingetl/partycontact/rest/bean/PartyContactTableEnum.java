package com.transglobe.streamingetl.partycontact.rest.bean;

public enum PartyContactTableEnum {
	T_PARTY_CONTACT("TGLMINER", "T_PARTY_CONTACT", "createtable-T_PARTY_CONTACT.sql");
	
	private String schema;
	
	private String tableName;
	
	private String scriptFile;

	PartyContactTableEnum(String schema, String tableName, String scriptFile) {
		this.schema = schema;
		this.tableName = tableName;
		this.scriptFile = scriptFile;
	}

	
	public String getSchema() {
		return schema;
	}


	public void setSchema(String schema) {
		this.schema = schema;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getScriptFile() {
		return scriptFile;
	}

	public void setScriptFile(String scriptFile) {
		this.scriptFile = scriptFile;
	}
	
	
}
