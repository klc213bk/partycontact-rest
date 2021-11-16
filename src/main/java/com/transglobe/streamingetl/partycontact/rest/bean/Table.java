package com.transglobe.streamingetl.partycontact.rest.bean;

public class Table {
	// source table
	public static String T_POLICY_HOLDER = "T_POLICY_HOLDER";
	public static String T_INSURED_LIST = "T_INSURED_LIST";
	public static String T_CONTRACT_BENE = "T_CONTRACT_BENE";
	public static String T_POLICY_HOLDER_LOG = "T_POLICY_HOLDER_LOG";
	public static String T_INSURED_LIST_LOG = "T_INSURED_LIST_LOG";
	public static String T_CONTRACT_BENE_LOG = "T_CONTRACT_BENE_LOG";
	public static String T_ADDRESS = "T_ADDRESS";
	public static String T_CONTRACT_MASTER = "T_CONTRACT_MASTER";
	public static String T_POLICY_CHANGE = "T_POLICY_CHANGE";
	
	// sink table
	public static String T_PARTY_CONTACT = "T_PARTY_CONTACT"; 
	
	// streamingetl table
	public static String STREAMING_ETL = "STREAMING_ETL";
	public static String LOGMINER_HEARTBEAT = "LOGMINER_HEARTBEAT";
	public static String STREAMING_HEALTH = "STREAMING_HEALTH";
}
