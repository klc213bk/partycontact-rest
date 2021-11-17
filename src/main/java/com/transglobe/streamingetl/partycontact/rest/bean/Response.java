package com.transglobe.streamingetl.partycontact.rest.bean;

public class Response {

	private String returnCode;

	public Response() {}
	
	public Response(String returnCode) {
		this.returnCode = returnCode;
	}
	public String getReturnCode() {
		return returnCode;
	}

	public void setReturnCode(String returnCode) {
		this.returnCode = returnCode;
	}
	
	
}
