package com.transglobe.streamingetl.partycontact.rest.bean;

public class Response {

private String returnCode;
	
	private String errMsg;

	public Response() {}
	
	public Response(String returnCode) {
		this.returnCode = returnCode;
		this.errMsg = null;
	}
	public Response(String returnCode, String errMsg) {
		this.returnCode = returnCode;
		this.errMsg = errMsg;
	}
	public String getReturnCode() {
		return returnCode;
	}

	public void setReturnCode(String returnCode) {
		this.returnCode = returnCode;
	}

	public String getErrMsg() {
		return errMsg;
	}

	public void setErrMsg(String errMsg) {
		this.errMsg = errMsg;
	}
}
