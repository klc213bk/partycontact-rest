package com.transglobe.streamingetl.partycontact.rest.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PartyContact {

	private Integer roleType;
	
	@JsonProperty("LIST_ID")
	private Long listId;
	
	@JsonProperty("POLICY_ID")
	private Long policyId;
	
	@JsonProperty("NAME")
	private String name;
	
	@JsonProperty("CERTI_CODE")
	private String certiCode;
	
	@JsonProperty("MOBILE_TEL")
	private String mobileTel;
	
	@JsonProperty("EMAIL")
	private String email;
	
	@JsonProperty("ADDRESS_ID")
	private Long addressId;
	
	@JsonProperty("ADDRESS_1")
	private String address1;
	
	/** for Log 表*/
	@JsonProperty("LAST_CMT_FLG")
	private String lastCmtFlg;
	
	/** for Log 表*/
	@JsonProperty("POLICY_CHG_ID")
	private Long policyChgId;
	
	public Integer getRoleType() {
		return roleType;
	}

	public void setRoleType(Integer roleType) {
		this.roleType = roleType;
	}

	public Long getListId() {
		return listId;
	}

	public void setListId(Long listId) {
		this.listId = listId;
	}

	public Long getPolicyId() {
		return policyId;
	}

	public void setPolicyId(Long policyId) {
		this.policyId = policyId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCertiCode() {
		return certiCode;
	}

	public void setCertiCode(String certiCode) {
		this.certiCode = certiCode;
	}

	public String getMobileTel() {
		return mobileTel;
	}

	public void setMobileTel(String mobileTel) {
		this.mobileTel = mobileTel;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Long getAddressId() {
		return addressId;
	}

	public void setAddressId(Long addressId) {
		this.addressId = addressId;
	}

	public String getAddress1() {
		return address1;
	}

	public void setAddress1(String address1) {
		this.address1 = address1;
	}

	public String getLastCmtFlg() {
		return lastCmtFlg;
	}

	public void setLastCmtFlg(String lastCmtFlg) {
		this.lastCmtFlg = lastCmtFlg;
	}

	public Long getPolicyChgId() {
		return policyChgId;
	}

	public void setPolicyChgId(Long policyChgId) {
		this.policyChgId = policyChgId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((address1 == null) ? 0 : address1.hashCode());
		result = prime * result + ((addressId == null) ? 0 : addressId.hashCode());
		result = prime * result + ((certiCode == null) ? 0 : certiCode.hashCode());
		result = prime * result + ((email == null) ? 0 : email.hashCode());
		result = prime * result + ((lastCmtFlg == null) ? 0 : lastCmtFlg.hashCode());
		result = prime * result + ((listId == null) ? 0 : listId.hashCode());
		result = prime * result + ((mobileTel == null) ? 0 : mobileTel.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((policyChgId == null) ? 0 : policyChgId.hashCode());
		result = prime * result + ((policyId == null) ? 0 : policyId.hashCode());
		result = prime * result + ((roleType == null) ? 0 : roleType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PartyContact other = (PartyContact) obj;
		if (address1 == null) {
			if (other.address1 != null)
				return false;
		} else if (!address1.equals(other.address1))
			return false;
		if (addressId == null) {
			if (other.addressId != null)
				return false;
		} else if (!addressId.equals(other.addressId))
			return false;
		if (certiCode == null) {
			if (other.certiCode != null)
				return false;
		} else if (!certiCode.equals(other.certiCode))
			return false;
		if (email == null) {
			if (other.email != null)
				return false;
		} else if (!email.equals(other.email))
			return false;
		if (lastCmtFlg == null) {
			if (other.lastCmtFlg != null)
				return false;
		} else if (!lastCmtFlg.equals(other.lastCmtFlg))
			return false;
		if (listId == null) {
			if (other.listId != null)
				return false;
		} else if (!listId.equals(other.listId))
			return false;
		if (mobileTel == null) {
			if (other.mobileTel != null)
				return false;
		} else if (!mobileTel.equals(other.mobileTel))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (policyChgId == null) {
			if (other.policyChgId != null)
				return false;
		} else if (!policyChgId.equals(other.policyChgId))
			return false;
		if (policyId == null) {
			if (other.policyId != null)
				return false;
		} else if (!policyId.equals(other.policyId))
			return false;
		if (roleType == null) {
			if (other.roleType != null)
				return false;
		} else if (!roleType.equals(other.roleType))
			return false;
		return true;
	}
	
	
}
