package com.transglobe.streamingetl.partycontact.rest.controller;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.streamingetl.partycontact.rest.bean.Response;
import com.transglobe.streamingetl.partycontact.rest.service.ConsumerService;
import com.transglobe.streamingetl.partycontact.rest.service.PartyContactService;

@RestController
@RequestMapping("/partycontact")
public class PartyContactController {
	static final Logger LOG = LoggerFactory.getLogger(PartyContactController.class);


	@Autowired
	private PartyContactService partyContactService;

	@Autowired
	private ObjectMapper mapper;
	
	
	@PostMapping(path="/cleanup", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> cleanup() {
		LOG.info(">>>>controller cleanup is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			partyContactService.cleanup();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller cleanup finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/initialize", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> initialize() {
		LOG.info(">>>>controller initialize is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			partyContactService.initialize();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller initialize finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
//	@PostMapping(path="/runPartyContact", produces=MediaType.APPLICATION_JSON_VALUE)
//	@ResponseBody
//	public ResponseEntity<Object> runPartyContact() {
//		LOG.info(">>>>controller runPartyContact is called");
//		
//		ObjectNode objectNode = mapper.createObjectNode();
//		
//		try {
//			partyContactService.runPartyContact();
//			objectNode.put("returnCode", "0000");
//		} catch (Exception e) {
//			String errMsg = ExceptionUtils.getMessage(e);
//			String stackTrace = ExceptionUtils.getStackTrace(e);
//			objectNode.put("returnCode", "-9999");
//			objectNode.put("errMsg", errMsg);
//			objectNode.put("returnCode", stackTrace);
//			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
//		}
//		
//		LOG.info(">>>>controller runPartyContact finished ");
//		
//		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
//	}
	@PostMapping(path="/applyLogminerSync", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> applyLogminerSync() {
		LOG.info(">>>>controller applyLogminerSync is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			String configMap = partyContactService.applyLogminerSync(null, 1);
			objectNode.put("returnCode", "0000");
			objectNode.put("configMap", configMap);
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller applyLogminerSync finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/dropLogminerSync", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> dropLogminerSync() {
		LOG.info(">>>>controller dropLogminerSync is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			String configMap = partyContactService.applyLogminerSync(null, -1);
			objectNode.put("returnCode", "0000");
			objectNode.put("configMap", configMap);
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}
		
		LOG.info(">>>>controller dropLogminerSync finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
//	@PostMapping(path="/stopPartyContact", produces=MediaType.APPLICATION_JSON_VALUE)
//	@ResponseBody
//	public ResponseEntity<Object> stopPartyContact() {
//		LOG.info(">>>>controller stopPartyContact is called");
//		
//		ObjectNode objectNode = mapper.createObjectNode();
//		
//		try {
//			partyContactService.stopPartyContact();
//			objectNode.put("returnCode", "0000");
//		} catch (Exception e) {
//			String errMsg = ExceptionUtils.getMessage(e);
//			String stackTrace = ExceptionUtils.getStackTrace(e);
//			objectNode.put("returnCode", "-9999");
//			objectNode.put("errMsg", errMsg);
//			objectNode.put("returnCode", stackTrace);
//			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
//		}
//		
//		LOG.info(">>>>controller stopPartyContact finished ");
//		
//		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
//	}
//	
	
	@PostMapping(value="/truncateTable/{table}")
	@ResponseBody
	public ResponseEntity<Object> truncateTable(@PathVariable("table") String table) throws Exception{
		LOG.info(">>>>truncateTable");
		ObjectNode objectNode = mapper.createObjectNode();
		try {
			partyContactService.truncateTable(table);
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller truncateTable finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	
	@PostMapping(value="/loadData/{table}")
	@ResponseBody
	public ResponseEntity<Object> loadData(@PathVariable("table") String table) throws Exception{
		LOG.info(">>>>loadData {}", table);
		ObjectNode objectNode = mapper.createObjectNode();
		try {
			partyContactService.loadTable(table);
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller loadData finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/loadAllData")
	@ResponseBody
	public ResponseEntity<Object> loadAllData() throws Exception{
		LOG.info(">>>>loadAllData ");
		ObjectNode objectNode = mapper.createObjectNode();
		try {

			long count = partyContactService.loadAllData();
			objectNode.put("returnCode", "0000");
			objectNode.put("count", count);
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller loadAllData finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/addPrimaryKey")
	@ResponseBody
	public ResponseEntity<Object> addPrimaryKey() throws Exception{
		LOG.info(">>>>addPrimaryKey ");
		ObjectNode objectNode = mapper.createObjectNode();
		try {
			partyContactService.addPrimaryKey();
			objectNode.put("returnCode", "0000");

		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller addPrimaryKey finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/disablePrimaryKey")
	@ResponseBody
	public ResponseEntity<Object> disablePrimaryKey() throws Exception{
		LOG.info(">>>>disablePrimaryKey ");
		ObjectNode objectNode = mapper.createObjectNode();
		try {
			partyContactService.disablePrimaryKey();
			objectNode.put("returnCode", "0000");

		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller disablePrimaryKey finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/enablePrimaryKey")
	@ResponseBody
	public ResponseEntity<Object> enablePrimaryKey() throws Exception{
		LOG.info(">>>>enablePrimaryKey ");
		ObjectNode objectNode = mapper.createObjectNode();
		try {
			partyContactService.enablePrimaryKey();
			objectNode.put("returnCode", "0000");

		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller enablePrimaryKey finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	
//	@PostMapping(value="/createIndexes")
//	@ResponseBody
//	public ResponseEntity<Object> createIndex() throws Exception{
//		LOG.info(">>>>createIndex ");
//		ObjectNode objectNode = mapper.createObjectNode();
//		try {
//			partyContactService.createIndexes();
//			objectNode.put("returnCode", "0000");
//
//		} catch (Exception e) {
//			String errMsg = ExceptionUtils.getMessage(e);
//			String stackTrace = ExceptionUtils.getStackTrace(e);
//			objectNode.put("returnCode", "-9999");
//			objectNode.put("errMsg", errMsg);
//			objectNode.put("returnCode", stackTrace);
//			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
//		}
//
//		LOG.info(">>>>controller createIndexes finished ");
//		
//		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
//	}
	@PostMapping(value="/createIndex/{columnName}")
	@ResponseBody
	public ResponseEntity<Object> createIndex(@PathVariable("columnName") String columnName) throws Exception{
		LOG.info(">>>>createIndex ");
		ObjectNode objectNode = mapper.createObjectNode();
		try {
			partyContactService.createIndex(columnName);
			objectNode.put("returnCode", "0000");

		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller createIndex finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/createIndexes")
	@ResponseBody
	public ResponseEntity<Object> createIndexes() throws Exception{
		LOG.info(">>>>createIndex ");
		ObjectNode objectNode = mapper.createObjectNode();
		try {
			partyContactService.createIndexes();
			objectNode.put("returnCode", "0000");

		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller createIndexes finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/dropIndexes")
	@ResponseBody
	public ResponseEntity<Object> dropIndexes() throws Exception{
		LOG.info(">>>>createIndex ");
		ObjectNode objectNode = mapper.createObjectNode();
		try {
			partyContactService.dropIndexes();
			objectNode.put("returnCode", "0000");

		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			LOG.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		LOG.info(">>>>controller dropIndexes finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}


}
