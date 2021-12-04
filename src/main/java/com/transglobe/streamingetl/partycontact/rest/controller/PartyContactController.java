package com.transglobe.streamingetl.partycontact.rest.controller;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.streamingetl.partycontact.rest.bean.Response;
import com.transglobe.streamingetl.partycontact.rest.service.PartyContactService;

@RestController
@RequestMapping("/partycontact")
public class PartyContactController {
	static final Logger logger = LoggerFactory.getLogger(PartyContactController.class);


	@Autowired
	private PartyContactService partyContactService;

	@Autowired
	private ObjectMapper mapper;
	
	@PostMapping(path="/cleanup", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> cleanup() {
		logger.info(">>>>controller cleanup is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			partyContactService.cleanup();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller cleanup finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/initialize", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> initialize() {
		logger.info(">>>>controller initialize is called");
		
		ObjectNode objectNode = mapper.createObjectNode();
		
		try {
			partyContactService.initialize();
			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", ExceptionUtils.getMessage(e));
			objectNode.put("returnCode", ExceptionUtils.getStackTrace(e));
		}
		
		logger.info(">>>>controller initialize finished ");
		
		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	
	@PostMapping(value="/createTable")
	@ResponseBody
	public ResponseEntity<Response> createTable() throws Exception{
		logger.info(">>>>createTable");
		long t0 = System.currentTimeMillis();
		String errMsg = null;
		String returnCode = "0000";
		try {
			partyContactService.createTable();

		} catch (Exception e) {
			returnCode = "-9999";
			errMsg = ExceptionUtils.getMessage(e);
			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
		}

		long t1 = System.currentTimeMillis();

		logger.info(">>>>createTable finished returnCode={}, span={}", returnCode, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	}
	@PostMapping(value="/dropTable")
	@ResponseBody
	public ResponseEntity<Response> dropTable() throws Exception{
		logger.info(">>>>dropTable");
		long t0 = System.currentTimeMillis();
		String errMsg = null;
		String returnCode = "0000";
		try {
			partyContactService.dropTable();

		} catch (Exception e) {
			returnCode = "-9999";
			errMsg = ExceptionUtils.getMessage(e);
			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
		}

		long t1 = System.currentTimeMillis();

		logger.info(">>>>dropTable finished returnCode={}, span={}", returnCode, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	}
	@PostMapping(value="/truncateTable/{table}")
	@ResponseBody
	public ResponseEntity<Response> truncateTable(@PathVariable("table") String table) throws Exception{
		logger.info(">>>>truncateTable");
		long t0 = System.currentTimeMillis();
		String errMsg = null;
		String returnCode = "0000";
		try {
			partyContactService.truncateTable(table);
		} catch (Exception e) {
			returnCode = "-9999";
			errMsg = ExceptionUtils.getMessage(e);
			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
		}

		long t1 = System.currentTimeMillis();

		logger.info(">>>>truncateTable finished returnCode={}, span={}", returnCode, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	}
	@PostMapping(value="/loadData/{table}")
	@ResponseBody
	public ResponseEntity<Response> loadData(@PathVariable("table") String table) throws Exception{
		logger.info(">>>>loadData {}", table);
		long t0 = System.currentTimeMillis();
		String errMsg = null;
		String returnCode = "0000";
		try {
			partyContactService.loadTable(table);
		} catch (Exception e) {
			returnCode = "-9999";
			errMsg = ExceptionUtils.getMessage(e);
			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
		}

		long t1 = System.currentTimeMillis();

		logger.info(">>>>loadData finished returnCode={}, span={}", returnCode, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	}
	@PostMapping(value="/loadAllData")
	@ResponseBody
	public ResponseEntity<Response> loadAllData() throws Exception{
		logger.info(">>>>loadAllData ");
		long t0 = System.currentTimeMillis();
		String errMsg = null;
		String returnCode = "0000";
		Long count = 0L;
		try {

			count = partyContactService.loadAllData();

		} catch (Exception e) {
			returnCode = "-9999";
			errMsg = ExceptionUtils.getMessage(e);
			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
		}

		long t1 = System.currentTimeMillis();

		logger.info(">>>>loadAllData finished count={}, returnCode={}, span={}", count, returnCode, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	}
	@PostMapping(value="/addPrimaryKey")
	@ResponseBody
	public ResponseEntity<Response> addPrimaryKey() throws Exception{
		logger.info(">>>>addPrimaryKey ");
		long t0 = System.currentTimeMillis();
		String errMsg = null;
		String returnCode = "0000";
		try {
			partyContactService.addPrimaryKey();


		} catch (Exception e) {
			returnCode = "-9999";
			errMsg = ExceptionUtils.getMessage(e);
			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
		}

		long t1 = System.currentTimeMillis();

		logger.info(">>>>addPrimaryKey finished returnCode={}, span={}", returnCode, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	}
	@PostMapping(value="/createIndex/{columnName}")
	@ResponseBody
	public ResponseEntity<Response> createIndex(@PathVariable("columnName") String columnName) throws Exception{
		logger.info(">>>>createIndex ");
		long t0 = System.currentTimeMillis();
		String errMsg = null;
		String returnCode = "0000";
		try {
			partyContactService.createIndex(columnName);


		} catch (Exception e) {
			returnCode = "-9999";
			errMsg = ExceptionUtils.getMessage(e);
			logger.error(">>>errMsg:{}, stacktrace={}", errMsg, ExceptionUtils.getStackTrace(e));
		}

		long t1 = System.currentTimeMillis();

		logger.info(">>>>createIndex finished returnCode={}, span={}", returnCode, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response(returnCode, errMsg));
	}


}
