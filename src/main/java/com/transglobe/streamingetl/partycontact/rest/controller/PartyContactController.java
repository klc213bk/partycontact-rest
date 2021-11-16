package com.transglobe.streamingetl.partycontact.rest.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.transglobe.streamingetl.partycontact.rest.bean.Response;
import com.transglobe.streamingetl.partycontact.rest.service.PartyContactService;

//@RestController
//@RequestMapping("/partycontact")
public class PartyContactController {
	static final Logger logger = LoggerFactory.getLogger(PartyContactController.class);
	

	@Autowired
	private PartyContactService partyContactService;
	
	@PostMapping(value="/createTable")
	@ResponseBody
	public ResponseEntity<Response> createPartyContactTable() throws Exception{
		logger.info(">>>>createTable");
		long t0 = System.currentTimeMillis();
		
		partyContactService.createPartyContactTable();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>createTable finished, span={}", (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}
	@PostMapping(value="/dropTable")
	@ResponseBody
	public ResponseEntity<Response> dropPartyContactTable() throws Exception{
		logger.info(">>>>dropTable");
		long t0 = System.currentTimeMillis();
		
		partyContactService.dropPartyContactTable();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>dropTable finished, span={}", (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}
	@PostMapping(value="/truncateTable")
	@ResponseBody
	public ResponseEntity<Response> truncatePartyContactTable() throws Exception{
		logger.info(">>>>truncateTable");
		long t0 = System.currentTimeMillis();
		
		partyContactService.truncatePartyContactTable();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>truncateTable finished, span={}", (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}
	@PostMapping(value="/loadData/{table}")
	@ResponseBody
	public ResponseEntity<Response> loadData(@PathVariable("table") String table) throws Exception{
		logger.info(">>>>loadData {}", table);
		long t0 = System.currentTimeMillis();
		
		partyContactService.loadTable(table);
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>loadData {} finished, span={}", table, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}
	@PostMapping(value="/addPrimaryKey")
	@ResponseBody
	public ResponseEntity<Response> addPrimaryKey() throws Exception{
		logger.info(">>>>addPrimaryKey ");
		long t0 = System.currentTimeMillis();
		
		partyContactService.addPrimaryKey();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>addPrimaryKey finished, span={}", (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}
	@PostMapping(value="/createIndex/{columnName}")
	@ResponseBody
	public ResponseEntity<Response> createIndex(@PathVariable("columnName") String columnName) throws Exception{
		logger.info(">>>>createIndex ");
		long t0 = System.currentTimeMillis();
		
		partyContactService.createIndex(columnName);
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>createIndex finished, span={}", (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}


}
