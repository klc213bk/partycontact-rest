package com.transglobe.streamingetl.partycontact.rest.controller;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.transglobe.streamingetl.partycontact.rest.bean.Response;
import com.transglobe.streamingetl.partycontact.rest.service.LoadDataService;
import com.transglobe.streamingetl.partycontact.rest.service.PartyContactService;

@RestController
@RequestMapping("/partycontact")
public class PartyContactController {
	static final Logger logger = LoggerFactory.getLogger(PartyContactController.class);
	

	@Autowired
	private PartyContactService partyContactService;

	@Autowired
	private LoadDataService loadDataService;
	
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
	@PutMapping(value="/loadData/{table}")
	@ResponseBody
	public ResponseEntity<Response> loadData(@PathVariable("table") String table) throws Exception{
		logger.info(">>>>loadData {}", table);
		long t0 = System.currentTimeMillis();
		
		loadDataService.loadTable(table);
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>loadData {} finished, span={}", table, (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}

}
