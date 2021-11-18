package com.transglobe.streamingetl.partycontact.rest.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.transglobe.streamingetl.partycontact.rest.bean.KafkaConsumerState;
import com.transglobe.streamingetl.partycontact.rest.bean.Response;
import com.transglobe.streamingetl.partycontact.rest.service.ConsumerService;

@RestController
@RequestMapping("/consumer")
public class ConsumerController {
	static final Logger logger = LoggerFactory.getLogger(ConsumerController.class);

	@Autowired
	private ConsumerService consumerService;
	
	@PostMapping(value="/start")
	@ResponseBody
	public ResponseEntity<Response> start() throws Exception{
		logger.info(">>>>start");
		long t0 = System.currentTimeMillis();
		
		consumerService.start();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>start finished, span={}", (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}
	@PostMapping(value="/shutdown")
	@ResponseBody
	public ResponseEntity<Response> shutdown() throws Exception{
		logger.info(">>>>shutdown");
		long t0 = System.currentTimeMillis();
		
		consumerService.shutdown();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>shutdown finished, span={}", (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(new Response("0000"));
	}
	
	@GetMapping(value="/consumerState")
	@ResponseBody
	public ResponseEntity<KafkaConsumerState> consumerState() throws Exception{
		long t0 = System.currentTimeMillis();
		
		KafkaConsumerState consumerState = consumerService.consumerState();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>consumerState finished state={}, span={}", consumerState.getState(), (t1 - t0));

		return ResponseEntity.status(HttpStatus.OK).body(consumerState);
	}
}
