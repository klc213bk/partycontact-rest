package com.transglobe.streamingetl.partycontact.rest.controller;

import org.apache.commons.lang3.exception.ExceptionUtils;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.streamingetl.partycontact.rest.bean.KafkaConsumerState;
import com.transglobe.streamingetl.partycontact.rest.bean.Response;
import com.transglobe.streamingetl.partycontact.rest.service.ConsumerService;

@RestController
@RequestMapping("/consumer")
public class ConsumerController {
	static final Logger logger = LoggerFactory.getLogger(ConsumerController.class);

	@Autowired
	private ConsumerService consumerService;

	@Autowired
	private ObjectMapper mapper;

	@GetMapping(value="/withPartyContactSync")
	@ResponseBody
	public ResponseEntity<Object> withPartyContactSync() throws Exception{
		logger.info(">>>>withPartyContactSync");
		ObjectNode objectNode = mapper.createObjectNode();

		try {
			Boolean withSync = consumerService.getWithPartyContactSync();
			objectNode.put("returnCode", "0000");
			objectNode.put("withSync", withSync);
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		logger.info(">>>>controller withPartyContactSync finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);

	}

	@PostMapping(value="/startDefaultConsumer")
	@ResponseBody
	public ResponseEntity<Object> startDefaultConsumer() throws Exception{
		logger.info(">>>>startDefaultConsumer");
		ObjectNode objectNode = mapper.createObjectNode();

		try {
			consumerService.startDefaultConsumer();

			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		logger.info(">>>>controller startDefaultConsumer finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/startPartyContactConsumer")
	@ResponseBody
	public ResponseEntity<Object> startPartyContactConsumer() throws Exception{
		logger.info(">>>>startPartyContactConsumer");
		
		ObjectNode objectNode = mapper.createObjectNode();

		try {
			consumerService.startPartyContactConsumer();

			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		logger.info(">>>>controller startPartyContactConsumer finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(value="/shutdown")
	@ResponseBody
	public ResponseEntity<Object> shutdown() throws Exception{
		logger.info(">>>>shutdown");
		
		ObjectNode objectNode = mapper.createObjectNode();

		try {
			consumerService.shutdown();

			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
		}

		logger.info(">>>>controller shutdown finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}


}
