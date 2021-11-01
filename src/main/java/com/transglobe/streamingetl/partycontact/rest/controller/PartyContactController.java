package com.transglobe.streamingetl.partycontact.rest.controller;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import com.transglobe.streamingetl.partycontact.rest.service.PartyContactService;

@RestController
@RequestMapping("/partycontact")
public class PartyContactController {
	static final Logger logger = LoggerFactory.getLogger(PartyContactController.class);
	

	@Autowired
	private PartyContactService partyContactService;

	@PostMapping(value="/startIgnite1")
	@ResponseBody
	public void startIgnite1(){
		logger.info(">>>>startIgnite1");
		long t0 = System.currentTimeMillis();
		
		partyContactService.startIgnite1();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>startIgnite1 span={}", (t1 - t0));

	}
	@PostMapping(value="/stopIgnite1")
	@ResponseBody
	public void stopIgnite1(){
		logger.info(">>>>stopIgnite1");
		long t0 = System.currentTimeMillis();
		partyContactService.stopIgnite1();
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>stopIgnite1 span={}", (t1 - t0));

	}

	@PostMapping(value="/loadData1")
	@ResponseBody
	public void loadData1(){
		logger.info(">>>>loadData1");
		long t0 = System.currentTimeMillis();
		
		partyContactService.loadData1();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>loadData1 completed. span={}", (t1 - t0));

	}
	
	@PostMapping(value="/stopLoadData1")
	@ResponseBody
	public void stopLoadData1(){
		logger.info(">>>>stopLoadData1");
		long t0 = System.currentTimeMillis();
		
		partyContactService.stopLoadData1();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>stopLoadData1 completed. span={}", (t1 - t0));

	}
	@PostMapping(value="/loadData2")
	@ResponseBody
	public void loadData2(){
		logger.info(">>>>loadData2");
		long t0 = System.currentTimeMillis();
		
		partyContactService.loadData2();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>loadData2 completed. span={}", (t1 - t0));

	}
	
	@PostMapping(value="/lstopLoadData2")
	@ResponseBody
	public void stopLoadData2(){
		logger.info(">>>>stopLoadData2");
		long t0 = System.currentTimeMillis();
		
		partyContactService.stopLoadData2();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>stopLoadData2 completed. span={}", (t1 - t0));

	}
	@PostMapping(value="/startIgnite2")
	@ResponseBody
	public void startIgnite2(){
		logger.info(">>>>startIgnite2");
		long t0 = System.currentTimeMillis();
		
		partyContactService.startIgnite2();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>startIgnite2 span={}", (t1 - t0));

	}
	@PostMapping(value="/stopIgnite2")
	@ResponseBody
	public void stopIgnite2(){
		logger.info(">>>>stopIgnite2");
		long t0 = System.currentTimeMillis();
		partyContactService.stopIgnite2();
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>stopIgnite2 span={}", (t1 - t0));

	}
	

}
