package com.transglobe.streamingetl.partycontact.rest.controller;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

	@PostMapping(value="/startIgnite")
	@ResponseBody
	public void startIgnite(){
		logger.info(">>>>startIgnite");
		long t0 = System.currentTimeMillis();
		
		partyContactService.startIgnite();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>startIgnite span={}", (t1 - t0));

	}
	@PostMapping(value="/stopIgnite")
	@ResponseBody
	public void stopIgnite(){
		logger.info(">>>>stopIgnite");
		long t0 = System.currentTimeMillis();
		partyContactService.stopIgnite();
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>stopIgnite span={}", (t1 - t0));

	}

	@PostMapping(value="/loadData")
	@ResponseBody
	public void loadData(){
		logger.info(">>>>loadData");
		long t0 = System.currentTimeMillis();
		
		partyContactService.loadData();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>loadData completed. span={}", (t1 - t0));

	}
	
	@PostMapping(value="/stopLoadData")
	@ResponseBody
	public void stopLoadData(){
		logger.info(">>>>stopLoadData");
		long t0 = System.currentTimeMillis();
		
		partyContactService.stopLoadData();
		
		long t1 = System.currentTimeMillis();
		
		logger.info(">>>>stopLoadData completed. span={}", (t1 - t0));

	}
	

}
