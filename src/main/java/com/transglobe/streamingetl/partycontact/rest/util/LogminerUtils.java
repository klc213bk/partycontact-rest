package com.transglobe.streamingetl.partycontact.rest.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.partycontact.rest.bean.ApplyLogminerSync;
import com.transglobe.streamingetl.partycontact.rest.bean.URLConstants;

public class LogminerUtils {
	static final Logger LOG = LoggerFactory.getLogger(LogminerUtils.class);
	
	public static String restartLogminerConnector(ApplyLogminerSync applySync) throws Exception{
		LOG.info(">>>>>>> restartLogminerConnector ...");

		String applySyncUrl = URLConstants.URL_APPLY_LOGMINER_SYNC;

		ObjectMapper mapper = new ObjectMapper();
		String jsonStr = mapper.writeValueAsString(applySync);

		LOG.info(">>>>>>> applySyncUrl={}, jsonStr={}", applySyncUrl, jsonStr); 
		String response = HttpUtils.restPostService(applySyncUrl, jsonStr);

		LOG.info(">>>>>>> applyLogminerSync response={}", response);

		return response;
	}
}
