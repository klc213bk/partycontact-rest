package com.transglobe.streamingetl.partycontact.rest.service;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.streamingetl.partycontact.rest.bean.KafkaConsumerState;



@Service
public class ConsumerService {
	static final Logger LOG = LoggerFactory.getLogger(ConsumerService.class);

	private static final String CONSUMER_GROUP_1 = "partycontact1";

	@Value("${streaming.etl.name}")
	private String streamingEtlName;

	@Value("${source.db.driver}")
	private String sourceDbDriver;

	@Value("${source.db.url}")
	private String sourceDbUrl;

	@Value("${source.db.username}")
	private String sourceDbUsername;

	@Value("${source.db.password}")
	private String sourceDbPassword;

	@Value("${sink.db.driver}")
	private String sinkDbDriver;

	@Value("${sink.db.url}")
	private String sinkDbUrl;

	@Value("${sink.db.username}")
	private String sinkDbUsername;

	@Value("${sink.db.password}")
	private String sinkDbPassword;

	@Value("${logminer.db.driver}")
	private String logminerDbDriver;

	@Value("${logminer.db.url}")
	private String logminerDbUrl;

	@Value("${logminer.db.username}")
	private String logminerDbUsername;

	@Value("${logminer.db.password}")
	private String logminerDbPassword;

	@Value("${bootstrap.servers}")
	private String bootStrapServers;

	@Value("${topics}")
	private String topics;

	@Value("${streamingetl.rest.api}")
	private String streamingetlRestApi;

	private BasicDataSource sinkConnPool;
	private BasicDataSource sourceConnPool;
	private BasicDataSource logminerConnPool;

	ExecutorService executor = null;

	List<Consumer> consumers = null;

	public void start() throws Exception {
		LOG.info(">>>>>>>>>>>> start ");

		sourceConnPool = new BasicDataSource();
		sourceConnPool.setUrl(sourceDbUrl);
		sourceConnPool.setUsername(sourceDbUsername);
		sourceConnPool.setPassword(sourceDbPassword);
		sourceConnPool.setDriverClassName(sourceDbDriver);
		sourceConnPool.setMaxTotal(3);

		sinkConnPool = new BasicDataSource();
		sinkConnPool.setUrl(sinkDbUrl);
		sinkConnPool.setUsername(sinkDbUsername);
		sinkConnPool.setPassword(sinkDbPassword);
		sinkConnPool.setDriverClassName(sinkDbDriver);
		sinkConnPool.setMaxTotal(3);

		logminerConnPool = new BasicDataSource();
		logminerConnPool.setUrl(logminerDbUrl);
		logminerConnPool.setDriverClassName(logminerDbDriver);
		logminerConnPool.setUsername(logminerDbUsername);
		logminerConnPool.setPassword(logminerDbPassword);
		logminerConnPool.setMaxTotal(1);

		String[] topicArr = topics.split(",");
		List<String> topicList = Arrays.asList(topicArr);

		executor = Executors.newFixedThreadPool(1);

		consumers = new ArrayList<>();
		//		String groupId1 = config.groupId1;
		Consumer consumer = new Consumer(1, CONSUMER_GROUP_1, bootStrapServers, topicList, sourceConnPool, sinkConnPool, logminerConnPool);
		consumers.add(consumer);
		executor.submit(consumer);

		LOG.info(">>>>>log consumer running");
		logComsumerState(KafkaConsumerState.RUNNING);

		LOG.info(">>>>>>>>>>>> started Done!!!");

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {

				shutdown();

			}
		});

	}
	public void shutdown() {
		LOG.info(">>>>>>>>>>>> shutdown ");
		if (executor != null && consumers != null) {
			for (Consumer consumer : consumers) {
				consumer.shutdown();
			} 

			try {
				if (sourceConnPool != null) sourceConnPool.close();
				if (sinkConnPool != null) sinkConnPool.close();
				if (logminerConnPool != null) logminerConnPool.close();
			} catch (Exception e) {
				LOG.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}

			executor.shutdown();
			if (!executor.isTerminated()) {
				executor.shutdownNow();

				try {
					executor.awaitTermination(3000, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}
			LOG.info(">>>>>log consumer running");

			try {
				logComsumerState(KafkaConsumerState.STOPPED);
			} catch (Exception e) {
				LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
						ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				try {
					logComsumerState(KafkaConsumerState.ERROR);
				} catch (Exception e1) {
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}
		}

		LOG.info(">>>>>>>>>>>> shutdown done !!!");
	}
	private void logComsumerState(String state) throws Exception{

		String urlStr = String.format("%s/streamingetl/logConsumerState/%s/%s", streamingetlRestApi,streamingEtlName, state);
		LOG.info(">>>>> connector urlStr:" + urlStr);
		HttpURLConnection httpConn = null;
		try {
			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("POST");
			int responseCode = httpConn.getResponseCode();
			LOG.info(">>>>> logComsumerState  responseCode={}",responseCode);

		} finally {
			if (httpConn != null ) httpConn.disconnect();
		}
	}
}
