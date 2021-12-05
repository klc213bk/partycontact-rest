package com.transglobe.streamingetl.partycontact.rest.service;

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

	@Value("${tglminer.db.driver}")
	private String tglminerDbDriver;

	@Value("${tglminer.db.url}")
	private String tglminerDbUrl;

	@Value("${tglminer.db.username}")
	private String tglminerDbUsername;

	@Value("${tglminer.db.password}")
	private String tglminerDbPassword;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;

	@Value("${kafka.consumer.topics}")
	private String kafkaConsumerTopics;
	
	@Value("${heartbeat.table}")
	private String heartbeatTable;

	private BasicDataSource sinkConnPool;
	private BasicDataSource sourceConnPool;
	private BasicDataSource tglminerConnPool;

	private ExecutorService executor = null;

	private Consumer consumer = null;

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

		tglminerConnPool = new BasicDataSource();
		tglminerConnPool.setUrl(tglminerDbUrl);
		tglminerConnPool.setDriverClassName(tglminerDbDriver);
		tglminerConnPool.setUsername(tglminerDbUsername);
		tglminerConnPool.setPassword(tglminerDbPassword);
		tglminerConnPool.setMaxTotal(1);

		String[] topicArr = kafkaConsumerTopics.split(",");
		List<String> topicList = Arrays.asList(topicArr);

		executor = Executors.newFixedThreadPool(1);

		//		String groupId1 = config.groupId1;
		consumer = new Consumer(1, CONSUMER_GROUP_1, kafkaBootstrapServer, topicList, sourceConnPool, sinkConnPool, tglminerConnPool, heartbeatTable);
		executor.submit(consumer);

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
		if (executor != null && consumer != null) {
			consumer.shutdown();

			try {
				if (sourceConnPool != null) sourceConnPool.close();
				if (sinkConnPool != null) sinkConnPool.close();
				if (tglminerConnPool != null) tglminerConnPool.close();
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
			
		}

		LOG.info(">>>>>>>>>>>> shutdown done !!!");
	}
	public boolean isConsumerClosed() throws Exception {
		LOG.info(">>>>>>>>>>>> isConsumerClosed ");
		
		if (executor == null || executor.isTerminated()) {
			return true;
		} else {
			if (consumer == null) {
				return true;
			} else {
				return consumer.isConsumerClosed();
			}
		}

	}
}
