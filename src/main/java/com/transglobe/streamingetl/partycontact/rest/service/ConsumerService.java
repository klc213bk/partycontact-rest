package com.transglobe.streamingetl.partycontact.rest.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

	@Value("${kafka.consumer.topics.default}")
	private String kafkaConsumerTopicsDefault;
	
	@Value("${kafka.consumer.topics.partycontact}")
	private String kafkaConsumerTopicsPartycontact;
	
	@Value("${heartbeat.table}")
	private String heartbeatTable;

	private BasicDataSource sinkConnPool;
	private BasicDataSource sourceConnPool;
	private BasicDataSource tglminerConnPool;

	private ExecutorService executor = null;

	private Consumer consumer = null;
	
	private Boolean withPartyContactSync;
	
	
	
	public Boolean getWithPartyContactSync() {
		return withPartyContactSync;
	}
	public void startPartyContactConsumer() throws Exception {
		
		List<String> topicList = new ArrayList<>();
		String[] topicArr = kafkaConsumerTopicsDefault.split(",");
		for (String e : topicArr) {
			topicList.add(e);
		}
		
		String[] pcTopicArr = kafkaConsumerTopicsPartycontact.split(",");
		for (String e : pcTopicArr) {
			topicList.add(e);
		}
		
		LOG.info(">>>>>>>>>>>> startPartyContactConsumer:{} ", String.join(",", topicList));
		
		withPartyContactSync = Boolean.TRUE;
		start(topicList);
	}
	public void startDefaultConsumer() throws Exception {
		String[] topicArr = kafkaConsumerTopicsDefault.split(",");
		List<String> topicList = Arrays.asList(topicArr);
		
		LOG.info(">>>>>>>>>>>> startDefaultConsumer:{} ", String.join(",", topicList));
		
		withPartyContactSync = Boolean.FALSE;
		start(topicList);
	}
	public void start(List<String> topicList) throws Exception {
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

		
		// get scnLowmark, scnHighmark
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		long scnLowMark = 0L;
		long scnHighMark = 0L;
		try {
			conn = sinkConnPool.getConnection();
			sql = "select MAX(ROLE_SCN) MAX_ROLE_SCN, MAX(ADDR_SCN), MAX_ADDR_SCN, \n" +
					" MIN(ROLE_SCN) MIN_ROLE_SCN, MIN(ADDR_SCN) MIN_ADDR_SCN from T_PARTY_CONTACT";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			long maxRoleScn = 0L;
			long maxAddrScn = 0L;
			long minRoleScn = 0L;
			long minAddrScn = 0L;
			while (rs.next()) {
				maxRoleScn = rs.getLong("MAX_ROLE_SCN");
				maxAddrScn = rs.getLong("MAX_ADDR_SCN");
				minRoleScn = rs.getLong("MIN_ROLE_SCN");
				minAddrScn = rs.getLong("MIN_ADDR_SCN");
			}
			scnHighMark = (maxRoleScn > maxAddrScn)? maxRoleScn : maxAddrScn;
			scnLowMark = (minRoleScn < minAddrScn)? minRoleScn : minAddrScn;
		} finally {
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}
		LOG.info(">>>>>>>>>>>> scnLowMark={},scnHighMark={} ", scnLowMark, scnHighMark);
		
		executor = Executors.newFixedThreadPool(1);

		//		String groupId1 = config.groupId1;
		consumer = new Consumer(1, CONSUMER_GROUP_1, kafkaBootstrapServer, topicList, 
				sourceConnPool, sinkConnPool, tglminerConnPool, heartbeatTable, scnLowMark, scnHighMark);
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
