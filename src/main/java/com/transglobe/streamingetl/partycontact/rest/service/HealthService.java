package com.transglobe.streamingetl.partycontact.rest.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.streamingetl.partycontact.rest.bean.PartyContactETL;


@Service
public class HealthService {
	static final Logger LOG = LoggerFactory.getLogger(HealthService.class);

	private static final String CONSUMER_GROUP = "health";

	public static final String CLIENT_ID = "health-1";

	public static class HealthStatus {
		public String state;
	}
	
	@Value("${tglminer.db.driver}")
	private String tglminerDbDriver;

	@Value("${tglminer.db.url}")
	private String tglminerDbUrl;

	@Value("${tglminer.db.username}")
	private String tglminerDbUsername;

	@Value("${tglminer.db.password}")
	private String tglminerDbPassword;

	@Value("${kafka.rest.url}")
	private String kafkaRestUrl;

	@Value("${kafka.bootstrap.server}")
	private String kafkaBootstrapServer;

	private BasicDataSource tglminerConnPool;

	private ExecutorService executor = null;

	private HealthStatus healthStatus;

	
	public HealthStatus getHealthStatus() {
		return healthStatus;
	}

	public boolean startHealthChecker() throws Exception {
		LOG.info(">>>>>>>>>>>> startHealthChecker...");
		boolean result = true;
		healthStatus = new HealthStatus();
		if (tglminerConnPool == null) {
			tglminerConnPool = getConnectionPool();
		}

		executor = Executors.newSingleThreadExecutor();
		executor.submit(new Runnable() {

			@Override
			public void run() {

				try {
					while (true) {
						String state = checkHealth();
						healthStatus.state = state;
						LOG.info(">>>>>>>>>>>> healthStatus state={}", state);
						Thread.sleep(30000);
					}
				} catch (Exception e) {
					LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}
			}
			private String checkHealth() throws Exception{
				CallableStatement cstmt = null;
				Connection conn = null;
				try {
					conn = tglminerConnPool.getConnection();
					cstmt = conn.prepareCall("{call GET_STREAMING_ETL_STATE(?,?)}");
					cstmt.setString(1, PartyContactETL.NAME);
					cstmt.registerOutParameter(2, Types.VARCHAR);
					cstmt.execute();
					String state = cstmt.getString(2);
					
					return state;
				} catch(Exception e) {
					LOG.error(">>> Response code={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
					throw e;
				} finally {
					if (cstmt != null ) cstmt.close();
					if (conn != null ) conn.close();
				}

			}
		});


		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {

				stopHealthChecker();

			}
		});

		return result;


	}
	
	public boolean stopHealthChecker() {
		LOG.info(">>>>>>>>>>>> stopHeartbeat ");
		boolean result = true;
		if (executor != null) {

			try {
				if (tglminerConnPool != null) tglminerConnPool.close();
			} catch (Exception e) {
				result = false;
				LOG.error(">>>message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}

			executor.shutdown();
			if (!executor.isTerminated()) {
				executor.shutdownNow();

				try {
					executor.awaitTermination(300, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					result = false;
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}

		}

		LOG.info(">>>>>>>>>>>> stopHeartbeat done !!!");

		return result;
	}
	
	private BasicDataSource getConnectionPool() {
		BasicDataSource connPool = new BasicDataSource();
		connPool.setUrl(tglminerDbUrl);
		connPool.setDriverClassName(tglminerDbDriver);
		connPool.setUsername(tglminerDbUsername);
		connPool.setPassword(tglminerDbPassword);
		connPool.setMaxTotal(1);

		return connPool;
	}

}
