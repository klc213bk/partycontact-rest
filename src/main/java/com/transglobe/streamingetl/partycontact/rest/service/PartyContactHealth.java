package com.transglobe.streamingetl.partycontact.rest.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

import com.transglobe.streamingetl.partycontact.rest.bean.HealthStatus;
import com.transglobe.streamingetl.partycontact.rest.bean.HealthStatus.HealthState;
import com.transglobe.streamingetl.partycontact.rest.bean.PartyContactETL;


@Service
public class PartyContactHealth {
	static final Logger LOG = LoggerFactory.getLogger(PartyContactHealth.class);


	public static final String CLIENT_ID = "partycontact1-1";

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
	public void triggerHealthAlert(Integer notReceivedCnt) {
		LOG.info(">>>>>>>>>>>> triggerHealthAlert is called, {} !!!!...", notReceivedCnt);
		HealthState healthState = null;
		if (notReceivedCnt < 5) {
			healthState = HealthState.ACTIVE;
		} else {
			healthState = HealthState.STANDBY;
		}

		// update health state
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = null;
		try {
			if (tglminerConnPool == null) {
				tglminerConnPool = getConnectionPool();
			} else if (tglminerConnPool.isClosed()) {
				tglminerConnPool.restart();
			}
			LOG.info(">>>>>>>>>>>> update health status ....");
			
			conn = tglminerConnPool.getConnection();
			sql = "update TM_HEALTH_STATUS SET HEALTH_STATE=?,UPDATE_TIMESTAMP=? where ETL_NAME=?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, healthState.name());
			pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			pstmt.setString(3, PartyContactETL.NAME);
			pstmt.executeUpdate();
			pstmt.close();
			
			if (HealthState.ACTIVE == healthState) {
				LOG.info(">>>>>>>>>>>> IT IS HEALTHY, notReceivedCnt={}", notReceivedCnt);
				
			} else if (HealthState.STANDBY == healthState) {
				LOG.info(">>>>>>>>>>>> state ={}, notReceivedCnt={} ", healthState, notReceivedCnt);
				LOG.info(">>>>>>>>>>>>update health state ....");
				
				
				
				
				LOG.info(">>>>>>>>>>>> RESTARTING SYSTEM ....");
			}

		} catch(Exception e) {
			LOG.error(">>> errMsg={}, stacetrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					LOG.error(">>> errMsg={}, stacetrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					LOG.error(">>> errMsg={}, stacetrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}
			}
		}




		LOG.info(">>>>>>>>>>>> triggerHealthAlert  end !!!!...");
	}
	public void startHealthChecker() throws Exception {
		LOG.info(">>>>>>>>>>>> startHealthChecker...");

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
						int notReceivedCnt = checkHealth();

						LOG.info(">>>>>>>>>>>> healthStatus notReceivedCnt={}", notReceivedCnt);
						triggerHealthAlert(notReceivedCnt);

						Thread.sleep(30000);
					}
				} catch (Exception e) {
					LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}
			}
			private Integer checkHealth() throws Exception{
				CallableStatement cstmt = null;
				Connection conn = null;
				try {
					conn = tglminerConnPool.getConnection();
					cstmt = conn.prepareCall("{call SP_GET_CONSUMER_NOT_RECEIVED(?,?)}");
					cstmt.setString(1, CLIENT_ID);
					cstmt.registerOutParameter(2, Types.INTEGER);
					cstmt.execute();
					int notReceivedCnt = cstmt.getInt(2);

					return notReceivedCnt;
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
		connPool.setMaxTotal(2);

		return connPool;
	}

}
