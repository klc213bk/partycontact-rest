package com.transglobe.streamingetl.partycontact.rest.service;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.transglobe.streamingetl.partycontact.rest.bean.ApplyLogminerSync;
import com.transglobe.streamingetl.partycontact.rest.bean.HealthStatus;
import com.transglobe.streamingetl.partycontact.rest.bean.HealthStatus.HealthState;
import com.transglobe.streamingetl.partycontact.rest.util.LogminerUtils;
import com.transglobe.streamingetl.partycontact.rest.bean.PartyContactETL;
import com.transglobe.streamingetl.partycontact.rest.bean.TMHealth;


@Service
public class PartyContactHealth {
	static final Logger LOG = LoggerFactory.getLogger(PartyContactHealth.class);

	public static final String HEALTH_CLIENT_ID = "health-1";
	public static final String PARTY_CONTACT_CLIENT_ID = "partycontact1-1";

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

	private AtomicBoolean logminerStarting = new AtomicBoolean(false);

	public HealthStatus getHealthStatus() {
		return healthStatus;
	}
	public void triggerHealthAlert(TMHealth tmHealth) {
		if (tmHealth == null || tmHealth.getConsumerReceived() == null) {
			LOG.error(">>>>>>>>>>>> triggerHealthAlert tmHealth IS NULL, return");
			return;
		} else {
			LOG.info(">>>>>>>>>>>> triggerHealthAlert is called, {} !!!!...",ToStringBuilder.reflectionToString(tmHealth));
		}

		Timestamp lastReceived = tmHealth.getConsumerReceived();
		long lastReceivedMs = lastReceived.getTime();
		long nowms = System.currentTimeMillis();

		// update health state
		Connection conn = null;
		PreparedStatement pstmt = null;
		String sql = null;
		try {
			
			HealthState healthState = HealthState.STANDBY;
			if (nowms - lastReceivedMs < 5 * 60 * 1000 ) {
				healthState = HealthState.ACTIVE;
			} 

			LOG.error(">>>>>>>>>>>> consumer healthState={}, now={}, lastReceived={}", 
					healthState, new Timestamp(nowms), lastReceived);



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
				LOG.info(">>>>>>>>>>>> IT IS HEALTHY");

			} else if (HealthState.STANDBY == healthState) {

				Timestamp startTime = getConnectorStartTime();

				TMHealth logminerTMHealth = getLogminerLastReceived();

				Timestamp logminerLastReceived = logminerTMHealth.getLogminerReceiveed();
				long logminerLastReceivedMs = logminerLastReceived.getTime();

				LOG.error(">>>>>>>>>>>> logminer  now={}, logminerLastReceived={}, startTime={}", 
						new Timestamp(nowms), logminerLastReceived, startTime);

				if (nowms - logminerLastReceivedMs > 5 * 60 * 1000 
						&&  nowms - startTime.getTime() > 10 * 60 * 1000) {
					LOG.info(">>>>>>>>>>>> RESTARTING SYSTEM ....");
					if (logminerStarting.get()) {
						LOG.info(">>>>>>>>>>>> Logminer is still starting ... skip");

					} else {
						LOG.info(">>>>>>>>>>>> RESTARTING CONNECTOR with resetoffset = false, no apply/drop sync tables");
						logminerStarting.set(true);

						String tableListStr = "";

						ApplyLogminerSync applySync = new ApplyLogminerSync();
						applySync.setResetOffset(false);
						applySync.setStartScn(null);
						applySync.setApplyOrDrop(0);
						applySync.setTableListStr(tableListStr);

						String response = LogminerUtils.restartLogminerConnector(applySync);

						logminerStarting.set(false);
						LOG.info(">>>>>>>>>>>> restartLogminerConnector status={}", response);

					}
				}  else {
					LOG.info(">>>>>>>>>>>> logminerLastReceived < 5 min or time to prev start < 10 min, Chack later.");
				}


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
	private Timestamp getConnectorStartTime() throws Exception{
		PreparedStatement pstmt = null;
		Connection conn = null;
		ResultSet rs = null;
		String sql = null;
		try {
			conn = tglminerConnPool.getConnection();
			sql = "select START_TIME from TM_LOGMINER_OFFSET order by START_TIME desc fetch next 1 row only";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			Timestamp startTime = null;
			while (rs.next()) {
				startTime = rs.getTimestamp("START_TIME");

			}
			rs.close();
			pstmt.close();
			conn.close();

			return startTime;
		} catch(Exception e) {
			LOG.error(">>> Response code={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} finally {
			if (rs != null ) rs.close();
			if (pstmt != null ) pstmt.close();
			if (conn != null ) conn.close();
		}
	}	
	private TMHealth getLogminerLastReceived() throws Exception{
		CallableStatement cstmt = null;
		Connection conn = null;
		try {
			conn = tglminerConnPool.getConnection();
			cstmt = conn.prepareCall("{call SP_GET_LOGMINER_LAST_RECEIVED(?)}");
			cstmt.registerOutParameter(1, oracle.jdbc.OracleTypes.CURSOR);
			cstmt.execute();
			ResultSet rs = (ResultSet)cstmt.getObject(1);
			TMHealth tmHealth = new TMHealth(); 
			while (rs.next()) {
				tmHealth.setHeartbeatTime(rs.getTimestamp("HEARTBEAT_TIME"));
				tmHealth.setCurrentScn(rs.getLong("CURRENT_SCN"));
				tmHealth.setLogminerScn(rs.getLong("LOGMINER_SCN"));
				tmHealth.setLogminerReceiveed(rs.getTimestamp("LOGMINER_RECEIVED"));
				tmHealth.setConsumerReceived(rs.getTimestamp("CONSUMER_RECEIVED"));
				tmHealth.setConsumerClient(rs.getString("CONSUMER_CLIENT"));
				tmHealth.setCpuUtilValue(rs.getBigDecimal("CPU_UTIL_VALUE"));
			}
			return tmHealth;
		} catch(Exception e) {
			LOG.error(">>> Response code={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} finally {
			if (cstmt != null ) cstmt.close();
			if (conn != null ) conn.close();
		}
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

						//						int notReceivedCnt = getPartyContactNotReceived();
						TMHealth tmHealth = getConsumerLastReceived();

						LOG.info(">>>>>>>>>>>> healthStatus notReceivedCnt={}", ToStringBuilder.reflectionToString(tmHealth));
						triggerHealthAlert(tmHealth);

						Thread.sleep(30000);
					}
				} catch (Exception e) {
					LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}
			}
			private TMHealth getConsumerLastReceived() throws Exception{
				CallableStatement cstmt = null;
				Connection conn = null;
				try {
					conn = tglminerConnPool.getConnection();
					cstmt = conn.prepareCall("{call SP_GET_CONSUMER_LAST_RECEIVED(?,?)}");
					cstmt.setString(1, PARTY_CONTACT_CLIENT_ID);
					cstmt.registerOutParameter(2, oracle.jdbc.OracleTypes.CURSOR);
					cstmt.execute();
					ResultSet rs = (ResultSet)cstmt.getObject(2);
					TMHealth tmHealth = new TMHealth(); 
					while (rs.next()) {
						tmHealth.setHeartbeatTime(rs.getTimestamp("HEARTBEAT_TIME"));
						tmHealth.setCurrentScn(rs.getLong("CURRENT_SCN"));
						tmHealth.setLogminerScn(rs.getLong("LOGMINER_SCN"));
						tmHealth.setLogminerReceiveed(rs.getTimestamp("LOGMINER_RECEIVED"));
						tmHealth.setConsumerReceived(rs.getTimestamp("CONSUMER_RECEIVED"));
						tmHealth.setConsumerClient(rs.getString("CONSUMER_CLIENT"));
						tmHealth.setCpuUtilValue(rs.getBigDecimal("CPU_UTIL_VALUE"));
					}
					return tmHealth;
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
