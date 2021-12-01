package com.transglobe.streamingetl.partycontact.rest.service;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.partycontact.rest.bean.PartyContactTableEnum;
import com.transglobe.streamingetl.partycontact.rest.bean.Table;
import com.transglobe.streamingetl.partycontact.rest.service.bean.LoadBean;



@Service
public class PartyContactService {
	static final Logger LOG = LoggerFactory.getLogger(PartyContactService.class);

	private static final int THREADS = 15;

	private static final int BATCH_SIZE = 3000;

	@Value("{streaming.etl.host}")
	private String streamingEtlHost;

	@Value("{streaming.etl.port}")
	private String streamingEtlPort;

	@Value("{streaming.etl.name}")
	private String streamingEtlName;

	//	@Value("${table.name.partycontact}")
	//	private String tableNamePartycontact;

	//	@Value("${table.create.file.party_contact}")
	//	private String tableCreateFilePartycontact;

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
	private String partycontactDbUrl;

	@Value("${sink.db.username}")
	private String sinkDbUsername;

	@Value("${sink.db.password}")
	private String sinkDbPassword;


	public void createTable() throws Exception {
		LOG.info(">>>>>>>>>>>> createTable ");

		for (PartyContactTableEnum e : PartyContactTableEnum.values()) {
			if (tableExists(e.getTableName())) {
				LOG.info(">>>>>>> DROP TABLE {}",e.getTableName());
				executeScript("DROP TABLE " + e.getTableName());
			} 
			executeSqlScriptFromFile(e.getScriptFile());
		}


	}
	public void dropTable() throws Exception {
		LOG.info(">>>>>>>>>>>> drop Table ");

		for (PartyContactTableEnum e : PartyContactTableEnum.values()) {
			if (tableExists(e.getTableName())) {
				LOG.info(">>>>>>> DROP TABLE {}",e.getTableName());
				executeScript("DROP TABLE " + e.getTableName());
			} 
		}

	}

	public void truncateTable(String table) throws Exception {
		LOG.info(">>>>>>>>>>>> truncateTable ");

		executeScript("TRUNCATE TABLE " + table);

		LOG.info(">>>>>>>>>>>> truncatePartyContactTable Done!!!");
	}
	public long loadAllData() throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(6);

		List<String> tableList = new ArrayList<>();
		tableList.add(Table.T_POLICY_HOLDER);
		tableList.add(Table.T_POLICY_HOLDER_LOG);
		tableList.add(Table.T_INSURED_LIST);
		tableList.add(Table.T_INSURED_LIST_LOG);
		tableList.add(Table.T_CONTRACT_BENE);
		tableList.add(Table.T_CONTRACT_BENE_LOG);

		List<CompletableFuture<Long>> futures = 
				tableList.stream().map(t ->CompletableFuture.supplyAsync(
						() -> {			
							Long cnt = 0L;
							try {
								LOG.info(">>> loading Table async {} ... ", t);
								cnt = loadTable(t);
							} catch (Exception e) {
								LOG.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
							}
							return cnt;
						}
						, executor)
						)
				.collect(Collectors.toList());			

		List<Long> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

		LOG.info(">>> add primary key");
		addPrimaryKey();

		LOG.info(">>> create index");
		createIndexes();


		long count = 0;
		for (Long c : result) {
			count = count  +c;
		}
		return count;
	}
	public long loadTable(String table) throws Exception {
		BasicDataSource sourceConnectionPool = null;
		BasicDataSource sinkConnectionPool = null;

		long count = 0;
		try {
			sourceConnectionPool = new BasicDataSource();

			sourceConnectionPool.setUrl(sourceDbUrl);
			sourceConnectionPool.setUsername(sourceDbUsername);
			sourceConnectionPool.setPassword(sourceDbPassword);
			sourceConnectionPool.setDriverClassName(sourceDbDriver);
			sourceConnectionPool.setMaxTotal(THREADS);

			sinkConnectionPool = new BasicDataSource();
			sinkConnectionPool.setUrl(partycontactDbUrl);
			sinkConnectionPool.setDriverClassName(sinkDbDriver);
			sinkConnectionPool.setUsername(sinkDbUsername);
			sinkConnectionPool.setPassword(sinkDbPassword);
			sinkConnectionPool.setMaxTotal(THREADS);

			if (Table.T_POLICY_HOLDER.equalsIgnoreCase(table)) {
				count = loadTable(table, 1, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_INSURED_LIST.equalsIgnoreCase(table)) {
				count = loadTable(table, 2, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_CONTRACT_BENE.equalsIgnoreCase(table)) {
				count = loadTable(table, 3, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_POLICY_HOLDER_LOG.equalsIgnoreCase(table)) {
				count = loadTableTLog(table, 1, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_POLICY_HOLDER_LOG.equalsIgnoreCase(table)) {
				count = loadTableTLog(table, 2, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_CONTRACT_BENE_LOG.equalsIgnoreCase(table)) {
				count = loadTableTLog(table, 3, sourceConnectionPool, sinkConnectionPool);
			} else {	
				throw new Exception("Incorrect table name '" + table + "'");
			}
		} catch (Exception ex) {
			LOG.error("message={}, stack trace={}", ex.getMessage(), ExceptionUtils.getStackTrace(ex));

		} finally {
			try {
				if (sourceConnectionPool != null) sourceConnectionPool.close();
			} catch (Exception e) {
				LOG.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
			try {
				if (sinkConnectionPool != null) sinkConnectionPool.close();
			} catch (Exception e) {
				LOG.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
		return count;
	}

	public void addPrimaryKey() throws Exception {
		LOG.info(">>>>>>>>>>>> addPrimaryKey ");

		executeScript("ALTER TABLE  " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " ADD CONSTRAINT PK_T_PARTY_CONTACT PRIMARY KEY (ROLE_TYPE,LIST_ID)");

		LOG.info(">>>>>>>>>>>> addPrimaryKey done!!! ");
	}
	public void createIndexes() throws Exception {
	
		ExecutorService executor = Executors.newFixedThreadPool(8);

		List<String> indexList = new ArrayList<>();
		indexList.add("ADDRESS_1");
		indexList.add("EMAIL");
		indexList.add("MOBILE_TEL");
		indexList.add("CERTI_CODE");
		indexList.add("POLICY_ID");
		indexList.add("UPDATE_TIMESTAMP");
		indexList.add("ROLE_SCN");
		indexList.add("ADDRE_SCN");

		List<CompletableFuture<String>> futures = 
				indexList.stream().map(t ->CompletableFuture.supplyAsync(
						() -> {		
							String ret = "";
							try {
								ret = createIndex(t);
							} catch (Exception e) {
								LOG.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
							}
							return ret;
						}
						, executor)
						)
				.collect(Collectors.toList());			

		List<String> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

	}
	public String createIndex(String columnName) throws Exception {
		LOG.info(">>>>>>>>>>>> createIndexes ");

		if ("ADDRESS_1".equalsIgnoreCase(columnName)) {
			executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_ADDR1 ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (ADDRESS_1)");
			LOG.info(">>>>>>>>>>>> 1/7 createIndex for addr1 done!!! ");
		} else if ("EMAIL".equalsIgnoreCase(columnName)) {
			executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_EMAIL ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (EMAIL)");
			LOG.info(">>>>>>>>>>>> 2/7 createIndex for email done!!! ");
		}  else if ("MOBILE_TEL".equalsIgnoreCase(columnName)) {
			executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_MOBILE_TEL ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (MOBILE_TEL)");
			LOG.info(">>>>>>>>>>>> 3/7 createIndex for mobile_tel done!!! ");
		} else if ("CERTI_CODE".equalsIgnoreCase(columnName)) {
			executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_CERTI_CODE ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (CERTI_CODE)");
			LOG.info(">>>>>>>>>>>> 4/7 createIndex for certi_code done!!! ");
		} else if ("POLICY_ID".equalsIgnoreCase(columnName)) {
			executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_POLICY_ID ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (POLICY_ID)");
			LOG.info(">>>>>>>>>>>> 5/7 createIndex for policy_id done!!! ");
		} else if ("UPDATE_TIMESTAMP".equalsIgnoreCase(columnName)) {
			executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_UPD_TS ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (UPDATE_TIMESTAMP)");
			LOG.info(">>>>>>>>>>>> 7/7 createIndex for update_timestamp done!!! ");
		} else if ("UPDATE_TIMESTAMP".equalsIgnoreCase(columnName)) {
			executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_ROLE_SCN ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (ROLE_SCN)");
		} else if ("UPDATE_TIMESTAMP".equalsIgnoreCase(columnName)) {
			executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_ADDR_SCN ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (ADDR_SCN)");
		} else {
			throw new Exception("Invalid Column Name:" + columnName);
		}
		return columnName;
	}
	private long loadTable(String table, Integer roleType, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool) throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(THREADS);

		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		long count = 0L;
		try {

			Connection sourceConn = sourceConnectionPool.getConnection();

			sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " + table;

			//				sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " 
			//				+ table + " where list_id >= 31000000";
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			long maxListId = 0;
			long minListId = 0;
			while (rs.next()) {
				minListId = rs.getLong("MIN_LIST_ID");
				maxListId = rs.getLong("MAX_LIST_ID");
			}
			rs.close();
			pstmt.close();

			long stepSize = 100000;
			long startIndex = minListId;

			List<LoadBean> loadBeanList = new ArrayList<>();
			while (startIndex <= maxListId) {
				long endIndex = startIndex + stepSize;


				int j = 0;
				long  subStepSize = stepSize;
				LoadBean loadBean = new LoadBean();
				loadBean.tableName = table;
				loadBean.roleType = roleType;
				loadBean.startSeq = startIndex + j * subStepSize;
				loadBean.endSeq = startIndex + (j + 1) * subStepSize;
				loadBeanList.add(loadBean);

				startIndex = endIndex;
			}

			LOG.info("table={}, maxlistid={}, minListId={}, loadBeanListsize={}", table, maxListId, minListId, loadBeanList.size());

			List<CompletableFuture<Map<String, String>>> futures = 
					loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
							() -> {
								String sqlStr = "select a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,c.ADDRESS_1,a.ORA_ROWSCN ROLE_SCN,a.ROWID ROLE_ROW_ID,c.ORA_ROWSCN ADDR_SCN,c.ROWID ADDR_ROW_ID from " 
										+ t.tableName 
										+ " a inner join T_CONTRACT_MASTER b ON a.POLICY_ID=b.POLICY_ID "
										+ " left join T_ADDRESS c on a.address_id = c.address_id "
										+ " where b.LIABILITY_STATE = 0 and " + t.startSeq + " <= a.list_id and a.list_id < " + t.endSeq;
								return loadPartyContact(sqlStr, t, sourceConnectionPool, sinkConnectionPool);
							}
							, executor)
							)
					.collect(Collectors.toList());			

			List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			for (Map<String, String> map : result) {
				if (StringUtils.equals("-999", map.get("RETURN_CODE"))) {
					LOG.error(">>>  error message={}, STACK_TRACE={}", map.get("ERROR_MSG"), map.get("STACK_TRACE"));
				} else {
					count = count + Long.valueOf(map.get("COUNT"));
				}
			}

			return count;
		} finally {
			if (executor != null) executor.shutdown();

		}
	}
	private long loadTableTLog(String table, Integer roleType, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool) throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(THREADS);

		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		long count = 0L;
		try {

			Connection sourceConn = sourceConnectionPool.getConnection();

			sql = "select min(log_id) as MIN_LOG_ID, max(log_id) as MAX_LOG_ID from " + table;

			//				sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " 
			//				+ table + " where list_id >= 31000000";
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			long maxLogId = 0;
			long minLogId = 0;
			while (rs.next()) {
				minLogId = rs.getLong("MIN_LOG_ID");
				maxLogId = rs.getLong("MAX_LOG_ID");
			}
			rs.close();
			pstmt.close();

			long stepSize = 100000;
			long startIndex = minLogId;

			List<LoadBean> loadBeanList = new ArrayList<>();
			while (startIndex <= maxLogId) {
				long endIndex = startIndex + stepSize;


				int j = 0;
				long  subStepSize = stepSize;
				LoadBean loadBean = new LoadBean();
				loadBean.tableName = table;
				loadBean.roleType = roleType;
				loadBean.startSeq = startIndex + j * subStepSize;
				loadBean.endSeq = startIndex + (j + 1) * subStepSize;
				loadBeanList.add(loadBean);

				startIndex = endIndex;
			}

			LOG.info("table={}, maxlistid={}, minListId={}, size={}", table, maxLogId, minLogId, loadBeanList.size());

			List<CompletableFuture<Map<String, String>>> futures = 
					loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
							() -> {
								String sqlStr = "select a.LIST_ID,a.POLICY_ID,a.NAME,a.CERTI_CODE,a.MOBILE_TEL,a.EMAIL,a.ADDRESS_ID,c.ADDRESS_1,a.ORA_ROWSCN ROLE_SCN,a.ROWID ROLE_ROW_ID,c.ORA_ROWSCN ADDR_SCN,c.ROWID ADDR_ROW_ID from " 
										+ t.tableName 
										+ " a inner join T_POLICY_CHANGE b ON a.POLICY_CHG_ID=b.POLICY_CHG_ID "
										+ " left join T_ADDRESS c on a.address_id = c.address_id "
										+ " where a.LAST_CMT_FLG = 'Y' and b.POLICY_CHG_STATUS = 2 and " + t.startSeq + " <= a.log_id and a.log_id < " + t.endSeq;
								return loadPartyContact(sqlStr, t, sourceConnectionPool, sinkConnectionPool);
							}
							, executor)
							)
					.collect(Collectors.toList());			

			List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			for (Map<String, String> map : result) {
				if (StringUtils.equals("-999", map.get("RETURN_CODE"))) {
					LOG.error(">>>  error message={}, STACK_TRACE={}", map.get("ERROR_MSG"), map.get("STACK_TRACE"));
				} else {
					count = count + Long.valueOf(map.get("COUNT"));
				}
			}

			return count;
		} finally {
			if (executor != null) executor.shutdown();

		}
	}
	private Map<String, String> loadPartyContact(String sql, LoadBean loadBean, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool){
		//		logger.info(">>> run loadInterestedPartyContact");

		Console cnsl = null;
		Map<String, String> map = new HashMap<>();
		Connection sourceConn = null;
		Connection sinkConn = null;
		//		Connection minerConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Long count = 0L;
		try {

			sourceConn = sourceConnectionPool.getConnection();

			sinkConn = sinkConnectionPool.getConnection();

			Statement stmt = sourceConn.createStatement();
			rs = stmt.executeQuery(sql);

			sinkConn.setAutoCommit(false); 
			pstmt = sinkConn.prepareStatement(
					"insert into " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1,INSERT_TIMESTAMP,UPDATE_TIMESTAMP,ROLE_TABLE,ROLE_SCN,ROLE_COMMIT_SCN,ROLE_ROW_ID, ADDR_SCN,ADDR_COMMIT_SCN,ADDR_ROW_ID) " 
							+ " values (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?,?,NULL,?,?,NULL,?)");

			while (rs.next()) {
				count++;

				pstmt.setInt(1, loadBean.roleType);

				pstmt.setLong(2, rs.getLong("LIST_ID"));

				Long policyId = rs.getLong("POLICY_ID");
				if (rs.wasNull()) {
					pstmt.setNull(3, Types.BIGINT);
				} else {
					pstmt.setLong(3, policyId);
				}
				pstmt.setString(4, rs.getString("NAME"));
				pstmt.setString(5, rs.getString("CERTI_CODE"));
				pstmt.setString(6, rs.getString("MOBILE_TEL"));

				//因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
				String email = (loadBean.roleType == 3)? null : rs.getString("EMAIL");
				if (email == null) {
					pstmt.setNull(7, Types.VARCHAR);
				} else {
					pstmt.setString(7, StringUtils.trim(email.toLowerCase()));
				}
				Long addressId = rs.getLong("ADDRESS_ID");
				if (rs.wasNull()) {
					pstmt.setNull(8, Types.BIGINT);
				} else {
					pstmt.setLong(8, addressId);
				}

				pstmt.setString(9, StringUtils.trim(rs.getString("ADDRESS_1")));

				pstmt.setString(10, loadBean.tableName);
				pstmt.setLong(11, rs.getLong("ROLE_SCN"));
				pstmt.setString(12, rs.getString("ROLE_ROW_ID"));
				pstmt.setLong(13, rs.getLong("ADDR_SCN"));
				pstmt.setString(14, rs.getString("ADDR_ROW_ID"));

				pstmt.addBatch();

				if (count % BATCH_SIZE == 0) {
					pstmt.executeBatch();//executing the batch  
					sinkConn.commit(); 
					pstmt.clearBatch();
				}
			}
			//	if (startSeq % 50000000 == 0) {
			//				
			//			cnsl = System.console();
			LOG.info("   >>>roletype={}, startSeq={}, endSeq={}, count={} ", loadBean.roleType, loadBean.startSeq, loadBean.endSeq, count);
			//			cnsl.printf("   >>>roletype=%d, startSeq=%d, endSeq=%d, count=%d \n", loadBean.roleType, loadBean.startSeq, loadBean.endSeq, count);

			//				cnsl.printf("   >>>roletype=" + roleType + ", startSeq=" + startSeq + ", count=" + count +", span=" + ",span=" + (System.currentTimeMillis() - t0));
			//			cnsl.flush();
			//		}

			pstmt.executeBatch();
			if (pstmt != null) pstmt.close();
			if (count > 0) sinkConn.commit(); 

			rs.close();
			stmt.close();

			sourceConn.close();
			sinkConn.close();

			map.put("COUNT", String.valueOf(count));
		}  catch (Exception e) {
			LOG.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			map.put("RETURN_CODE", "-999");
			map.put("SQL", sql);
			map.put("SINK_TABLE", PartyContactTableEnum.T_PARTY_CONTACT.getTableName());
			map.put("ERROR_MSG", e.getMessage());
			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sinkConn != null) {
				try {
					sinkConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}


		}
		return map;
	}

	private void executeScript(String script) throws Exception {

		Connection conn = null;
		Statement stmt = null;
		try {

			Class.forName(sinkDbDriver);
			conn = DriverManager.getConnection(partycontactDbUrl, sinkDbUsername, sinkDbPassword);

			stmt = conn.createStatement();
			stmt.executeUpdate(script);
			stmt.close();

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
			if (conn != null) conn.close();
		}

	}
	public void executeSqlScriptFromFile(String file) throws Exception {
		LOG.info(">>>>>>>>>>>> executeSqlScriptFromFile file={}", file);

		Connection conn = null;
		PreparedStatement pstmt = null;
		try {

			Class.forName(sinkDbDriver);
			conn = DriverManager.getConnection(partycontactDbUrl, sinkDbUsername, sinkDbPassword);

			Statement stmt = null;
			ClassLoader loader = Thread.currentThread().getContextClassLoader();	
			try (InputStream inputStream = loader.getResourceAsStream(file)) {
				String createScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
				stmt = conn.createStatement();
				stmt.executeUpdate(createScript);
			} catch (SQLException | IOException e) {
				if (stmt != null) stmt.close();
				throw e;
			}

			LOG.info(">>>>>>>>>>>> executeSqlScriptFromFile Done!!!");


		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}
	}
	public boolean tableExists(String table) throws Exception {

		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		boolean exists = false;
		try {

			Class.forName(sinkDbDriver);
			conn = DriverManager.getConnection(partycontactDbUrl, sinkDbUsername, sinkDbPassword);

			sql = "select count(*) CNT from USER_TABLES where TABLE_NAME = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, table);
			rs = pstmt.executeQuery();
			int cnt = 0;
			while (rs.next()) {
				cnt = rs.getInt("CNT");
			}
			rs.close();
			pstmt.close();

			if (cnt > 0) {
				exists = true;
			}

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}
		return exists;
	}
}
