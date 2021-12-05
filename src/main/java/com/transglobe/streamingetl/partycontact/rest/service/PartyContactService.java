package com.transglobe.streamingetl.partycontact.rest.service;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.partycontact.rest.bean.PartyContactETL;
import com.transglobe.streamingetl.partycontact.rest.bean.PartyContactSyncTableEnum;
import com.transglobe.streamingetl.partycontact.rest.bean.PartyContactTableEnum;
import com.transglobe.streamingetl.partycontact.rest.bean.PartyContactTopicEnum;
import com.transglobe.streamingetl.partycontact.rest.bean.Table;
import com.transglobe.streamingetl.partycontact.rest.service.bean.LoadBean;
import com.transglobe.streamingetl.partycontact.rest.util.DbUtils;
import com.transglobe.streamingetl.partycontact.rest.util.HttpUtils;


@Service
public class PartyContactService {
	static final Logger LOG = LoggerFactory.getLogger(PartyContactService.class);

	private static final int THREADS = 15;

	private static final int BATCH_SIZE = 3000;

	@Value("${tglminer.rest.url}")
	private String tglminerRestUrl;

	//	@Value("${kafka.rest.url}")
	//	private String kafkaRestUrl;
	//
	//	@Value("{streaming.etl.host}")
	//	private String streamingEtlHost;
	//
	//	@Value("{streaming.etl.port}")
	//	private String streamingEtlPort;
	//
	//	@Value("{streaming.etl.name}")
	//	private String streamingEtlName;

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

	@Autowired
	private ConsumerService consumerService;
	
	public void cleanup() throws Exception {
		LOG.info(">>>>>>>>>>>> cleanup ");
		Connection conn = null;
		try {
			Class.forName(sinkDbDriver);
			conn = DriverManager.getConnection(sinkDbUrl, sinkDbUsername, sinkDbPassword);

			// connect to tglminer rest to remove etl
			String removeEtlUrl = tglminerRestUrl + "/init/removeEtl/" + PartyContactETL.NAME;
			LOG.info(">>>>>>> removeEtlUrl={}", removeEtlUrl);
//			
			String response = HttpUtils.restService(removeEtlUrl, "POST");
			LOG.info(">>>>>>> remove ETL response={}", response);

			for (PartyContactTableEnum e : PartyContactTableEnum.values()) {
				if (tableExists(e.getTableName())) {
					LOG.info(">>>>>>> DROP TABLE {}",e.getTableName());
					executeScript(conn, "DROP TABLE " + e.getTableName());
				} 
			}
		} finally {
			if (conn != null) conn.close();
		}
	}
	public void runPartyContact() throws Exception{
		
		LOG.info(">>>>>>> start partycontact consumer ...");
		consumerService.start();
		
		LOG.info(">>>>>>> call applyLogminerSync ...");
		String applyLogminerSyncUrl = tglminerRestUrl + "/tglminer/applyLogminerSync/" + PartyContactETL.NAME;
		LOG.info(">>>>>>> applyLogminerSyncUrl={}", applyLogminerSyncUrl); 
		String response = HttpUtils.restService(applyLogminerSyncUrl, "POST");
		
		LOG.info(">>>>>>> applyLogminerSync response={}", response);
	}
	public void dropLogminerSync() throws Exception{
		LOG.info(">>>>>>> call dropLogminerSync ...");
		String dropLogminerSyncUrl = tglminerRestUrl + "/tglminer/dropLogminerSync/" + PartyContactETL.NAME;
		LOG.info(">>>>>>> dropLogminerSyncUrl={}", dropLogminerSyncUrl); 
		String response = HttpUtils.restService(dropLogminerSyncUrl, "POST");
	}

	public void stopPartyContact() throws Exception {
		LOG.info(">>>>>>> stopPartyContact ...");
		
		LOG.info(">>>>>>> dropLogminerSync");
		dropLogminerSync();
		
		LOG.info(">>>>>>> consumerService.shutdown");
		consumerService.shutdown();
		
		if (!consumerService.isConsumerClosed()) {
			throw new Exception("consumerService consumer IS NOT Closed.");
		}
	}
	public void initialize() throws Exception{
		Connection conn = null;

		CallableStatement cstmt = null;
		try {

			Class.forName(tglminerDbDriver);
			conn = DriverManager.getConnection(tglminerDbUrl, tglminerDbUsername, tglminerDbPassword);

			setupDbObjects(conn);

			// insert etl
			LOG.info(">>> insert etl name ");
			cstmt = conn.prepareCall("{call SP_INS_ETL_NAME(?,?,?)}");

			cstmt.setString(1,  PartyContactETL.CAT);
			cstmt.setString(2,  PartyContactETL.NAME);
			cstmt.setString(3,  PartyContactETL.NOTE);
			cstmt.execute();
			cstmt.close();

			LOG.info(">>> insert kafka topic");
			for (PartyContactTopicEnum e : PartyContactTopicEnum.values()) {
				String topic = e.getTopic();
				String insertTopicUrl = tglminerRestUrl + "/init/createTopic/" + PartyContactETL.NAME + "/" + topic;
				String response = HttpUtils.restService(insertTopicUrl, "POST");
				
				LOG.info(">>>>>>> kafka topic {} inserted, response={}", topic,response);
			}

			LOG.info(">>> insert logminer table");
			for (PartyContactSyncTableEnum e : PartyContactSyncTableEnum.values()) {
				String syncTableName = e.getSyncTableName();
				String insertLogminerUrl = tglminerRestUrl + "/init/insertLogminerSyncTable/" + PartyContactETL.NAME + "/" + syncTableName;
				String response = HttpUtils.restService(insertLogminerUrl, "POST");
				LOG.info(">>>>>>> logminer syncTableName {} inserted, response={}", syncTableName,response);
			}
			

		} finally {
			if (cstmt != null) cstmt.close();
			if (conn != null) conn.close();
		}

	}
	private void setupDbObjects(Connection conn) throws Exception {

		for (PartyContactTableEnum e : PartyContactTableEnum.values()) {
			LOG.info(">>>>>>> create TABLE file {}",e.getScriptFile());
			DbUtils.executeSqlScriptFromFile(conn, e.getScriptFile());
		}

	}
	
	public void createTable() throws Exception {
		LOG.info(">>>>>>>>>>>> createTable ");
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(sinkDbDriver);
			conn = DriverManager.getConnection(sinkDbUrl, sinkDbUsername, sinkDbPassword);


			for (PartyContactTableEnum e : PartyContactTableEnum.values()) {
				if (tableExists(e.getTableName())) {
					LOG.info(">>>>>>> DROP TABLE {}",e.getTableName());
					executeScript(conn, "DROP TABLE " + e.getTableName());
				} 
				executeSqlScriptFromFile(conn, e.getScriptFile());
			}

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
			if (conn != null) conn.close();
		}
	}
	public void dropTable() throws Exception {
		LOG.info(">>>>>>>>>>>> drop Table ");
		Connection conn = null;
		try {
			for (PartyContactTableEnum e : PartyContactTableEnum.values()) {
				if (tableExists(e.getTableName())) {
					LOG.info(">>>>>>> DROP TABLE {}",e.getTableName());
					executeScript(conn, "DROP TABLE " + e.getTableName());
				} 
			}
		} finally {
			if (conn != null) conn.close();
		}
	}

	public void truncateTable(String table) throws Exception {
		LOG.info(">>>>>>>>>>>> truncateTable ");
		Connection conn = null;
		try {
			executeScript(conn, "TRUNCATE TABLE " + table);

			LOG.info(">>>>>>>>>>>> truncatePartyContactTable Done!!!");
		} finally {
			if (conn != null) conn.close();
		}
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
			sinkConnectionPool.setUrl(sinkDbUrl);
			sinkConnectionPool.setDriverClassName(sinkDbDriver);
			sinkConnectionPool.setUsername(sinkDbUsername);
			sinkConnectionPool.setPassword(sinkDbPassword);
			sinkConnectionPool.setMaxTotal(THREADS);

			if (Table.T_POLICY_HOLDER.equalsIgnoreCase(table)) {
				count = doLoadTable(table, 1, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_INSURED_LIST.equalsIgnoreCase(table)) {
				count = doLoadTable(table, 2, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_CONTRACT_BENE.equalsIgnoreCase(table)) {
				count = doLoadTable(table, 3, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_POLICY_HOLDER_LOG.equalsIgnoreCase(table)) {
				count = doLoadTableTLog(table, 1, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_INSURED_LIST_LOG.equalsIgnoreCase(table)) {
				count = doLoadTableTLog(table, 2, sourceConnectionPool, sinkConnectionPool);
			} else if (Table.T_CONTRACT_BENE_LOG.equalsIgnoreCase(table)) {
				count = doLoadTableTLog(table, 3, sourceConnectionPool, sinkConnectionPool);
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
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(sinkDbDriver);
			conn = DriverManager.getConnection(sinkDbUrl, sinkDbUsername, sinkDbPassword);

			executeScript(conn, "ALTER TABLE  " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " ADD CONSTRAINT PK_T_PARTY_CONTACT PRIMARY KEY (ROLE_TYPE,LIST_ID)");

			LOG.info(">>>>>>>>>>>> addPrimaryKey done!!! ");
		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
			if (conn != null) conn.close();
		}
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
		Connection conn = null;
		try {
			Class.forName(sinkDbDriver);
			conn = DriverManager.getConnection(sinkDbUrl, sinkDbUsername, sinkDbPassword);

			if ("ADDRESS_1".equalsIgnoreCase(columnName)) {
				executeScript(conn, "CREATE INDEX IDX_T_PARTY_CONTACT_ADDR1 ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (ADDRESS_1)");
				LOG.info(">>>>>>>>>>>> createIndex for addr1 done!!! ");
			} else if ("EMAIL".equalsIgnoreCase(columnName)) {
				executeScript(conn,"CREATE INDEX IDX_T_PARTY_CONTACT_EMAIL ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (EMAIL)");
				LOG.info(">>>>>>>>>>>> createIndex for email done!!! ");
			}  else if ("MOBILE_TEL".equalsIgnoreCase(columnName)) {
				executeScript(conn,"CREATE INDEX IDX_T_PARTY_CONTACT_MOBILE_TEL ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (MOBILE_TEL)");
				LOG.info(">>>>>>>>>>>> createIndex for mobile_tel done!!! ");
			} else if ("CERTI_CODE".equalsIgnoreCase(columnName)) {
				executeScript(conn,"CREATE INDEX IDX_T_PARTY_CONTACT_CERTI_CODE ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (CERTI_CODE)");
				LOG.info(">>>>>>>>>>>> createIndex for certi_code done!!! ");
			} else if ("POLICY_ID".equalsIgnoreCase(columnName)) {
				executeScript(conn,"CREATE INDEX IDX_T_PARTY_CONTACT_POLICY_ID ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (POLICY_ID)");
				LOG.info(">>>>>>>>>>>> createIndex for policy_id done!!! ");
			} else if ("UPDATE_TIMESTAMP".equalsIgnoreCase(columnName)) {
				executeScript(conn,"CREATE INDEX IDX_T_PARTY_CONTACT_UPD_TS ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (UPDATE_TIMESTAMP)");
				LOG.info(">>>>>>>>>>>> createIndex for update_timestamp done!!! ");
			} else if ("ROLE_SCN".equalsIgnoreCase(columnName)) {
				executeScript(conn,"CREATE INDEX IDX_T_PARTY_CONTACT_ROLE_SCN ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (ROLE_SCN)");
			} else if ("ADDR_SCN".equalsIgnoreCase(columnName)) {
				executeScript(conn,"CREATE INDEX IDX_T_PARTY_CONTACT_ADDR_SCN ON " + PartyContactTableEnum.T_PARTY_CONTACT.getTableName() + " (ADDR_SCN)");
			} else {
				throw new Exception("Invalid Column Name:" + columnName);
			}
		} finally {
			if (conn != null) conn.close();
		}
		return columnName;
	}
	private long doLoadTable(String table, Integer roleType, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool) throws Exception {

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
								return insertPartyContact(sqlStr, t, sourceConnectionPool, sinkConnectionPool);
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
	private long doLoadTableTLog(String table, Integer roleType, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool) throws Exception {

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
								return insertPartyContact(sqlStr, t, sourceConnectionPool, sinkConnectionPool);
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
	private Map<String, String> insertPartyContact(String sql, LoadBean loadBean, BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool){
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

	private void executeScript(Connection conn, String script) throws Exception {

		Statement stmt = null;
		try {

			Class.forName(sinkDbDriver);
			conn = DriverManager.getConnection(sinkDbUrl, sinkDbUsername, sinkDbPassword);

			stmt = conn.createStatement();
			stmt.executeUpdate(script);
			stmt.close();

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
		}

	}
	public void executeSqlScriptFromFile(Connection conn, String file) throws Exception {
		LOG.info(">>>>>>>>>>>> executeSqlScriptFromFile file={}", file);

		Statement stmt = null;
		try {
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
			if (stmt != null) stmt.close();
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
			conn = DriverManager.getConnection(sinkDbUrl, sinkDbUsername, sinkDbPassword);

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
	//	private void insertTopic(Connection conn, String etlName, String topic) throws Exception{
	//		LOG.info(">>>>>>>>>> insertTopic,{},{}", etlName, topic);
	//
	//		CallableStatement cstmt = null;
	//		try {	
	//
	//
	//			cstmt = conn.prepareCall("{call SP_INS_KAFKA_TOPIC(?,?)}");
	//			cstmt.setString(1,  etlName);
	//			cstmt.setString(2,  topic);
	//			cstmt.execute();
	//			cstmt.close();
	//
	//
	//			String response = kafkaRestService(kafkaRestUrl+"/listTopics", "GET");
	//			ObjectMapper mapper = new ObjectMapper();
	//			JsonNode jsonNode = mapper.readTree(response);
	//			String returnCode = jsonNode.get("returnCode").asText();
	//			String topicStr = jsonNode.get("topics").asText();
	//
	//			List<String> topicList = mapper.readValue(topicStr, new TypeReference<List<String>>() {});
	//			Set<String> topicSet = new HashSet<>(topicList);
	//
	//			LOG.info(">>>>>>>>>> returnCode={}, topicstr={}", returnCode, topicStr);
	//			if (topicSet.contains(topic)) {
	//				kafkaRestService(kafkaRestUrl+"/deleteTopic/" + topic, "POST");
	//				LOG.info(">>>>>>>>>>>> topic={} deleted ", topic);
	//
	//			}
	//
	//			kafkaRestService(kafkaRestUrl+"/createTopic/"+topic, "POST");
	//			LOG.info(">>>>>>>>>>>> topic={} created ", topic);
	//
	//		} catch (Exception e1) {
	//
	//			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));
	//
	//			throw e1;
	//		} finally {
	//			if (cstmt != null) cstmt.close();
	//		}
	//	}
	//	public void deleteKafkaTopics(List<String> deleteTopicList) throws Exception{
	//		LOG.info(">>>>>>>>>> deleteTopic");
	//
	//		try {	
	//
	//			String response = kafkaRestService(kafkaRestUrl+"/listTopics", "GET");
	//			ObjectMapper mapper = new ObjectMapper();
	//			JsonNode jsonNode = mapper.readTree(response);
	//			String returnCode = jsonNode.get("returnCode").asText();
	//			String topicStr = jsonNode.get("topics").asText();
	//
	//			List<String> topicList = mapper.readValue(topicStr, new TypeReference<List<String>>() {});
	//			Set<String> topicSet = new HashSet<>(topicList);
	//
	//			LOG.info(">>>>>>>>>> returnCode={}, topicstr={}", returnCode, topicStr);
	//
	//			for (String t : deleteTopicList) {
	//				if (topicSet.contains(t)) {
	//					kafkaRestService(kafkaRestUrl+"/deleteTopic/" + t, "POST");
	//					LOG.info(">>>>>>>>>>>> topic={} deleted ", t);
	//
	//				}
	//			}
	//
	//		} catch (Exception e1) {
	//
	//			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));
	//
	//			throw e1;
	//		} finally {
	//
	//		}
	//	}
	private String kafkaRestService(String urlStr, String requestMethod) throws Exception {
		LOG.info(">>>>>>>>>>>> kafka service urlStr={}", urlStr);

		HttpURLConnection httpConn = null;
		URL url = null;
		try {
			url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod(requestMethod);
			int responseCode = httpConn.getResponseCode();
			//			LOG.info(">>>>>  responseCode={}",responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
			StringBuffer response = new StringBuffer();
			String readLine = null;
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			LOG.info(">>>>> kafkaRestService responsecode={}, response={}", responseCode, response.toString());

			return response.toString();
		} finally {
			if (httpConn != null ) httpConn.disconnect();
		}
	}

}
