package com.transglobe.streamingetl.partycontact.rest.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class PartyContactService {
	static final Logger LOG = LoggerFactory.getLogger(PartyContactService.class);
			
	@Value("${table.name.partycontact}")
	private String tableNamePartycontact;

	@Value("$table.create.file.party_contact}")
	private String tableCreateFilePartycontact;
	
	@Value("${source.db.driver}")
	private String sourceDbDriver;

	@Value("${source.db.url}")
	private String sourceDbUrl;

	@Value("${source.db.username}")
	private String sourceDbUsername;

	@Value("${source.db.password}")
	private String sourceDbPassword;
	
	@Value("${partycontact.db.driver}")
	private String partycontactDbDriver;

	@Value("${partycontact.db.url}")
	private String partycontactDbUrl;

	@Value("${partycontact.db.username}")
	private String partycontactDbUsername;

	@Value("${partycontact.db.password}")
	private String partycontactDbPassword;
	
//	protected BasicDataSource sourceConnectionPool;
	
	public void createPartyContactTable() throws Exception {
		LOG.info(">>>>>>>>>>>> createPartyContactTable ");

		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			String file = null;
			
			LOG.info(">>>>>> create table File={} ",tableCreateFilePartycontact);

			Class.forName(partycontactDbDriver);
			conn = DriverManager.getConnection(partycontactDbUrl, partycontactDbUsername, partycontactDbPassword);

			executeSqlScriptFile(conn, file);

			LOG.info(">>>>>>>>>>>> createPartyContactTable Done!!!");

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();
		}
	}
	public void dropPartyContactTable() throws Exception {
		LOG.info(">>>>>>>>>>>> dropPartyContactTable ");

		Connection conn = null;
		Statement stmt = null;
		try {
			String file = null;
			
			Class.forName(partycontactDbDriver);
			conn = DriverManager.getConnection(partycontactDbUrl, partycontactDbUsername, partycontactDbPassword);

			stmt = conn.createStatement();
			stmt.executeUpdate("DROP TABLE " + tableNamePartycontact);
			stmt.close();

			LOG.info(">>>>>>>>>>>> dropPartyContactTable Done!!!");

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
			if (conn != null) conn.close();
		}
		
	}

	public void truncatePartyContactTable() throws Exception {
		LOG.info(">>>>>>>>>>>> truncatePartyContactTable ");

		executeScript("TRUNCATE TABLE " + tableNamePartycontact);
			
	}
	public void createIndexes() throws Exception {
		LOG.info(">>>>>>>>>>>> createIndexes ");

		executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_ADDR1 ON " + tableNamePartycontact + " (ADDRESS_1)");
		executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_EMAIL ON " + tableNamePartycontact + " (EMAIL)");
		executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_MOBILE_TEL ON " + tableNamePartycontact + " (MOBILE_TEL)");
		executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_CERTI_CODE ON " + tableNamePartycontact + " (CERTI_CODE)");
		executeScript("CREATE INDEX IDX_T_PARTY_CONTACT_UPD_TS ON " + tableNamePartycontact + " (UPDATE_TIMESTAMP)");
		
	}
	public void executeScript(String script) throws Exception {
		LOG.info(">>>>>>>>>>>> truncatePartyContactTable ");

		Connection conn = null;
		Statement stmt = null;
		try {
			
			Class.forName(partycontactDbDriver);
			conn = DriverManager.getConnection(partycontactDbUrl, partycontactDbUsername, partycontactDbPassword);

			stmt = conn.createStatement();
			stmt.executeUpdate(script);
			stmt.close();

			LOG.info(">>>>>>>>>>>> truncatePartyContactTable Done!!!");

		} catch (Exception e1) {

			LOG.error(">>>>> Error!!!, error msg={}, stacetrace={}", ExceptionUtils.getMessage(e1), ExceptionUtils.getStackTrace(e1));

			throw e1;
		} finally {
			if (stmt != null) stmt.close();
			if (conn != null) conn.close();
		}
		
	}
	private void executeSqlScriptFile(Connection conn, String sqlScriptFile) throws Exception {

		Statement stmt = null;
		ClassLoader loader = Thread.currentThread().getContextClassLoader();	
		try (InputStream inputStream = loader.getResourceAsStream(sqlScriptFile)) {
			String createTableScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			stmt = conn.createStatement();
			stmt.executeUpdate(createTableScript);
		} catch (SQLException | IOException e) {
			if (stmt != null) stmt.close();
			throw e;
		}

	}

}
