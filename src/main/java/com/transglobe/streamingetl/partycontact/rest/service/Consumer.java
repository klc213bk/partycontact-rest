package com.transglobe.streamingetl.partycontact.rest.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.partycontact.rest.bean.Address;
import com.transglobe.streamingetl.partycontact.rest.bean.PartyContact;
import com.transglobe.streamingetl.partycontact.rest.bean.Table;

public class Consumer implements Runnable {
	static final Logger logger = LoggerFactory.getLogger(Consumer.class);

	private static final String URL_UPDATE_HEALTH_CPONSUMER_RECEIVED = "http://localhost:9100/streamingetl/updateHealthConsumerReceived";
	private static final String HEARTBEAT_TABLE = "HE_HEARTBEAT";

	private static final Integer POLICY_HOLDER_ROLE_TYPE = 1;
	private static final Integer INSURED_LIST_ROLE_TYPE = 2;
	private static final Integer CONTRACT_BENE_ROLE_TYPE = 3;

	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;

	private BasicDataSource sourceConnPool;
	private BasicDataSource sinkConnPool;
	private BasicDataSource logminerConnPool;

	private String clientId;

	private List<String> topicList;

	public Consumer(int id,
			String groupId,  
			String bootstrapServers,
			List<String> topicList,
			BasicDataSource sourceConnPool,
			BasicDataSource sinkConnPool,
			BasicDataSource logminerConnPool
			) {
		this.sourceConnPool = sourceConnPool;
		this.sinkConnPool = sinkConnPool;
		this.logminerConnPool = logminerConnPool;
		this.clientId = groupId + "-" + id;
		this.topicList = topicList;

		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrapServers);
		props.put("group.id", groupId);
		props.put("client.id", clientId);
		props.put("group.instance.id", groupId + "-mygid" );
		props.put("enable.auto.commit", "false");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		props.put("session.timeout.ms", 60000 ); // 60 seconds
		props.put("max.poll.records", 50 );
		props.put("auto.offset.reset", "earliest" );
		this.consumer = new KafkaConsumer<>(props);

	}
	@Override
	public void run() {
		try {
			consumer.subscribe(topicList);

			logger.info("   >>>>>>>>>>>>>>>>>>>>>>>> run ........closed={}",closed.get());

			List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
			while (!closed.get()) {

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, String> record : records) {
					buffer.add(record);
				}

				if (buffer.size() > 0) {

					//Connection sinkConn = null;
					//Connection sourceConn = null;
					int tries = 0;
					while (sourceConnPool.isClosed()) {
						tries++;
						try {
							sourceConnPool.restart();

							logger.info("   >>> sourceConnPool restart, try {} times", tries);

							Thread.sleep(10000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}
					}
					tries = 0;
					while (sinkConnPool.isClosed()) {
						tries++;
						try {
							sinkConnPool.restart();

							logger.info("   >>> sinkConnPool restart, try {} times", tries);

							Thread.sleep(10000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}

					}
					tries = 0;
					while (logminerConnPool.isClosed()) {
						tries++;
						try {
							logminerConnPool.restart();

							logger.info("   >>> logminerConnPool restart, try {} times", tries);

							Thread.sleep(10000);
						} catch (Exception e) {
							logger.error(">>> message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
						}

					}

					process(buffer);

					consumer.commitSync();

					buffer.clear();
				}
			}
		} catch (WakeupException e) {
			// ignore excepton if closing 
			if (!closed.get()) throw e;

			logger.info(">>>ignore excepton if closing, message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));

		} finally {
			consumer.close();
			if (sourceConnPool != null) {
				try {
					sourceConnPool.close();
				} catch (SQLException e) {
					logger.error(">>>sourceConnPool close error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
			if (sinkConnPool != null) {
				try {
					sinkConnPool.close();
				} catch (SQLException e) {
					logger.error(">>>sinkConnPool error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
			if (logminerConnPool != null) {
				try {
					logminerConnPool.close();
				} catch (SQLException e) {
					logger.error(">>>logminerConnPool error, finally message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
				}
			}
		}
	}
	public void process(List<ConsumerRecord<String, String>> buffer) {

		Connection sourceConn = null;
		Connection sinkConn = null;
		Connection logminerConn = null;
		PreparedStatement sinkPstmt = null;
		ResultSet sinkRs = null;
		ConsumerRecord<String, String> recordEx = null;
		try {
			sourceConn = sourceConnPool.getConnection();
			sinkConn = sinkConnPool.getConnection();
			sourceConn.setAutoCommit(false);
			sinkConn.setAutoCommit(false);

			for (ConsumerRecord<String, String> record : buffer) {
				recordEx = record;

				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

				JsonNode jsonNode = objectMapper.readTree(record.value());
				JsonNode payload = jsonNode.get("payload");
				//	payloadStr = payload.toString();

				String operation = payload.get("OPERATION").asText();
				String tableName = payload.get("TABLE_NAME").asText();
				Long scn = Long.valueOf(payload.get("SCN").asText());
				Long commitScn = Long.valueOf(payload.get("COMMIT_SCN").asText());
				String rowId = payload.get("ROW_ID").asText();
				//String sqlRedo = payload.get("SQL_REDO").toString();
				logger.info("   >>>clientId={},offset={},operation={}, TableName={}, scn={}, commitScn={}, rowId={}", clientId, record.offset(), operation, tableName, scn, commitScn, rowId);

				boolean isTtable = false;
				boolean isTlogtable = false;
				boolean isHeartbeatTable = false;
				if (StringUtils.equals(Table.T_POLICY_HOLDER, tableName)
						|| StringUtils.equals(Table.T_INSURED_LIST, tableName)
						|| StringUtils.equals(Table.T_CONTRACT_BENE, tableName) ) {
					isTtable = true;		
				} else if (StringUtils.equals(Table.T_POLICY_HOLDER_LOG, tableName)
						|| StringUtils.equals(Table.T_INSURED_LIST_LOG, tableName)
						|| StringUtils.equals(Table.T_CONTRACT_BENE_LOG, tableName) ) {
					isTlogtable = true;
				} else if (StringUtils.equals(HEARTBEAT_TABLE, tableName)) {
					isHeartbeatTable = true;
				}

				PartyContact partyContact = null;
				PartyContact beforePartyContact = null;
				Integer roleType = null;
				Long listId = null;
				if (isTtable || isTlogtable) {
					logger.info("   >>>payload={}", payload.toPrettyString());
					String payLoadData = payload.get("data").toString();
					String beforePayLoadData = payload.get("before").toString();
					partyContact = (payLoadData == null)? null : objectMapper.readValue(payLoadData, PartyContact.class);;
					beforePartyContact = (beforePayLoadData == null)? null : objectMapper.readValue(beforePayLoadData, PartyContact.class);;

					if (partyContact != null) { 
						partyContact.setMobileTel(StringUtils.trim(partyContact.getMobileTel()));
						partyContact.setEmail(StringUtils.trim(StringUtils.lowerCase(partyContact.getEmail())));
						if (Table.T_POLICY_HOLDER.equals(tableName)
								|| Table.T_POLICY_HOLDER_LOG.equals(tableName)) {
							partyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
						} else if (Table.T_INSURED_LIST.equals(tableName)
								|| Table.T_INSURED_LIST_LOG.equals(tableName)) {
							partyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
						} else if (Table.T_CONTRACT_BENE.equals(tableName)
								|| Table.T_CONTRACT_BENE_LOG.equals(tableName)) {
							partyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
							partyContact.setEmail(null);// 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
						} 

					}
					if (beforePartyContact != null) { 
						beforePartyContact.setMobileTel(StringUtils.trim(beforePartyContact.getMobileTel()));
						beforePartyContact.setEmail(StringUtils.trim(StringUtils.lowerCase(beforePartyContact.getEmail())));
						if (Table.T_POLICY_HOLDER.equals(tableName)
								|| Table.T_POLICY_HOLDER_LOG.equals(tableName)) {
							beforePartyContact.setRoleType(POLICY_HOLDER_ROLE_TYPE);
						} else if (Table.T_INSURED_LIST.equals(tableName)
								|| Table.T_INSURED_LIST_LOG.equals(tableName)) {
							beforePartyContact.setRoleType(INSURED_LIST_ROLE_TYPE);
						} else if (Table.T_CONTRACT_BENE.equals(tableName)
								|| Table.T_CONTRACT_BENE_LOG.equals(tableName)) {
							beforePartyContact.setRoleType(CONTRACT_BENE_ROLE_TYPE);
							beforePartyContact.setEmail(null);// 因BSD規則調整,受益人的email部份,畫面並沒有輸入t_contract_bene.email雖有值但不做比對
						} 

					}			
					logger.info("   >>>partyContact={}", ((partyContact == null)? null : ToStringBuilder.reflectionToString(partyContact)));
					logger.info("   >>>beforepartyContact={}", ((beforePartyContact == null)? null : ToStringBuilder.reflectionToString(beforePartyContact)));

					roleType = (partyContact != null)? partyContact.getRoleType()
							: beforePartyContact.getRoleType();
					listId = (partyContact != null)? partyContact.getListId()
							: beforePartyContact.getListId();
				} 

				// T 表
				if (isTtable) {
					// check sourceSyncTableContractMaster
					Integer liabilityState = (partyContact != null)? getLiabilityState(sourceConn, partyContact.getPolicyId())
							: getLiabilityState(sourceConn, beforePartyContact.getPolicyId());
					logger.info("   >>>liabilityState={}", liabilityState);

					if (liabilityState != null && liabilityState == 0) {
						// do  同步(Insert/Update/Delete)
						if ("INSERT".equals(operation)) {
							logger.info("   >>>insert ...");
							insertPartyContact(sourceConn, sinkConn, partyContact, tableName, scn, commitScn, rowId);
						} else if ("UPDATE".equals(operation)) {
							if (partyContact.equals(beforePartyContact)) {
								// ignore
								logger.info("   >>>ignore, equal ...");
							} else {
								logger.info("   >>>update ...");
								updatePartyContact(sourceConn, sinkConn, partyContact, tableName, scn, commitScn, rowId);
							}
						} else if ("DELETE".equals(operation)) {
							logger.info("   >>>delete ...");
							deletePartyContact(sinkConn, roleType, listId);
						}

					} else {
						// ignore
					}

				} // Log 表
				else if (isTlogtable) {
					String lastCmtFlg = (partyContact != null)? partyContact.getLastCmtFlg()
							: beforePartyContact.getLastCmtFlg();
					Long policyChgId = (partyContact != null)? partyContact.getPolicyChgId()
							: beforePartyContact.getPolicyChgId();

					if (StringUtils.equals("Y", lastCmtFlg)) {
						// check ignite list_id exists
						boolean exists = checkSinkExists(sinkConn, roleType, listId);
						if (exists) {
							logger.info("   >>>ignite list_id exist, update ...");
							updatePartyContact(sourceConn, sinkConn, partyContact, tableName, scn, commitScn, rowId);
						} else {
							logger.info("   >>> ignite list_id does not exist, insert ...");
							insertPartyContact(sourceConn, sinkConn, partyContact, tableName, scn, commitScn, rowId);
						}
					} else if (StringUtils.equals("N", lastCmtFlg)) {
						int policyChgStatus = getPolicyChangeStatus(sourceConn, policyChgId);

						if (policyChgStatus == 2) {
							logger.info("   >>>policyChgStatus={}", policyChgStatus);	
							// check if T 表 exists
							boolean exists = checkExists(sourceConn, roleType, listId);
							if (exists) {
								logger.info("   >>>T 表 exist, do nothing ...");
							} else {
								logger.info("   >>> T 表 does not exist, delete ...");
								deletePartyContact(sinkConn, roleType, listId);
							}
						} else {
							logger.info("   >>>do nothing with policyChgStatus={}", policyChgStatus);
						}
					} else {
						logger.error("   >>>lastCmtFlg={} is not recognized", lastCmtFlg);
					}

				} 
				// Address 表
				else if (StringUtils.equals(Table.T_ADDRESS, tableName)) {
					String payLoadData = payload.get("data").toString();
					String beforePayLoadData = payload.get("before").toString();
					Address address = (payLoadData == null)? null : objectMapper.readValue(payLoadData, Address.class);
					Address beforeAddress = (beforePayLoadData == null)? null : objectMapper.readValue(beforePayLoadData, Address.class);

					logger.info("   >>>address={}", ((address == null)? null : ToStringBuilder.reflectionToString(address)));
					logger.info("   >>>beforeAddress={}", ((beforeAddress == null)? null : ToStringBuilder.reflectionToString(beforeAddress)));

					if ("INSERT".equals(operation)) {
						logger.info("   >>>insert ...");
						insertAddress(sinkConn, address, scn, commitScn, rowId);
					} else if ("UPDATE".equals(operation)) {

						if (address.equals(beforeAddress)) {
							// ignore
							logger.info("   >>>ignore, equal ...");
						} else {
							logger.info("   >>>update ...");
							updateAddress(sinkConn, beforeAddress, address, scn, commitScn, rowId);
						}	

					} else if ("DELETE".equals(operation)) {
						logger.info("   >>>delete ...");
						deleteAddress(sinkConn, beforeAddress);
					}
				} else if (isHeartbeatTable) {
					logger.info("   >>>payload={}", payload.toPrettyString());
					JsonNode payLoadData = payload.get("data");
					long hartBeatTimeMs = Long.valueOf(payLoadData.get("HEARTBEAT_TIME").asText());
					Timestamp heartbeatTime = new Timestamp(hartBeatTimeMs);

					logminerConn = logminerConnPool.getConnection();
					String sql = null;
					PreparedStatement pstmt = null;

					try {
						
						updateHealthConsumerReceived(clientId, heartbeatTime);
						
					}
					finally {
						if (pstmt != null) pstmt.close();
						if (logminerConn != null) logminerConn.close();
					}
				} else {
					throw new Exception(">>> Error: no such table name:" + tableName);
				}

				//							long t = System.currentTimeMillis();
				//							logger.info("   >>>insertSupplLogSync, rsId={}, ssn={}, scn={}, time={}", rsId, ssn, scn, t);
				//							sqlRedo = StringUtils.substring(sqlRedo, 0, 1000);
				//							insertSupplLogSync(sinkConn, rsId, ssn, scn, t, "");

				sinkConn.commit();
				logger.info("   >>>Done !!!");
			}
		}  catch(Exception e) {
			if (recordEx != null) {
				Map<String, Object> data = new HashMap<>();
				data.put("topic", recordEx.topic());
				data.put("partition", recordEx.partition());
				data.put("offset", recordEx.offset());
				data.put("value", recordEx.value());
				logger.error(">>>record error, message={}, stack trace={}, record str={}", e.getMessage(), ExceptionUtils.getStackTrace(e), data);
			} else {
				logger.error(">>>record error, message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		} finally {
			if (sinkRs != null) {
				try {
					sinkRs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sinkPstmt != null) {
				try {
					sinkPstmt.close();
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
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (logminerConn != null) {
				try {
					logminerConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
	public boolean isConsumerClosed() {
		return closed.get();
	}
	private boolean checkSinkExists(Connection sinkConn, Integer roleType, Long listId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean exists = false;
		try {

			sql = "select * from " + Table.T_PARTY_CONTACT  + " where ROLE_TYPE = ? and LIST_ID = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setInt(1, roleType);
			pstmt.setLong(2, listId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				exists = true;
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return exists;
	}

	private boolean checkExists(Connection sourceConn, Integer roleType, Long listId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean exists = false;
		try {
			String table = "";
			if (POLICY_HOLDER_ROLE_TYPE.equals(roleType) ) {
				table = Table.T_POLICY_HOLDER;
			} else if (INSURED_LIST_ROLE_TYPE.equals(roleType) ) {
				table = Table.T_INSURED_LIST;
			} if (CONTRACT_BENE_ROLE_TYPE.equals(roleType) ) {
				table = Table.T_CONTRACT_BENE;
			}
			sql = "select * from " + table + " where LIST_ID = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, listId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				exists = true;
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return exists;
	}
	private Integer getPolicyChangeStatus(Connection sourceConn, Long policyChgId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Integer policyChgStatus = null;
		try {
			sql = "select POLICY_CHG_STATUS from " + Table.T_POLICY_CHANGE + " where POLICY_CHG_ID = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, policyChgId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				policyChgStatus = rs.getInt("POLICY_CHG_STATUS");
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return policyChgStatus;
	}
	private Integer getLiabilityState(Connection sourceConn, Long policyId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		Integer liabilityState = null;
		try {
			sql = "select LIABILITY_STATE from " + Table.T_CONTRACT_MASTER + " where POLICY_ID = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, policyId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				liabilityState = rs.getInt("LIABILITY_STATE");
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return liabilityState;
	}
	private String getSourceAddress1(Connection sourceConn, Long addressId) throws SQLException {
		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String address1 = null;
		try {
			sql = "select ADDRESS_1 from " + Table.T_ADDRESS + " where ADDRESS_ID = ?";
			pstmt = sourceConn.prepareStatement(sql);
			pstmt.setLong(1, addressId);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				address1 = rs.getString("ADDRESS_1");
				break;
			}
		}
		finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
		return address1;
	}

	private void insertPartyContact(Connection sourceConn, Connection sinkConn, PartyContact partyContact, String tableName, Long scn, Long commitScn, String rowId) throws Exception  {
		PreparedStatement pstmt = null;
		try {
			String sql = "select count(*) AS COUNT from " + Table.T_PARTY_CONTACT 
					+ " where role_type = " + partyContact.getRoleType() + " and list_id = " + partyContact.getListId();
			int count = getCount(sinkConn, sql);
			if (count == 0) {
				long t = System.currentTimeMillis();
				if (partyContact.getAddressId() == null) {
					sql = "insert into " + Table.T_PARTY_CONTACT + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1,INSERT_TIMESTAMP,UPDATE_TIMESTAMP,ROLE_TABLE,ROLE_SCN,ROLE_COMMIT_SCN,ROLE_ROW_ID) " 
							+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					pstmt = sinkConn.prepareStatement(sql);
					pstmt.setInt(1, partyContact.getRoleType());
					pstmt.setLong(2, partyContact.getListId());
					pstmt.setLong(3, partyContact.getPolicyId());
					pstmt.setString(4, partyContact.getName());
					pstmt.setString(5, partyContact.getCertiCode());
					pstmt.setString(6, partyContact.getMobileTel());
					if (partyContact.getRoleType() == CONTRACT_BENE_ROLE_TYPE) {
						pstmt.setNull(7, Types.VARCHAR);
					} else {
						pstmt.setString(7, partyContact.getEmail());
					}
					pstmt.setNull(8, Types.BIGINT);
					pstmt.setNull(9, Types.VARCHAR);
					pstmt.setTimestamp(10, new Timestamp(t));
					pstmt.setTimestamp(11, new Timestamp(t));
					pstmt.setString(12, tableName);
					pstmt.setLong(13, scn);
					pstmt.setLong(14, commitScn);
					pstmt.setString(15, rowId);

					pstmt.executeUpdate();
					pstmt.close();
				} else {
					String address1 = getSourceAddress1(sourceConn, partyContact.getAddressId());
					partyContact.setAddress1(address1);

					sql = "insert into " + Table.T_PARTY_CONTACT + " (ROLE_TYPE,LIST_ID,POLICY_ID,NAME,CERTI_CODE,MOBILE_TEL,EMAIL,ADDRESS_ID,ADDRESS_1,INSERT_TIMESTAMP,UPDATE_TIMESTAMP,SCN,COMMIT_SCN,ROW_ID) " 
							+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
					pstmt = sinkConn.prepareStatement(sql);
					pstmt.setInt(1, partyContact.getRoleType());
					pstmt.setLong(2, partyContact.getListId());
					pstmt.setLong(3, partyContact.getPolicyId());
					pstmt.setString(4, partyContact.getName());
					pstmt.setString(5, partyContact.getCertiCode());
					pstmt.setString(6, partyContact.getMobileTel());
					pstmt.setString(7, partyContact.getEmail());
					pstmt.setLong(8, partyContact.getAddressId());
					if (address1 == null) {
						pstmt.setNull(9, Types.VARCHAR);
					} else {
						pstmt.setString(9, partyContact.getAddress1());
					}
					pstmt.setTimestamp(10, new Timestamp(t));
					pstmt.setTimestamp(11, new Timestamp(t));

					pstmt.setLong(12, scn);
					pstmt.setLong(13, commitScn);
					pstmt.setString(14, rowId);

					pstmt.executeUpdate();
					pstmt.close();
				}

			} else {
				// record exists, error
				String error = String.format("table=%s record already exists, therefore cannot insert, role_type=%d, list_id=%d", Table.T_PARTY_CONTACT, partyContact.getRoleType(), partyContact.getListId());
				throw new Exception(error);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private void updatePartyContact(Connection sourceConn, Connection sinkConn, PartyContact partyContact, String tableName, Long scn, Long commitScn, String rowId) throws Exception  {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			String sql = "select *  from " + Table.T_PARTY_CONTACT 
					+ " where role_type = ? and list_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setInt(1, partyContact.getRoleType());
			pstmt.setLong(2, partyContact.getListId());
			rs = pstmt.executeQuery();
			Long partyAddressId = null;
			boolean exists = false;
			while (rs.next()) {
				partyAddressId = rs.getLong("ADDRESS_ID");
				exists = true;
			}
			rs.close();
			pstmt.close();
			logger.info(">>>partyAddressId={}", partyAddressId);

			long t = System.currentTimeMillis();
			if (exists) {
				if (partyContact.getAddressId() == null) {
					sql = "update " + Table.T_PARTY_CONTACT
							+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=null,ADDRESS_1=null,UPDATE_TIMESTAMP=?,ROLE_TABLE=?,ROLE_SCN=?,ROLE_COMMIT_SCN=?,ROLE_ROW_ID=?"
							+ " where ROLE_TYPE=? and LIST_ID=?";
					pstmt = sinkConn.prepareStatement(sql);
					pstmt.setLong(1, partyContact.getPolicyId());
					pstmt.setString(2, partyContact.getName());
					pstmt.setString(3, partyContact.getCertiCode());
					pstmt.setString(4, partyContact.getMobileTel());
					pstmt.setString(5, partyContact.getEmail());
					pstmt.setTimestamp(6, new Timestamp(t));
					pstmt.setString(7, tableName);
					pstmt.setLong(8, scn);
					pstmt.setLong(9, commitScn);
					pstmt.setString(10, rowId);

					pstmt.setInt(10, partyContact.getRoleType());
					pstmt.setLong(11, partyContact.getListId());

					pstmt.executeUpdate();
					pstmt.close();
				} else {

					// update without address
					if (partyAddressId != null 
							&& partyContact.getAddressId() != null
							&& partyAddressId.longValue() == partyContact.getAddressId().longValue()) {
						sql = "update " + Table.T_PARTY_CONTACT
								+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,UPDATE_TIMESTAMP=?,SCN=?,COMMIT_SCN=?,ROW_ID=?"
								+ " where ROLE_TYPE=? and LIST_ID=?";
						pstmt = sinkConn.prepareStatement(sql);
						pstmt.setLong(1, partyContact.getPolicyId());
						pstmt.setString(2, partyContact.getName());
						pstmt.setString(3, partyContact.getCertiCode());
						pstmt.setString(4, partyContact.getMobileTel());
						pstmt.setString(5, partyContact.getEmail());
						pstmt.setTimestamp(6, new Timestamp(t));
						pstmt.setLong(7, scn);
						pstmt.setLong(8, commitScn);
						pstmt.setString(9, rowId);
						pstmt.setInt(10, partyContact.getRoleType());
						pstmt.setLong(11, partyContact.getListId());

						pstmt.executeUpdate();
						pstmt.close();
					} // update with address
					else {
						// get address1
						String address1 = getSourceAddress1(sourceConn, partyContact.getAddressId());	
						partyContact.setAddress1(address1);

						// update 
						sql = "update " + Table.T_PARTY_CONTACT
								+ " set POLICY_ID=?,NAME=?,CERTI_CODE=?,MOBILE_TEL=?,EMAIL=?,ADDRESS_ID=?,ADDRESS_1=?,UPDATE_TIMESTAMP=?,SCN=?,COMMIT_SCN=?,ROW_ID=?"
								+ " where ROLE_TYPE=? and LIST_ID=?";
						pstmt = sinkConn.prepareStatement(sql);
						pstmt.setLong(1, partyContact.getPolicyId());
						pstmt.setString(2, partyContact.getName());
						pstmt.setString(3, partyContact.getCertiCode());
						pstmt.setString(4, partyContact.getMobileTel());
						pstmt.setString(5, partyContact.getEmail());
						pstmt.setLong(6, partyContact.getAddressId());
						pstmt.setString(7, partyContact.getAddress1());
						pstmt.setTimestamp(8, new Timestamp(t));
						pstmt.setLong(9, scn);
						pstmt.setLong(10, commitScn);
						pstmt.setString(11, rowId);
						pstmt.setInt(12, partyContact.getRoleType());
						pstmt.setLong(13, partyContact.getListId());

						pstmt.executeUpdate();
						pstmt.close();
					}

				}

			} else {
				// record exists, error
				String error = String.format("table=%s record does not exists, therefore cannot update, role_type=%d, list_id=%d", Table.T_PARTY_CONTACT, partyContact.getRoleType(), partyContact.getListId());
				throw new Exception(error);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
	}
	private void deletePartyContact(Connection sinkConn, Integer roleType, Long listId) throws Exception  {
		PreparedStatement pstmt = null;
		try {
			String sql = "select count(*) AS COUNT from " + Table.T_PARTY_CONTACT 
					+ " where role_type = " + roleType + " and list_id = " + listId;
			int count = getCount(sinkConn, sql);
			if (count > 0) {
				sql = "delete from " + Table.T_PARTY_CONTACT + " where role_type = ? and list_id = ?";

				pstmt = sinkConn.prepareStatement(sql);
				pstmt.setInt(1, roleType);
				pstmt.setLong(2, listId);

				pstmt.executeUpdate();
				pstmt.close();

			} else {
				// record exists, error
				String error = String.format("table=%s record does not exist, therefore cannot delete, role_type=%d, list_id=%d", Table.T_PARTY_CONTACT, roleType, listId);
				throw new Exception(error);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private void insertAddress(Connection sinkConn, Address address, Long scn, Long commitScn, String rowId) throws Exception {

		String sql = null;
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		try {
			sql = "select ROLE_TYPE,LIST_ID from " + Table.T_PARTY_CONTACT + " where address_id = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setLong(1, address.getAddressId());
			rs = pstmt.executeQuery();
			int count = 0;
			while (rs.next()) {
				count++;
				break;

			}
			rs.close();
			pstmt.close();

			if (count == 0) {
				// ignore
			} else {
				long t = System.currentTimeMillis();
				// update party contact
				sql = "update "  + Table.T_PARTY_CONTACT + " set address_1 = ?,UPDATE_TIMESTAMP=?, ADDR_SCN=?, ADDR_COMMIT_SCN=?, ADDR_ROW_ID=? where address_id = ?";
				pstmt = sinkConn.prepareStatement(sql);
				pstmt.setString(1, StringUtils.trim(address.getAddress1()));
				pstmt.setTimestamp(2, new Timestamp(t));
				pstmt.setLong(3, scn);
				pstmt.setLong(4, commitScn);
				pstmt.setString(5, rowId);
				pstmt.setLong(6, address.getAddressId());
				pstmt.executeUpdate();
				pstmt.close();
			}

		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}

	}

	private Integer getCount(Connection conn, String sql) throws SQLException {

		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet resultSet = pstmt.executeQuery();
		Integer count = 0; 
		while (resultSet.next()) {
			count = resultSet.getInt("COUNT");
		}
		resultSet.close();
		pstmt.close();

		return count;
	}

	private void updateAddress(Connection conn, Address oldAddress, Address newAddress, Long scn, Long commitScn, String rowId) throws Exception  {

		PreparedStatement pstmt = null;
		String sql = "";
		try {
			long t = System.currentTimeMillis();
			// update PartyContact
			sql = "update " + Table.T_PARTY_CONTACT
					+ " set ADDRESS_1 = ?,UPDATE_TIMESTAMP=?, ADDR_SCN=?, ADDR_COMMIT_SCN=?, ADDR_ROW_ID=? where ADDRESS_ID = ?";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, StringUtils.trim(newAddress.getAddress1()));
			pstmt.setTimestamp(2, new Timestamp(t));
			pstmt.setLong(3, scn);
			pstmt.setLong(4, commitScn);
			pstmt.setString(5, rowId);
			pstmt.setLong(6, oldAddress.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}

	private void deleteAddress(Connection sinkConn, Address address) throws Exception  {

		PreparedStatement pstmt = null;
		String sql = "";
		try {
			long t = System.currentTimeMillis();
			// update PartyContact
			sql = "update " + Table.T_PARTY_CONTACT
					+ " set ADDRESS_ID = null,ADDRESS_1 = null,UPDATE_TIMESTAMP=? where ADDRESS_ID = ?";
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setTimestamp(1, new Timestamp(t));
			pstmt.setLong(2, address.getAddressId());

			pstmt.executeUpdate();
			pstmt.close();

		} finally {
			if (pstmt != null) pstmt.close();
		}
	}
	private void updateHealthConsumerReceived(String clientId, Timestamp received) throws Exception{
		String urlStr = String.format("%s/%s/%d", URL_UPDATE_HEALTH_CPONSUMER_RECEIVED, clientId, received.getTime());
		logger.info(">>>>> updateHealthConsumerReceived  urlStr:" + urlStr);

		HttpURLConnection httpCon = null;
		try {
			URL url = new URL(urlStr);
			httpCon = (HttpURLConnection)url.openConnection();
			httpCon.setRequestMethod("POST");
			int responseCode = httpCon.getResponseCode();
			String readLine = null;
			if (httpCon.HTTP_OK == responseCode) {
				BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
				StringBuffer response = new StringBuffer();
				while ((readLine = in.readLine()) != null) {
					response.append(readLine);
				}
				in.close();

			} else {
				logger.error(">>> Response code={}", responseCode);
				throw new Exception("Response code="+responseCode);
			}
		} catch(Exception e) {
			logger.error(">>> Response code={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} finally {
			if (httpCon != null ) httpCon.disconnect();
		}
	}
}