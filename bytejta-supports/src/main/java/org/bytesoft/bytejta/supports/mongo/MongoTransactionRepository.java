/**
 * Copyright 2014-2018 yangming.liu<bytefox@126.com>.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.bytesoft.bytejta.supports.mongo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.transaction.xa.XAException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.bytejta.work.CommandManager;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionException;
import org.bytesoft.transaction.TransactionRecovery;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.supports.serialize.XAResourceDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;

public class MongoTransactionRepository
		implements TransactionRepository, TransactionEndpointAware, EnvironmentAware, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(MongoTransactionRepository.class);
	static final String CONSTANTS_ROOT_PATH = "/org/bytesoft/bytejta";
	static final String CONSTANTS_DB_NAME = "bytejta";
	static final String CONSTANTS_TB_TRANSACTIONS = "transactions";
	static final String CONSTANTS_TB_PARTICIPANTS = "participants";
	static final String CONSTANTS_FD_GLOBAL = "gxid";
	static final String CONSTANTS_FD_BRANCH = "bxid";
	static final String CONSTANTS_FD_SYSTEM = "system";

	@javax.annotation.Resource
	private MongoClient mongoClient;
	private String endpoint;
	private Environment environment;
	@javax.inject.Inject
	private MongoInstanceVersionManager versionManager;
	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;
	@javax.inject.Inject
	private CommandManager commandManager;

	public void putTransaction(TransactionXid transactionXid, Transaction transaction) {
	}

	public Transaction getTransaction(TransactionXid xid) throws TransactionException {
		XidFactory xidFactory = this.beanFactory.getXidFactory();

		try {
			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);

			String application = CommonUtils.getApplication(this.endpoint);
			byte[] global = xid.getGlobalTransactionId();

			Bson globalFilter = Filters.eq(CONSTANTS_FD_GLOBAL, ByteUtils.byteArrayToString(global));
			Bson systemFilter = Filters.eq(CONSTANTS_FD_SYSTEM, application);

			FindIterable<Document> transactionItr = transactions.find(Filters.and(globalFilter, systemFilter));
			MongoCursor<Document> transactionCursor = transactionItr.iterator();
			transactionCursor = transactionItr.iterator();
			if (transactionCursor.hasNext() == false) {
				return null;
			}

			Document document = transactionCursor.next();
			TransactionArchive archive = new TransactionArchive();

			TransactionXid globalXid = xidFactory.createGlobalXid(global);
			archive.setXid(globalXid);

			String propagatedBy = document.getString("propagated_by");
			boolean coordinator = document.getBoolean("coordinator");
			int transactionStatus = document.getInteger("status");

			archive.setCoordinator(coordinator);
			archive.setStatus(transactionStatus);
			archive.setPropagatedBy(propagatedBy);

			this.initializeParticipantList(archive);

			TransactionRecovery transactionRecovery = this.beanFactory.getTransactionRecovery();
			return transactionRecovery.reconstruct(archive);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while loading transaction(xid= {}).", xid, rex);
		} catch (Exception ex) {
			logger.error("Error occurred while loading transaction(xid= {}).", xid, ex);
		}

		return null;
	}

	private void initializeParticipantList(TransactionArchive archive) {
		XidFactory xidFactory = this.beanFactory.getXidFactory();

		MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
		MongoCollection<Document> participants = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

		TransactionXid xid = (TransactionXid) archive.getXid();
		String application = CommonUtils.getApplication(this.endpoint);
		byte[] global = xid.getGlobalTransactionId();

		Bson globalFilter = Filters.eq(CONSTANTS_FD_GLOBAL, ByteUtils.byteArrayToString(global));
		Bson systemFilter = Filters.eq(CONSTANTS_FD_SYSTEM, application);

		MongoCursor<Document> participantCursor = null;
		try {
			FindIterable<Document> participantItr = participants.find(Filters.and(globalFilter, systemFilter));
			participantCursor = participantItr.iterator();
			for (; participantCursor.hasNext();) {
				Document document = participantCursor.next();
				XAResourceArchive participant = new XAResourceArchive();

				String bxid = document.getString("bxid");

				String descriptorType = document.getString("type");
				String descriptorName = document.getString("name");

				int vote = document.getInteger("vote");
				boolean committed = document.getBoolean("committed");
				boolean rolledback = document.getBoolean("rolledback");
				boolean readonly = document.getBoolean("readonly");
				boolean completed = document.getBoolean("completed");
				boolean heuristic = document.getBoolean("heuristic");

				byte[] branchQualifier = ByteUtils.stringToByteArray(bxid);
				TransactionXid globalXid = xidFactory.createGlobalXid(global);
				TransactionXid branchXid = xidFactory.createBranchXid(globalXid, branchQualifier);
				participant.setXid(branchXid);

				XAResourceDeserializer resourceDeserializer = this.beanFactory.getResourceDeserializer();
				XAResourceDescriptor descriptor = resourceDeserializer.deserialize(descriptorName);
				if (descriptor != null //
						&& descriptor.getClass().getName().equals(descriptorType) == false) {
					throw new IllegalStateException();
				}

				participant.setVote(vote);
				participant.setCommitted(committed);
				participant.setRolledback(rolledback);
				participant.setReadonly(readonly);
				participant.setCompleted(completed);
				participant.setHeuristic(heuristic);

				participant.setDescriptor(descriptor);

				boolean remote = //
						StringUtils.equalsIgnoreCase(RemoteResourceDescriptor.class.getName(), descriptorType);
				if (remote) {
					archive.getRemoteResources().add(participant);
				} else {
					archive.getNativeResources().add(participant);
				}

			}
		} finally {
			IOUtils.closeQuietly(participantCursor);
		}
	}

	public Transaction removeTransaction(TransactionXid xid) {
		return null;
	}

	public void putErrorTransaction(TransactionXid transactionXid, Transaction transaction) {
		try {
			TransactionArchive archive = (TransactionArchive) transaction.getTransactionArchive();
			byte[] global = transactionXid.getGlobalTransactionId();
			String identifier = ByteUtils.byteArrayToString(global);

			String application = CommonUtils.getApplication(this.endpoint);

			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> collection = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);

			Document target = new Document();
			target.append("modified", this.endpoint);
			target.append("status", archive.getStatus());
			target.append("error", true);
			target.append("recovered_at", archive.getRecoveredAt() == 0 ? null : new Date(archive.getRecoveredAt()));
			target.append("recovered_times", archive.getRecoveredTimes());

			Document document = new Document();
			document.append("$set", target);

			Bson globalFilter = Filters.eq(CONSTANTS_FD_GLOBAL, identifier);
			Bson systemFilter = Filters.eq(CONSTANTS_FD_SYSTEM, application);

			UpdateResult result = collection.updateOne(Filters.and(globalFilter, systemFilter), document);
			if (result.getMatchedCount() != 1) {
				throw new IllegalStateException(
						String.format("Error occurred while updating transaction(matched= %s, modified= %s).",
								result.getMatchedCount(), result.getModifiedCount()));
			}
		} catch (RuntimeException error) {
			logger.error("Error occurred while setting the error flag.", error);
		}
	}

	public Transaction getErrorTransaction(TransactionXid xid) throws TransactionException {
		TransactionRecovery transactionRecovery = this.beanFactory.getTransactionRecovery();
		XidFactory xidFactory = this.beanFactory.getXidFactory();

		MongoCursor<Document> transactionCursor = null;
		try {
			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);

			String application = CommonUtils.getApplication(this.endpoint);
			byte[] global = xid.getGlobalTransactionId();

			Bson globalFilter = Filters.eq(CONSTANTS_FD_GLOBAL, ByteUtils.byteArrayToString(global));
			Bson systemFilter = Filters.eq(CONSTANTS_FD_SYSTEM, application);
			Bson errorFilter = Filters.eq("error", true);

			FindIterable<Document> transactionItr = transactions.find(Filters.and(globalFilter, systemFilter, errorFilter));
			transactionCursor = transactionItr.iterator();
			if (transactionCursor.hasNext() == false) {
				return null;
			}

			Document document = transactionCursor.next();
			TransactionArchive archive = new TransactionArchive();

			TransactionXid globalXid = xidFactory.createGlobalXid(global);
			archive.setXid(globalXid);

			String propagatedBy = document.getString("propagated_by");
			// boolean propagated = document.getBoolean("propagated");
			boolean coordinator = document.getBoolean("coordinator");
			int transactionStatus = document.getInteger("status");
			Integer recoveredTimes = document.getInteger("recovered_times");
			Date recoveredAt = document.getDate("recovered_at");

			archive.setRecoveredAt(recoveredAt == null ? 0 : recoveredAt.getTime());
			archive.setRecoveredTimes(recoveredTimes == null ? 0 : recoveredTimes);

			archive.setCoordinator(coordinator);
			archive.setStatus(transactionStatus);
			archive.setPropagatedBy(propagatedBy);

			this.initializeParticipantList(archive);

			return transactionRecovery.reconstruct(archive);
		} catch (RuntimeException error) {
			logger.error("Error occurred while getting error transaction.", error);
			throw new TransactionException(XAException.XAER_RMERR);
		} catch (Exception error) {
			logger.error("Error occurred while getting error transaction.", error);
			throw new TransactionException(XAException.XAER_RMERR);
		} finally {
			IOUtils.closeQuietly(transactionCursor);
		}
	}

	public Transaction removeErrorTransaction(TransactionXid xid) {
		return null;
	}

	public List<Transaction> getActiveTransactionList() {
		return new ArrayList<Transaction>();
	}

	public List<Transaction> getErrorTransactionList() throws TransactionException {
		TransactionRecovery transactionRecovery = this.beanFactory.getTransactionRecovery();
		XidFactory xidFactory = this.beanFactory.getXidFactory();

		List<Transaction> transactionList = new ArrayList<Transaction>();
		MongoCursor<Document> transactionCursor = null;
		try {
			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);

			String application = CommonUtils.getApplication(this.endpoint);
			Bson systemFilter = Filters.eq(CONSTANTS_FD_SYSTEM, application);
			Bson coordinatorFilter = Filters.eq("coordinator", true);

			FindIterable<Document> transactionItr = transactions.find(Filters.and(systemFilter, coordinatorFilter));
			transactionCursor = transactionItr.iterator();
			for (; transactionCursor.hasNext();) {
				Document document = transactionCursor.next();
				TransactionArchive archive = new TransactionArchive();

				String gxid = document.getString("gxid");
				String propagatedBy = document.getString("propagated_by");
				boolean coordinator = document.getBoolean("coordinator");
				int transactionStatus = document.getInteger("status");

				boolean error = document.getBoolean("error");
				Integer recoveredTimes = document.getInteger("recovered_times");
				Date recoveredAt = document.getDate("recovered_at");

				String targetApplication = document.getString("created");
				long expectVersion = document.getLong("version");
				long actualVersion = this.versionManager.getInstanceVersion(targetApplication);

				if (error == false && actualVersion > 0 && actualVersion <= expectVersion) {
					continue; // ignore
				}

				byte[] globalByteArray = ByteUtils.stringToByteArray(gxid);
				TransactionXid globalXid = xidFactory.createGlobalXid(globalByteArray);
				archive.setXid(globalXid);

				archive.setCoordinator(coordinator);
				archive.setStatus(transactionStatus);
				archive.setPropagatedBy(propagatedBy);

				archive.setRecoveredAt(recoveredAt == null ? 0 : recoveredAt.getTime());
				archive.setRecoveredTimes(recoveredTimes == null ? 0 : recoveredTimes);

				this.initializeParticipantList(archive);

				Transaction transaction = transactionRecovery.reconstruct(archive);
				transactionList.add(transaction);
			}

			return transactionList;
		} catch (RuntimeException rex) {
			logger.error("Error occurred while loading transactions.", rex);
			throw new TransactionException(XAException.XAER_RMERR);
		} catch (Exception ex) {
			logger.error("Error occurred while loading transactions.", ex);
			throw new TransactionException(XAException.XAER_RMERR);
		} finally {
			IOUtils.closeQuietly(transactionCursor);
		}

	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

	public Environment getEnvironment() {
		return environment;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public MongoInstanceVersionManager getVersionManager() {
		return versionManager;
	}

	public void setVersionManager(MongoInstanceVersionManager versionManager) {
		this.versionManager = versionManager;
	}

	public CommandManager getCommandManager() {
		return commandManager;
	}

	public void setCommandManager(CommandManager commandManager) {
		this.commandManager = commandManager;
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
