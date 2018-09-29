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
package org.bytesoft.bytejta.supports.internal;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.recovery.TransactionRecoveryCallback;
import org.bytesoft.transaction.supports.TransactionResourceListener;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.supports.serialize.XAResourceDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;

import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoTransactionLogger implements TransactionLogger, TransactionResourceListener, EnvironmentAware,
		TransactionEndpointAware, TransactionBeanFactoryAware, InitializingBean {
	static Logger logger = LoggerFactory.getLogger(MongoTransactionLogger.class);
	static final String CONSTANTS_DB_NAME = "bytejta";
	static final String CONSTANTS_TB_TRANSACTIONS = "transactions";
	static final String CONSTANTS_TB_PARTICIPANTS = "participants";
	static final String CONSTANTS_FD_GLOBAL = "gxid";
	static final String CONSTANTS_FD_BRANCH = "bxid";
	static final String CONSTANTS_FD_SYSTEM = "system";

	static final int MONGODB_ERROR_DUPLICATE_KEY = 11000;

	@javax.annotation.Resource
	private MongoClient mongoClient;
	private String endpoint;
	private Environment environment;
	@javax.inject.Inject
	private MongoInstanceVersionManager versionManager;
	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;
	private volatile boolean initializeEnabled = true;

	public void afterPropertiesSet() throws Exception {
		if (this.initializeEnabled) {
			this.initializeIndexIfNecessary();
		}
	}

	private void initializeIndexIfNecessary() {
		this.createTransactionsGlobalTxKeyIndexIfNecessary();
		this.createTransactionsApplicationIndexIfNecessary();
		this.createParticipantsGlobalTxKeyIndexIfNecessary();
	}

	private void createTransactionsApplicationIndexIfNecessary() {
		MongoDatabase database = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
		MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);
		ListIndexesIterable<Document> transactionIndexList = transactions.listIndexes();
		boolean applicationIndexExists = false;
		MongoCursor<Document> applicationCursor = null;
		try {
			applicationCursor = transactionIndexList.iterator();
			while (applicationIndexExists == false && applicationCursor.hasNext()) {
				Document document = applicationCursor.next();
				Boolean unique = document.getBoolean("unique");
				Document key = (Document) document.get("key");

				boolean systemExists = key.containsKey(CONSTANTS_FD_SYSTEM);
				boolean lengthEquals = key.size() == 1;
				applicationIndexExists = lengthEquals && systemExists;

				if (applicationIndexExists && unique != null && unique) {
					throw new IllegalStateException();
				}
			}
		} finally {
			IOUtils.closeQuietly(applicationCursor);
		}

		if (applicationIndexExists == false) {
			transactions.createIndex(new Document(CONSTANTS_FD_SYSTEM, 1), new IndexOptions().unique(false));
		}
	}

	private void createTransactionsGlobalTxKeyIndexIfNecessary() {
		MongoDatabase database = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
		MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);
		ListIndexesIterable<Document> transactionIndexList = transactions.listIndexes();
		boolean transactionIndexExists = false;
		MongoCursor<Document> transactionCursor = null;
		try {
			transactionCursor = transactionIndexList.iterator();
			while (transactionIndexExists == false && transactionCursor.hasNext()) {
				Document document = transactionCursor.next();
				Boolean unique = document.getBoolean("unique");
				Document key = (Document) document.get("key");

				boolean globalExists = key.containsKey(CONSTANTS_FD_GLOBAL);
				boolean systemExists = key.containsKey(CONSTANTS_FD_SYSTEM);
				boolean lengthEquals = key.size() == 2;
				transactionIndexExists = lengthEquals && globalExists && systemExists;

				if (transactionIndexExists && (unique == null || unique == false)) {
					throw new IllegalStateException();
				}
			}
		} finally {
			IOUtils.closeQuietly(transactionCursor);
		}

		if (transactionIndexExists == false) {
			Document index = new Document(CONSTANTS_FD_GLOBAL, 1).append(CONSTANTS_FD_SYSTEM, 1);
			transactions.createIndex(index, new IndexOptions().unique(true));
		}
	}

	private void createParticipantsGlobalTxKeyIndexIfNecessary() {
		MongoDatabase database = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
		MongoCollection<Document> participants = database.getCollection(CONSTANTS_TB_PARTICIPANTS);
		ListIndexesIterable<Document> participantIndexList = participants.listIndexes();
		boolean participantIndexExists = false;
		MongoCursor<Document> participantCursor = null;
		try {
			participantCursor = participantIndexList.iterator();
			while (participantIndexExists == false && participantCursor.hasNext()) {
				Document document = participantCursor.next();
				Boolean unique = document.getBoolean("unique");
				Document key = (Document) document.get("key");

				boolean globalExists = key.containsKey(CONSTANTS_FD_GLOBAL);
				boolean branchExists = key.containsKey(CONSTANTS_FD_BRANCH);
				boolean lengthEquals = key.size() == 2;
				participantIndexExists = lengthEquals && globalExists && branchExists;

				if (participantIndexExists && (unique == null || unique == false)) {
					throw new IllegalStateException();
				}
			}
		} finally {
			IOUtils.closeQuietly(participantCursor);
		}

		if (participantIndexExists == false) {
			Document index = new Document(CONSTANTS_FD_GLOBAL, 1).append(CONSTANTS_FD_BRANCH, 1);
			participants.createIndex(index, new IndexOptions().unique(true));
		}
	}

	public void onEnlistResource(Xid xid, XAResource xares) {
	}

	public void onDelistResource(Xid xid, XAResource xares) {
		this.upsertParticipant((TransactionXid) xid, (XAResourceArchive) xares);
	}

	public void createTransaction(TransactionArchive archive) {
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();
			boolean coordinator = archive.isCoordinator();
			Object propagatedBy = archive.getPropagatedBy();
			int status = archive.getStatus();
			// boolean propagated = archive.isPropagated();

			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> collection = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);

			String application = CommonUtils.getApplication(this.endpoint);

			byte[] global = transactionXid.getGlobalTransactionId();
			String identifier = ByteUtils.byteArrayToString(global);

			long version = this.versionManager.getInstanceVersion(this.endpoint);
			if (version <= 0) {
				throw new IllegalStateException(String.format("Invalid version(%s)!", this.endpoint));
			}

			Document document = new Document();
			document.append(CONSTANTS_FD_GLOBAL, identifier);
			document.append(CONSTANTS_FD_SYSTEM, application);
			document.append("created", this.endpoint);
			document.append("modified", this.endpoint);
			document.append("propagated_by", propagatedBy);
			document.append("coordinator", coordinator);
			document.append("status", status);
			document.append("lock", 0);
			document.append("locked_by", this.endpoint);
			document.append("error", false);
			document.append("version", version);

			collection.insertOne(document);

			List<XAResourceArchive> nativeList = archive.getNativeResources();
			for (int i = 0; nativeList != null && i < nativeList.size(); i++) {
				XAResourceArchive participant = nativeList.get(i);
				this.updateParticipant(participant);
			}

			List<XAResourceArchive> remoteList = archive.getRemoteResources();
			for (int i = 0; remoteList != null && i < remoteList.size(); i++) {
				XAResourceArchive participant = remoteList.get(i);
				this.updateParticipant(participant);
			}
		} catch (RuntimeException error) {
			logger.error("Error occurred while creating transaction.", error);
			this.beanFactory.getTransactionManager().setRollbackOnlyQuietly();
		}
	}

	public void updateTransaction(TransactionArchive archive) {
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();
			byte[] global = transactionXid.getGlobalTransactionId();
			String identifier = ByteUtils.byteArrayToString(global);

			int vote = archive.getVote();
			int status = archive.getStatus();

			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> collection = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);

			Document variables = new Document();
			variables.append("modified", this.endpoint);
			variables.append("vote", vote);
			variables.append("status", status);
			variables.append("recovered_at", archive.getRecoveredAt() == 0 ? null : new Date(archive.getRecoveredAt()));
			variables.append("recovered_times", archive.getRecoveredTimes());

			Document document = new Document();
			document.append("$set", variables);

			String application = CommonUtils.getApplication(this.endpoint);

			Bson globalFilter = Filters.eq(CONSTANTS_FD_GLOBAL, identifier);
			Bson systemFilter = Filters.eq(CONSTANTS_FD_SYSTEM, application);
			UpdateResult result = collection.updateOne(Filters.and(globalFilter, systemFilter), document);
			if (result.getMatchedCount() != 1) {
				throw new IllegalStateException(
						String.format("Error occurred while updating transaction(matched= %s, modified= %s).",
								result.getMatchedCount(), result.getModifiedCount()));
			}

			List<XAResourceArchive> nativeResourceList = archive.getNativeResources();
			for (int i = 0; nativeResourceList != null && i < nativeResourceList.size(); i++) {
				XAResourceArchive resourceArchive = nativeResourceList.get(i);
				this.updateParticipant(resourceArchive);
			}

			List<XAResourceArchive> remoteResourceList = archive.getRemoteResources();
			for (int i = 0; remoteResourceList != null && i < remoteResourceList.size(); i++) {
				XAResourceArchive resourceArchive = remoteResourceList.get(i);
				this.updateParticipant(resourceArchive);
			}

		} catch (RuntimeException rex) {
			logger.error("Error occurred while updating transaction.", rex);
		}
	}

	public void deleteTransaction(TransactionArchive archive) {
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();

			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);
			MongoCollection<Document> participants = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

			String application = CommonUtils.getApplication(this.endpoint);

			byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
			Bson xidBson = Filters.eq(CONSTANTS_FD_GLOBAL, ByteUtils.byteArrayToString(globalTransactionId));
			Bson created = Filters.eq(CONSTANTS_FD_SYSTEM, application);

			participants.deleteMany(Filters.and(xidBson, created));

			DeleteResult result = transactions.deleteOne(Filters.and(xidBson, created));
			if (result.getDeletedCount() != 1) {
				throw new IllegalStateException(
						String.format("Error occurred while deleting transaction(deleted= %s).", result.getDeletedCount()));
			}
		} catch (RuntimeException rex) {
			logger.error("Error occurred while deleting transaction!", rex);
		}
	}

	public void createResource(XAResourceArchive archive) {
	}

	public void updateResource(XAResourceArchive archive) {
	}

	public void deleteResource(XAResourceArchive archive) {
	}

	public void createParticipant(XAResourceArchive archive) {
		try {
			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> collection = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

			Document document = new Document();

			TransactionXid transactionXid = (TransactionXid) archive.getXid();
			byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
			byte[] branchQualifier = transactionXid.getBranchQualifier();

			XAResourceDescriptor descriptor = archive.getDescriptor();
			String descriptorType = descriptor.getClass().getName();
			String descriptorName = descriptor.getIdentifier();

			int branchVote = archive.getVote();
			boolean readonly = archive.isReadonly();
			boolean committed = archive.isCommitted();
			boolean rolledback = archive.isRolledback();
			boolean completed = archive.isCompleted();
			boolean heuristic = archive.isHeuristic();

			String application = CommonUtils.getApplication(this.endpoint);

			document.put(CONSTANTS_FD_GLOBAL, ByteUtils.byteArrayToString(globalTransactionId));
			document.put(CONSTANTS_FD_BRANCH, ByteUtils.byteArrayToString(branchQualifier));
			document.put(CONSTANTS_FD_SYSTEM, application);

			document.put("type", descriptorType);
			document.put("name", descriptorName);

			document.put("vote", branchVote);
			document.put("committed", committed);
			document.put("rolledback", rolledback);
			document.put("readonly", readonly);
			document.put("completed", completed);
			document.put("heuristic", heuristic);

			document.put("created", this.endpoint);
			document.put("modified", this.endpoint);

			collection.insertOne(document);
		} catch (com.mongodb.MongoWriteException error) {
			com.mongodb.WriteError writeError = error.getError();
			if (MONGODB_ERROR_DUPLICATE_KEY == writeError.getCode()) {
				// this.upsertParticipant(transactionXid, archive);
				this.updateParticipant(archive);
			} else {
				throw error;
			}
		} catch (RuntimeException error) {
			logger.error("Error occurred while creating participant!", error);
			this.beanFactory.getTransactionManager().setRollbackOnlyQuietly();
		}
	}

	public void updateParticipant(XAResourceArchive archive) {
		try {
			this.upsertParticipant((TransactionXid) archive.getXid(), archive);
		} catch (RuntimeException error) {
			logger.error("Error occurred while updating participant.", error);
			this.beanFactory.getTransactionManager().setRollbackOnlyQuietly();
		}
	}

	public void upsertParticipant(TransactionXid transactionXid, XAResourceArchive archive) {
		MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
		MongoCollection<Document> collection = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

		byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
		byte[] branchQualifier = transactionXid.getBranchQualifier();

		XAResourceDescriptor descriptor = archive.getDescriptor();
		String descriptorType = descriptor.getClass().getName();
		String descriptorName = descriptor.getIdentifier();

		int branchVote = archive.getVote();
		boolean readonly = archive.isReadonly();
		boolean committed = archive.isCommitted();
		boolean rolledback = archive.isRolledback();
		boolean completed = archive.isCompleted();
		boolean heuristic = archive.isHeuristic();

		Document variables = new Document();
		variables.append("type", descriptorType);
		variables.append("name", descriptorName);

		variables.append("vote", branchVote);
		variables.append("committed", committed);
		variables.append("rolledback", rolledback);
		variables.append("readonly", readonly);
		variables.append("completed", completed);
		variables.append("heuristic", heuristic);

		variables.append("modified", this.endpoint);

		Document document = new Document();
		document.append("$set", variables);

		String application = CommonUtils.getApplication(this.endpoint);

		Bson gxidBson = Filters.eq(CONSTANTS_FD_GLOBAL, ByteUtils.byteArrayToString(globalTransactionId));
		Bson bxidBson = Filters.eq(CONSTANTS_FD_BRANCH, ByteUtils.byteArrayToString(branchQualifier));
		Bson crteBson = Filters.eq(CONSTANTS_FD_SYSTEM, application);

		Bson condition = Filters.and(gxidBson, bxidBson, crteBson);
		UpdateResult result = collection.updateOne(condition, document, new UpdateOptions().upsert(true));
		if (result.getUpsertedId() == null && result.getMatchedCount() != 1) {
			throw new IllegalStateException(
					String.format("Error occurred while upserting participant(matched= %s, modified= %s).",
							result.getMatchedCount(), result.getModifiedCount()));
		}
	}

	public void deleteParticipant(XAResourceArchive archive) {
		TransactionXid transactionXid = (TransactionXid) archive.getXid();
		byte[] global = transactionXid.getGlobalTransactionId();
		byte[] branch = transactionXid.getBranchQualifier();
		String globalKey = ByteUtils.byteArrayToString(global);
		String branchKey = ByteUtils.byteArrayToString(branch);

		MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
		MongoCollection<Document> collection = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

		Bson globalFilter = Filters.eq(CONSTANTS_FD_GLOBAL, globalKey);
		Bson branchFilter = Filters.eq(CONSTANTS_FD_BRANCH, branchKey);

		DeleteResult result = collection.deleteOne(Filters.and(globalFilter, branchFilter));
		if (result.getDeletedCount() != 1) {
			logger.error("Error occurred while deleting participant(deleted= {}).", result.getDeletedCount());
		}
	}

	public void recover(TransactionRecoveryCallback callback) {
		XidFactory xidFactory = this.beanFactory.getXidFactory();

		Map<Xid, TransactionArchive> archiveMap = new HashMap<Xid, TransactionArchive>();
		try {
			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);
			MongoCollection<Document> participants = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

			String application = CommonUtils.getApplication(this.endpoint);
			Bson filter = Filters.eq(CONSTANTS_FD_SYSTEM, application);

			FindIterable<Document> transactionItr = transactions.find(filter);
			MongoCursor<Document> transactionCursor = transactionItr.iterator();
			for (; transactionCursor.hasNext();) {
				Document document = transactionCursor.next();
				TransactionArchive archive = new TransactionArchive();

				String gxid = document.getString(CONSTANTS_FD_GLOBAL);
				byte[] globalTransactionId = ByteUtils.stringToByteArray(gxid);
				TransactionXid globalXid = xidFactory.createGlobalXid(globalTransactionId);
				archive.setXid(globalXid);

				String propagatedBy = document.getString("propagated_by");
				boolean coordinator = document.getBoolean("coordinator");
				int transactionStatus = document.getInteger("status");

				archive.setCoordinator(coordinator);
				archive.setStatus(transactionStatus);
				archive.setPropagatedBy(propagatedBy);

				archiveMap.put(globalXid, archive);
			}

			FindIterable<Document> participantItr = participants.find(filter);
			MongoCursor<Document> participantCursor = participantItr.iterator();
			for (; participantCursor.hasNext();) {
				Document document = participantCursor.next();
				XAResourceArchive participant = new XAResourceArchive();

				String gxid = document.getString(CONSTANTS_FD_GLOBAL);
				String bxid = document.getString(CONSTANTS_FD_BRANCH);

				String descriptorType = document.getString("type");
				String descriptorName = document.getString("name");

				int vote = document.getInteger("vote");
				boolean committed = document.getBoolean("committed");
				boolean rolledback = document.getBoolean("rolledback");
				boolean readonly = document.getBoolean("readonly");
				boolean completed = document.getBoolean("completed");
				boolean heuristic = document.getBoolean("heuristic");

				byte[] globalTransactionId = ByteUtils.stringToByteArray(gxid);
				byte[] branchQualifier = ByteUtils.stringToByteArray(bxid);
				TransactionXid globalXid = xidFactory.createGlobalXid(globalTransactionId);
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

				TransactionArchive archive = archiveMap.get(globalXid);
				if (archive == null) {
					throw new IllegalStateException();
				}

				boolean remote = //
						StringUtils.equalsIgnoreCase(RemoteResourceDescriptor.class.getName(), descriptorType);
				if (remote) {
					archive.getRemoteResources().add(participant);
				} else {
					archive.getNativeResources().add(participant);
				}

			}

		} catch (RuntimeException rex) {
			logger.error("Error occurred while recovering transaction.", rex);
		} catch (Exception ex) {
			logger.error("Error occurred while recovering transaction.", ex);
		}

		Iterator<Map.Entry<Xid, TransactionArchive>> itr = archiveMap.entrySet().iterator();
		for (; itr.hasNext();) {
			Map.Entry<Xid, TransactionArchive> entry = itr.next();
			TransactionArchive archive = entry.getValue();
			callback.recover(archive);
		}

	}

	public Environment getEnvironment() {
		return environment;
	}

	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	public MongoInstanceVersionManager getVersionManager() {
		return versionManager;
	}

	public void setVersionManager(MongoInstanceVersionManager versionManager) {
		this.versionManager = versionManager;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

	public TransactionBeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
