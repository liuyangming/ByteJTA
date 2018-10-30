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
import java.util.Iterator;
import java.util.List;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.commons.io.IOUtils;
import org.bson.Document;
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
import org.springframework.beans.factory.SmartInitializingSingleton;
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
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoTransactionLogger implements TransactionLogger, TransactionResourceListener, EnvironmentAware,
		TransactionEndpointAware, TransactionBeanFactoryAware, SmartInitializingSingleton {
	static Logger logger = LoggerFactory.getLogger(MongoTransactionLogger.class);
	static final String CONSTANTS_TB_TRANSACTIONS = "transactions";
	static final String CONSTANTS_FD_GLOBAL = "gxid";
	static final String CONSTANTS_FD_BRANCH = "bxid";

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

	public void createTransaction(TransactionArchive archive) {
		try {
			long version = this.versionManager.getInstanceVersion(this.endpoint);
			if (version <= 0) {
				throw new IllegalStateException(String.format("Invalid version(%s)!", this.endpoint));
			}

			TransactionXid transactionXid = (TransactionXid) archive.getXid();

			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			String application = CommonUtils.getApplication(this.endpoint);

			byte[] global = transactionXid.getGlobalTransactionId();
			String identifier = ByteUtils.byteArrayToString(global);

			XAResourceArchive optimized = archive.getOptimizedResource();

			Document document = new Document();
			document.append(CONSTANTS_FD_GLOBAL, identifier);
			document.append("system", application);
			document.append("propagated_by", archive.getPropagatedBy());
			document.append("coordinator", archive.isCoordinator());
			document.append("version", version);
			document.append("status", archive.getStatus());
			document.append("vote", archive.getVote());
			document.append("strategy", archive.getTransactionStrategyType());
			document.append("created", this.endpoint);
			document.append("modified", this.endpoint);
			document.append("error", false);
			document.append("participants", this.constructResourcesDocument(archive.getRemoteResources()));
			document.append("xaresources", this.constructResourcesDocument(archive.getNativeResources()));
			document.append("optimized",
					optimized == null ? null : this.constructResourceDocument(archive.getOptimizedResource()));
			document.append("recovered_at", archive.getRecoveredAt() == 0 ? null : new Date(archive.getRecoveredAt()));
			document.append("recovered_times", archive.getRecoveredTimes());

			transactions.insertOne(document);
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

			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			Document variables = new Document();
			variables.append("status", archive.getStatus());
			variables.append("vote", archive.getVote());
			variables.append("modified", this.endpoint);
			variables.append("participants", this.constructResourcesDocument(archive.getRemoteResources()));
			variables.append("xaresources", this.constructResourcesDocument(archive.getNativeResources()));
			variables.append("recovered_at", archive.getRecoveredAt() == 0 ? null : new Date(archive.getRecoveredAt()));
			variables.append("recovered_times", archive.getRecoveredTimes());

			Document document = new Document();
			document.append("$set", variables);

			UpdateResult result = transactions.updateOne(Filters.eq(CONSTANTS_FD_GLOBAL, identifier), document);
			if (result.getMatchedCount() != 1) {
				throw new IllegalStateException(
						String.format("Error occurred while updating transaction(matched= %s, modified= %s).",
								result.getMatchedCount(), result.getModifiedCount()));
			}

		} catch (RuntimeException rex) {
			logger.error("Error occurred while updating transaction.", rex);
		}
	}

	private Document constructResourcesDocument(List<XAResourceArchive> resourceList) {
		Document documents = new Document();
		for (int i = 0; resourceList != null && i < resourceList.size(); i++) {
			XAResourceArchive resource = resourceList.get(i);
			TransactionXid resourceXid = (TransactionXid) resource.getXid();
			byte[] branchByteArray = resourceXid.getBranchQualifier();
			String branchKey = ByteUtils.byteArrayToString(branchByteArray);

			Document document = this.constructResourceDocument(resource);

			documents.append(branchKey, document);
		}

		return documents;
	}

	private Document constructResourceDocument(XAResourceArchive resource) {
		TransactionXid resourceXid = (TransactionXid) resource.getXid();
		byte[] globalByteArray = resourceXid.getGlobalTransactionId();
		byte[] branchByteArray = resourceXid.getBranchQualifier();
		String globalKey = ByteUtils.byteArrayToString(globalByteArray);
		String branchKey = ByteUtils.byteArrayToString(branchByteArray);

		XAResourceDescriptor descriptor = resource.getDescriptor();
		String descriptorType = descriptor.getClass().getName();
		String descriptorName = descriptor.getIdentifier();

		int branchVote = resource.getVote();
		boolean readonly = resource.isReadonly();
		boolean committed = resource.isCommitted();
		boolean rolledback = resource.isRolledback();
		boolean completed = resource.isCompleted();
		boolean heuristic = resource.isHeuristic();

		Document document = new Document();
		document.append(CONSTANTS_FD_GLOBAL, globalKey);
		document.append(CONSTANTS_FD_BRANCH, branchKey);

		document.append("type", descriptorType);
		document.append("name", descriptorName);

		document.append("vote", branchVote);
		document.append("committed", committed);
		document.append("rolledback", rolledback);
		document.append("readonly", readonly);
		document.append("completed", completed);
		document.append("heuristic", heuristic);

		return document;
	}

	public void deleteTransaction(TransactionArchive archive) {
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();

			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
			String globalKey = ByteUtils.byteArrayToString(globalTransactionId);

			DeleteResult result = transactions.deleteOne(Filters.eq(CONSTANTS_FD_GLOBAL, globalKey));
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
			this.upsertParticipant((TransactionXid) archive.getXid(), archive);
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

	private void upsertParticipant(TransactionXid transactionXid, XAResourceArchive archive) {
		byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
		byte[] branchQualifier = transactionXid.getBranchQualifier();

		String globalKey = ByteUtils.byteArrayToString(globalTransactionId);
		String branchKey = ByteUtils.byteArrayToString(branchQualifier);

		XAResourceDescriptor descriptor = archive.getDescriptor();
		String descriptorType = descriptor.getClass().getName();
		String descriptorName = descriptor.getIdentifier();

		int branchVote = archive.getVote();
		boolean readonly = archive.isReadonly();
		boolean committed = archive.isCommitted();
		boolean rolledback = archive.isRolledback();
		boolean completed = archive.isCompleted();
		boolean heuristic = archive.isHeuristic();

		Document participant = new Document();
		participant.append("type", descriptorType);
		participant.append("name", descriptorName);

		participant.append("vote", branchVote);
		participant.append("committed", committed);
		participant.append("rolledback", rolledback);
		participant.append("readonly", readonly);
		participant.append("completed", completed);
		participant.append("heuristic", heuristic);

		String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
		MongoDatabase database = this.mongoClient.getDatabase(databaseName);
		MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

		Document participants = new Document();
		participants.append(String.format("participants.%s", branchKey), participant);

		Document document = new Document();
		document.append("$set", participants);

		UpdateResult result = transactions.updateOne(Filters.eq(CONSTANTS_FD_GLOBAL, globalKey), document);
		if (result.getMatchedCount() != 1) {
			throw new IllegalStateException(
					String.format("Error occurred while creating/updating participant(matched= %s, modified= %s).",
							result.getMatchedCount(), result.getModifiedCount()));
		}
	}

	public void deleteParticipant(XAResourceArchive archive) {
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();
			byte[] global = transactionXid.getGlobalTransactionId();
			byte[] branch = transactionXid.getBranchQualifier();
			String globalKey = ByteUtils.byteArrayToString(global);
			String branchKey = ByteUtils.byteArrayToString(branch);

			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			Document participants = new Document();
			participants.append(String.format("participants.%s", branchKey), null);

			Document document = new Document();
			document.append("$unset", participants);

			UpdateResult result = transactions.updateOne(Filters.eq(CONSTANTS_FD_GLOBAL, globalKey), document);
			if (result.getMatchedCount() != 1) {
				throw new IllegalStateException(
						String.format("Error occurred while deleting participant(matched= %s, modified= %s).",
								result.getMatchedCount(), result.getModifiedCount()));
			}
		} catch (RuntimeException error) {
			logger.error("Error occurred while deleting participant.", error);
			this.beanFactory.getTransactionManager().setRollbackOnlyQuietly();
		}
	}

	public void recover(TransactionRecoveryCallback callback) {
		MongoCursor<Document> transactionCursor = null;
		try {
			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			FindIterable<Document> transactionItr = transactions.find(Filters.eq("coordinator", true));
			for (transactionCursor = transactionItr.iterator(); transactionCursor.hasNext();) {
				Document document = transactionCursor.next();
				boolean error = document.getBoolean("error");

				String targetApplication = document.getString("system");
				long expectVersion = document.getLong("version");
				long actualVersion = this.versionManager.getInstanceVersion(targetApplication);

				if (error == false && actualVersion > 0 && actualVersion <= expectVersion) {
					continue; // ignore
				}

				callback.recover(this.reconstructTransactionArchive(document));
			}
		} catch (RuntimeException rex) {
			logger.error("Error occurred while recovering transaction.", rex);
		} catch (Exception ex) {
			logger.error("Error occurred while recovering transaction.", ex);
		} finally {
			IOUtils.closeQuietly(transactionCursor);
		}
	}

	public TransactionArchive reconstructTransactionArchive(Document document) throws Exception {
		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionArchive archive = new TransactionArchive();

		String global = document.getString(CONSTANTS_FD_GLOBAL);
		byte[] globalTransactionId = ByteUtils.stringToByteArray(global);
		TransactionXid globalXid = xidFactory.createGlobalXid(globalTransactionId);
		archive.setXid(globalXid);

		String propagatedBy = document.getString("propagated_by");
		boolean coordinator = document.getBoolean("coordinator");
		int transactionStatus = document.getInteger("status");
		int vote = document.getInteger("vote");
		int strategy = document.getInteger("strategy");
		Integer recoveredTimes = document.getInteger("recovered_times");
		Date recoveredAt = document.getDate("recovered_at");

		archive.setCoordinator(coordinator);
		archive.setStatus(transactionStatus);
		archive.setVote(vote);
		archive.setPropagatedBy(propagatedBy);
		archive.setTransactionStrategyType(strategy);

		archive.setRecoveredAt(recoveredAt == null ? 0 : recoveredAt.getTime());
		archive.setRecoveredTimes(recoveredTimes == null ? 0 : recoveredTimes);

		Document optimized = document.get("optimized", Document.class);
		if (optimized != null) {
			archive.setOptimizedResource(this.reconstructXAResourceArchive(optimized));
		}

		Document natives = document.get("xaresources", Document.class);
		for (Iterator<String> itr = natives.keySet().iterator(); itr.hasNext();) {
			String key = itr.next();
			Document element = natives.get(key, Document.class);
			XAResourceArchive resourceArchive = this.reconstructXAResourceArchive(element);
			archive.getNativeResources().add(resourceArchive);
		}

		Document remotes = document.get("participants", Document.class);
		for (Iterator<String> itr = remotes.keySet().iterator(); itr.hasNext();) {
			String key = itr.next();
			Document element = remotes.get(key, Document.class);
			XAResourceArchive resourceArchive = this.reconstructXAResourceArchive(element);
			archive.getRemoteResources().add(resourceArchive);
		}

		return archive;
	}

	public XAResourceArchive reconstructXAResourceArchive(Document document) {
		XidFactory xidFactory = this.beanFactory.getXidFactory();

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

		return participant;
	}

	public void onEnlistResource(Xid xid, XAResource xares) {
	}

	public void onDelistResource(Xid xid, XAResource xares) {
		this.upsertParticipant((TransactionXid) xid, (XAResourceArchive) xares);
	}

	public void afterSingletonsInstantiated() {
		try {
			this.afterPropertiesSet();
		} catch (Exception error) {
			throw new RuntimeException(error);
		}
	}

	public void afterPropertiesSet() throws Exception {
		if (this.initializeEnabled) {
			this.initializeIndexIfNecessary();
		}
	}

	private void initializeIndexIfNecessary() {
		this.createTransactionsGlobalTxKeyIndexIfNecessary();
	}

	private void createTransactionsGlobalTxKeyIndexIfNecessary() {
		String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
		MongoDatabase database = this.mongoClient.getDatabase(databaseName);
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
				boolean lengthEquals = key.size() == 1;
				transactionIndexExists = lengthEquals && globalExists;

				if (transactionIndexExists && (unique == null || unique == false)) {
					throw new IllegalStateException();
				}
			}
		} finally {
			IOUtils.closeQuietly(transactionCursor);
		}

		if (transactionIndexExists == false) {
			Document index = new Document(CONSTANTS_FD_GLOBAL, 1);
			transactions.createIndex(index, new IndexOptions().unique(true));
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
