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

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
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

	@javax.annotation.Resource(name = "transactionMongoClient")
	private MongoClient mongoClient;
	private String endpoint;
	private Environment environment;
	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;
	private boolean initializeEnabled = true;

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

				if (applicationIndexExists && unique) {
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

				if (transactionIndexExists && unique == false) {
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

				if (participantIndexExists && unique == false) {
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
		this.createOrUpdateResource((XAResourceArchive) xares);
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

			String[] values = this.endpoint.split("\\s*:\\s*");
			String application = values[1];

			byte[] global = transactionXid.getGlobalTransactionId();
			String identifier = ByteUtils.byteArrayToString(global);

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
			document.append("version", 0L);

			collection.insertOne(document);
		} catch (RuntimeException error) {
			logger.error("Error occurred while creating transaction.", error);
			this.beanFactory.getTransactionManager().setRollbackOnlyQuietly();
		}

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
	}

	public void updateTransaction(TransactionArchive archive) {
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();
			int vote = archive.getVote();
			int status = archive.getStatus();

			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> collection = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);

			Document variables = new Document();
			variables.append("modified", this.endpoint);
			variables.append("vote", vote);
			variables.append("status", status);

			Document document = new Document();
			document.append("$set", variables);
			document.append("$inc", new BasicDBObject("version", 1));

			String[] values = this.endpoint.split(":");
			String application = values[1];

			byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
			Bson xidBson = Filters.eq("gxid", ByteUtils.byteArrayToString(globalTransactionId));
			Bson created = Filters.eq("application", application);

			UpdateResult result = collection.updateOne(Filters.and(xidBson, created), document);
			if (result.getModifiedCount() != 1) {
				throw new IllegalStateException(
						String.format("Error occurred while updating transaction(matched= %s, modified= %s).",
								result.getMatchedCount(), result.getModifiedCount()));
			}

			List<XAResourceArchive> nativeResourceList = archive.getNativeResources();
			for (int i = 0; nativeResourceList != null && i < nativeResourceList.size(); i++) {
				XAResourceArchive resourceArchive = nativeResourceList.get(i);
				this.createOrUpdateResource(resourceArchive);
			}

			List<XAResourceArchive> remoteResourceList = archive.getRemoteResources();
			for (int i = 0; remoteResourceList != null && i < remoteResourceList.size(); i++) {
				XAResourceArchive resourceArchive = remoteResourceList.get(i);
				this.createOrUpdateResource(resourceArchive);
			}

		} catch (RuntimeException rex) {
			logger.error("Error occurred while updating transaction.", rex);
		}
	}

	private void createOrUpdateResource(XAResourceArchive archive) {
		try {
			this.updateParticipant(archive);
		} catch (IllegalStateException ex) {
			this.createParticipant(archive);
		}
	}

	public void deleteTransaction(TransactionArchive archive) {
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();

			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);
			MongoCollection<Document> participants = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

			String[] values = this.endpoint.split(":");
			String application = values[1];

			byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
			Bson xidBson = Filters.eq("gxid", ByteUtils.byteArrayToString(globalTransactionId));
			Bson created = Filters.eq("application", application);

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
		TransactionXid transactionXid = (TransactionXid) archive.getXid();

		MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
		MongoCollection<Document> collection = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

		Document document = new Document();
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

		String[] values = this.endpoint.split(":");
		String application = values[1];

		document.put("gxid", ByteUtils.byteArrayToString(globalTransactionId));
		document.put("bxid", ByteUtils.byteArrayToString(branchQualifier));
		document.put("application", application);

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
	}

	public void updateParticipant(XAResourceArchive archive) {
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();

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

			String[] values = this.endpoint.split(":");
			String application = values[1];

			Bson gxidBson = Filters.eq("gxid", ByteUtils.byteArrayToString(globalTransactionId));
			Bson bxidBson = Filters.eq("bxid", ByteUtils.byteArrayToString(branchQualifier));
			Bson crteBson = Filters.eq("application", application);

			UpdateResult result = collection.updateOne(Filters.and(gxidBson, bxidBson, crteBson),
					new Document("$set", variables));

			if (result.getModifiedCount() != 1) {
				throw new IllegalStateException(
						String.format("Error occurred while updating participant(matched= %s, modified= %s).",
								result.getMatchedCount(), result.getModifiedCount()));
			}

		} catch (RuntimeException rex) {
			logger.error("Error occurred while updating participant.", rex);
		}
	}

	public void deleteParticipant(XAResourceArchive archive) {
	}

	public void recover(TransactionRecoveryCallback callback) {
		XidFactory xidFactory = this.beanFactory.getXidFactory();

		Map<Xid, TransactionArchive> archiveMap = new HashMap<Xid, TransactionArchive>();
		try {
			MongoDatabase mdb = this.mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection(CONSTANTS_TB_TRANSACTIONS);
			MongoCollection<Document> participants = mdb.getCollection(CONSTANTS_TB_PARTICIPANTS);

			String[] values = this.endpoint.split(":");
			String application = values[1];
			Bson filter = Filters.eq("application", application);

			FindIterable<Document> transactionItr = transactions.find(filter);
			MongoCursor<Document> transactionCursor = transactionItr.iterator();
			for (; transactionCursor.hasNext();) {
				Document document = transactionCursor.next();
				TransactionArchive archive = new TransactionArchive();

				String gxid = document.getString("gxid");
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

				String gxid = document.getString("gxid");
				String bxid = document.getString("bxid");

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

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
