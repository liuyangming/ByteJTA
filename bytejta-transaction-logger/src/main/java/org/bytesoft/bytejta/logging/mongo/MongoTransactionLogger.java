/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.logging.mongo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.Xid;

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
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.supports.serialize.XAResourceDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoTransactionLogger implements TransactionLogger, TransactionEndpointAware, TransactionBeanFactoryAware {
	static Logger logger = LoggerFactory.getLogger(MongoTransactionLogger.class);
	static final String CONSTANTS_DB_NAME = "test";

	private String endpoint;
	private TransactionBeanFactory beanFactory;

	public void createTransaction(TransactionArchive archive) {
		MongoClient mongoClient = MongoClientRegistry.getInstance().getMongoClient();
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();
			boolean coordinator = archive.isCoordinator();
			Object propagatedBy = archive.getPropagatedBy();
			int vote = archive.getVote();
			int status = archive.getStatus();
			// boolean propagated = archive.isPropagated();

			MongoDatabase mdb = mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> collection = mdb.getCollection("transaction");

			String[] values = this.endpoint.split(":");
			String application = values[1];

			Document document = new Document();
			byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
			document.put("gxid", ByteUtils.byteArrayToString(globalTransactionId));
			document.put("application", application);
			document.put("created", this.endpoint);
			document.put("modified", this.endpoint);
			document.put("propagatedBy", propagatedBy);
			document.put("coordinator", coordinator);
			document.put("status", status);
			document.put("vote", vote);
			document.put("version", 0L);

			collection.insertOne(document);

			List<XAResourceArchive> nativeResourceList = archive.getNativeResources();
			for (int i = 0; nativeResourceList != null && i < nativeResourceList.size(); i++) {
				XAResourceArchive resourceArchive = nativeResourceList.get(i);
				this.createResource(mongoClient, resourceArchive);
			}

			List<XAResourceArchive> remoteResourceList = archive.getRemoteResources();
			for (int i = 0; remoteResourceList != null && i < remoteResourceList.size(); i++) {
				XAResourceArchive resourceArchive = remoteResourceList.get(i);
				this.createResource(mongoClient, resourceArchive);
			}

		} catch (RuntimeException rex) {
			logger.error("Error occurred while creating transaction.", rex);
		}
	}

	private void createResource(MongoClient mongoClient, XAResourceArchive archive) {
		TransactionXid transactionXid = (TransactionXid) archive.getXid();

		MongoDatabase mdb = mongoClient.getDatabase(CONSTANTS_DB_NAME);
		MongoCollection<Document> collection = mdb.getCollection("participant");

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

	public void updateTransaction(TransactionArchive archive) {
		MongoClient mongoClient = MongoClientRegistry.getInstance().getMongoClient();
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();
			int vote = archive.getVote();
			int status = archive.getStatus();

			MongoDatabase mdb = mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> collection = mdb.getCollection("transaction");

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
		} catch (RuntimeException rex) {
			logger.error("Error occurred while updating transaction.", rex);
		}
	}

	public void deleteTransaction(TransactionArchive archive) {
		MongoClient mongoClient = MongoClientRegistry.getInstance().getMongoClient();
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();

			MongoDatabase mdb = mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection("transaction");
			MongoCollection<Document> participants = mdb.getCollection("participant");

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

	public void updateResource(XAResourceArchive archive) {
		MongoClient mongoClient = MongoClientRegistry.getInstance().getMongoClient();
		try {
			TransactionXid transactionXid = (TransactionXid) archive.getXid();

			MongoDatabase mdb = mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> collection = mdb.getCollection("participant");

			byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
			byte[] branchQualifier = transactionXid.getBranchQualifier();

			int branchVote = archive.getVote();
			boolean readonly = archive.isReadonly();
			boolean committed = archive.isCommitted();
			boolean rolledback = archive.isRolledback();
			boolean completed = archive.isCompleted();
			boolean heuristic = archive.isHeuristic();

			Document variables = new Document();
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

	public void recover(TransactionRecoveryCallback callback) {
		MongoClient mongoClient = MongoClientRegistry.getInstance().getMongoClient();

		XidFactory xidFactory = this.beanFactory.getXidFactory();

		Map<Xid, TransactionArchive> archiveMap = new HashMap<Xid, TransactionArchive>();
		try {
			MongoDatabase mdb = mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection("transaction");
			MongoCollection<Document> participants = mdb.getCollection("participant");

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

				String propagatedBy = document.getString("propagatedBy");
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

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
