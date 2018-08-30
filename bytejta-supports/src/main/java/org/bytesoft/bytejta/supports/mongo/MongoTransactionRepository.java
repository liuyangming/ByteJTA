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
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
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

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;

public class MongoTransactionRepository
		implements TransactionRepository, TransactionEndpointAware, EnvironmentAware, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(MongoTransactionRepository.class);

	static final String CONSTANTS_DB_NAME = "bytejta";

	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;
	private String endpoint;
	private Environment environment;

	public void putTransaction(TransactionXid transactionXid, Transaction transaction) {
		MongoClient mongoClient = MongoClientRegistry.getInstance().getMongoClient();
		try {
			boolean coordinator = transaction.getTransactionContext().isCoordinator();
			Object propagatedBy = transaction.getTransactionContext().getPropagatedBy();
			// int vote = transaction.getTransactionVote();
			int status = transaction.getTransactionStatus();
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
			// document.put("vote", vote);
			document.put("version", 0L);

			collection.insertOne(document);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while creating transaction.", rex);
		}
	}

	public Transaction getTransaction(TransactionXid xid) {
		MongoClient mongoClient = MongoClientRegistry.getInstance().getMongoClient();

		XidFactory xidFactory = this.beanFactory.getXidFactory();

		try {
			MongoDatabase mdb = mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection("transaction");
			MongoCollection<Document> participants = mdb.getCollection("participant");

			byte[] globalTransactionId = xid.getGlobalTransactionId();
			String gxid = ByteUtils.byteArrayToString(globalTransactionId);

			String[] values = this.endpoint.split(":");
			String application = values[1];
			Bson filter = Filters.and(Filters.eq("application", application), Filters.eq("gxid", gxid));

			FindIterable<Document> transactionItr = transactions.find(filter);
			MongoCursor<Document> transactionCursor = transactionItr.iterator();

			TransactionArchive archive = null;

			if (transactionCursor.hasNext()) {
				Document document = transactionCursor.next();
				archive = new TransactionArchive();

				TransactionXid globalXid = xidFactory.createGlobalXid(globalTransactionId);
				archive.setXid(globalXid);

				String propagatedBy = document.getString("propagatedBy");
				boolean coordinator = document.getBoolean("coordinator");
				int transactionStatus = document.getInteger("status");

				archive.setCoordinator(coordinator);
				archive.setStatus(transactionStatus);
				archive.setPropagatedBy(propagatedBy);
			} else {
				return null;
			}

			FindIterable<Document> participantItr = participants.find(filter);
			MongoCursor<Document> participantCursor = participantItr.iterator();
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

				boolean remote = //
						StringUtils.equalsIgnoreCase(RemoteResourceDescriptor.class.getName(), descriptorType);
				if (remote) {
					archive.getRemoteResources().add(participant);
				} else {
					archive.getNativeResources().add(participant);
				}

			}

			MongoTransactionRecovery transactionRecovery = (MongoTransactionRecovery) this.beanFactory.getTransactionRecovery();
			return transactionRecovery.reconstructTransactionForRecovery(archive);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while loading transaction(xid= {}).", xid, rex);
		} catch (Exception ex) {
			logger.error("Error occurred while loading transaction(xid= {}).", xid, ex);
		}

		return null;
	}

	public Transaction removeTransaction(TransactionXid xid) {
		MongoClient mongoClient = MongoClientRegistry.getInstance().getMongoClient();
		try {
			MongoDatabase mdb = mongoClient.getDatabase(CONSTANTS_DB_NAME);
			MongoCollection<Document> transactions = mdb.getCollection("transaction");
			MongoCollection<Document> participants = mdb.getCollection("participant");

			String[] values = this.endpoint.split(":");
			String application = values[1];

			byte[] globalTransactionId = xid.getGlobalTransactionId();
			Bson xidBson = Filters.eq("gxid", ByteUtils.byteArrayToString(globalTransactionId));
			Bson created = Filters.eq("application", application);

			participants.deleteMany(Filters.and(xidBson, created));

			DeleteResult result = transactions.deleteOne(Filters.and(xidBson, created));
			if (result.getDeletedCount() != 1) {
				throw new IllegalStateException(
						String.format("Error occurred while deleting transaction(deleted= %s).", result.getDeletedCount()));
			}
		} catch (RuntimeException rex) {
			logger.error("Error occurred while deleting transaction(xid= {})!", xid, rex);
		}

		return null;
	}

	public void putErrorTransaction(TransactionXid xid, Transaction transaction) {
	}

	public Transaction getErrorTransaction(TransactionXid xid) {
		return this.getTransaction(xid);
	}

	public Transaction removeErrorTransaction(TransactionXid xid) {
		return this.removeTransaction(xid);
	}

	public List<Transaction> getActiveTransactionList() {
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
			logger.error("Error occurred while loading transactions.", rex);
		} catch (Exception ex) {
			logger.error("Error occurred while loading transactions.", ex);
		}

		MongoTransactionRecovery transactionRecovery = (MongoTransactionRecovery) this.beanFactory.getTransactionRecovery();

		List<Transaction> resultList = new ArrayList<Transaction>();

		Iterator<Map.Entry<Xid, TransactionArchive>> itr = archiveMap.entrySet().iterator();
		while (itr.hasNext()) {
			Map.Entry<Xid, TransactionArchive> entry = itr.next();
			TransactionArchive archive = entry.getValue();

			Transaction transaction = transactionRecovery.reconstructTransactionForRecovery(archive);
			resultList.add(transaction);
		}

		return resultList;
	}

	public List<Transaction> getErrorTransactionList() {
		return this.getActiveTransactionList();
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

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
