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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.transaction.xa.XAException;

import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bytesoft.bytejta.TransactionManagerImpl;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionException;
import org.bytesoft.transaction.TransactionRecovery;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.xa.TransactionXid;
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
	static final String CONSTANTS_TB_TRANSACTIONS = "transactions";
	static final String CONSTANTS_TB_PARTICIPANTS = "participants";
	static final String CONSTANTS_FD_GLOBAL = "gxid";
	static final String CONSTANTS_FD_BRANCH = "bxid";

	@javax.annotation.Resource
	private MongoClient mongoClient;
	private String endpoint;
	private Environment environment;
	@javax.inject.Inject
	private MongoInstanceVersionManager versionManager;
	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;

	public void putTransaction(TransactionXid transactionXid, Transaction transaction) {
	}

	public Transaction getTransaction(TransactionXid xid) throws TransactionException {
		TransactionManagerImpl transactionManager = //
				(TransactionManagerImpl) this.beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransaction(xid);
		if (transaction != null) {
			return transaction;
		}

		return this.getTransactionFromMongoDB(xid);
	}

	private Transaction getTransactionFromMongoDB(TransactionXid xid) throws TransactionException {
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();
		TransactionRecovery transactionRecovery = this.beanFactory.getTransactionRecovery();
		MongoCursor<Document> transactionCursor = null;
		try {
			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			byte[] global = xid.getGlobalTransactionId();
			String globalKey = ByteUtils.byteArrayToString(global);

			FindIterable<Document> transactionItr = transactions.find(Filters.eq(CONSTANTS_FD_GLOBAL, globalKey));
			transactionCursor = transactionItr.iterator();
			transactionCursor = transactionItr.iterator();
			if (transactionCursor.hasNext() == false) {
				return null;
			}

			Document document = transactionCursor.next();

			MongoTransactionLogger mongoTransactionLogger = (MongoTransactionLogger) transactionLogger;
			TransactionArchive archive = mongoTransactionLogger.reconstructTransactionArchive(document);

			return transactionRecovery.reconstruct(archive);
		} catch (RuntimeException rex) {
			logger.error("Error occurred while loading transaction(xid= {}).", xid, rex);
			throw new TransactionException(XAException.XAER_RMERR);
		} catch (Exception ex) {
			logger.error("Error occurred while loading transaction(xid= {}).", xid, ex);
			throw new TransactionException(XAException.XAER_RMERR);
		} finally {
			IOUtils.closeQuietly(transactionCursor);
		}
	}

	public Transaction removeTransaction(TransactionXid transactionXid) {
		return null;
	}

	public void putErrorTransaction(TransactionXid transactionXid, Transaction transaction) {
		try {
			TransactionArchive archive = (TransactionArchive) transaction.getTransactionArchive();
			byte[] global = transactionXid.getGlobalTransactionId();
			String identifier = ByteUtils.byteArrayToString(global);

			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			Document target = new Document();
			target.append("modified", this.endpoint);
			target.append("status", archive.getStatus());
			target.append("error", true);
			target.append("recovered_at", archive.getRecoveredAt() == 0 ? null : new Date(archive.getRecoveredAt()));
			target.append("recovered_times", archive.getRecoveredTimes());

			Document document = new Document();
			document.append("$set", target);

			UpdateResult result = transactions.updateOne(Filters.eq(CONSTANTS_FD_GLOBAL, identifier), document);
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
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();
		TransactionRecovery transactionRecovery = this.beanFactory.getTransactionRecovery();
		MongoCursor<Document> transactionCursor = null;
		try {
			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			byte[] global = xid.getGlobalTransactionId();

			Bson globalFilter = Filters.eq(CONSTANTS_FD_GLOBAL, ByteUtils.byteArrayToString(global));
			Bson errorFilter = Filters.eq("error", true);

			FindIterable<Document> transactionItr = transactions.find(Filters.and(globalFilter, errorFilter));
			transactionCursor = transactionItr.iterator();
			if (transactionCursor.hasNext() == false) {
				return null;
			}

			Document document = transactionCursor.next();

			MongoTransactionLogger mongoTransactionLogger = (MongoTransactionLogger) transactionLogger;
			TransactionArchive archive = mongoTransactionLogger.reconstructTransactionArchive(document);

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
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();
		TransactionRecovery transactionRecovery = this.beanFactory.getTransactionRecovery();

		List<Transaction> transactionList = new ArrayList<Transaction>();
		MongoCursor<Document> transactionCursor = null;
		try {
			String databaseName = CommonUtils.getApplication(this.endpoint).replaceAll("\\W", "_");
			MongoDatabase database = this.mongoClient.getDatabase(databaseName);
			MongoCollection<Document> transactions = database.getCollection(CONSTANTS_TB_TRANSACTIONS);

			FindIterable<Document> transactionItr = transactions.find(Filters.eq("coordinator", true));
			transactionCursor = transactionItr.iterator();
			for (; transactionCursor.hasNext();) {
				Document document = transactionCursor.next();
				boolean error = document.getBoolean("error");

				String targetApplication = document.getString("created");
				long expectVersion = document.getLong("version");
				long actualVersion = this.versionManager.getInstanceVersion(targetApplication);

				if (error == false && actualVersion > 0 && actualVersion <= expectVersion) {
					continue; // ignore
				}

				MongoTransactionLogger mongoTransactionLogger = (MongoTransactionLogger) transactionLogger;
				TransactionArchive archive = mongoTransactionLogger.reconstructTransactionArchive(document);

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

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
