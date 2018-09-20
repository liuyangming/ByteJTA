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

import java.util.List;

import javax.transaction.SystemException;

import org.bytesoft.bytejta.TransactionCoordinator;
import org.bytesoft.bytejta.TransactionRecoveryImpl;
import org.bytesoft.bytejta.supports.election.AbstractElectionManager;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionException;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.xa.TransactionXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoTransactionRecovery extends TransactionRecoveryImpl {
	static final Logger logger = LoggerFactory.getLogger(MongoTransactionRecovery.class);

	@javax.inject.Inject
	private AbstractElectionManager electionManager;

	public void timingRecover() {
		TransactionBeanFactory beanFactory = this.getBeanFactory();
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		List<Transaction> transactions = null;
		try {
			transactions = transactionRepository.getErrorTransactionList();
		} catch (TransactionException tex) {
			logger.error("Error occurred while recovering transactions!", tex);
			return;
		}

		int total = transactions == null ? 0 : transactions.size(), value = 0;
		for (int i = 0; transactions != null && i < transactions.size(); i++) {
			Transaction transaction = transactions.get(i);
			TransactionContext transactionContext = transaction.getTransactionContext();
			TransactionXid xid = transactionContext.getXid();
			try {
				this.recoverTransaction(transaction);
				value++;
			} catch (CommitRequiredException ex) {
				logger.debug("[{}] recover: branch={}, message= commit-required",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex);
				continue;
			} catch (RollbackRequiredException ex) {
				logger.debug("[{}] recover: branch={}, message= rollback-required",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex);
				continue;
			} catch (SystemException ex) {
				logger.debug("[{}] recover: branch={}, message= {}", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex.getMessage(), ex);
				continue;
			} catch (RuntimeException ex) {
				logger.debug("[{}] recover: branch={}, message= {}", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex.getMessage(), ex);
				continue;
			}
		}
		logger.debug("[transaction-recovery] total= {}, success= {}", total, value);
	}

	public void startRecovery() {
		TransactionBeanFactory beanFactory = this.getBeanFactory();
		TransactionCoordinator transactionCoordinator = //
				(TransactionCoordinator) beanFactory.getNativeParticipant();
		transactionCoordinator.markParticipantReady();
	}

	public Transaction reconstruct(TransactionArchive archive) {
		TransactionBeanFactory beanFactory = this.getBeanFactory();
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		try {
			return super.reconstruct(archive);
		} catch (IllegalStateException ex) {
			transactionLogger.deleteTransaction(archive);
			return null;
		} catch (RuntimeException rex) {
			logger.error("Error occurred while reconstructing transaction from archive!", rex);
			return null;
		}
	}

	public AbstractElectionManager getElectionManager() {
		return electionManager;
	}

	public void setElectionManager(AbstractElectionManager electionManager) {
		this.electionManager = electionManager;
	}

}
