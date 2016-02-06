/**
 * Copyright 2014-2015 yangming.liu<liuyangming@gmail.com>.
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
package org.bytesoft.bytejta;

import java.util.List;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import org.apache.log4j.Logger;
import org.bytesoft.bytejta.aware.TransactionBeanFactoryAware;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionRecovery;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.resource.XATerminator;
import org.bytesoft.transaction.supports.logger.TransactionLogger;
import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionRecoveryImpl implements TransactionRecovery, TransactionBeanFactoryAware {
	static final Logger logger = Logger.getLogger(TransactionRecoveryImpl.class.getSimpleName());

	private TransactionBeanFactory beanFactory;

	public synchronized void timingRecover() {
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		List<Transaction> transactions = transactionRepository.getErrorTransactionList();
		int total = transactions == null ? 0 : transactions.size();
		int value = 0;
		for (int i = 0; transactions != null && i < transactions.size(); i++) {
			Transaction transaction = transactions.get(i);
			TransactionContext transactionContext = transaction.getTransactionContext();
			TransactionXid xid = transactionContext.getXid();
			try {
				this.recoverTransaction(transaction);
				transaction.recoveryForgetQuietly();
			} catch (CommitRequiredException ex) {
				logger.debug(String.format("[%s] recover: branch=%s, message= commit-required",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier())));
				continue;
			} catch (RollbackRequiredException ex) {
				logger.debug(String.format("[%s] recover: branch=%s, message= rollback-required",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier())));
				continue;
			} catch (SystemException ex) {
				logger.debug(String.format("[%s] recover: branch=%s, message= %s",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex.getMessage()));
				continue;
			} catch (RuntimeException ex) {
				logger.debug(String.format("[%s] recover: branch=%s, message= %s",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()), ex.getMessage()));
				continue;
			}
		}
		logger.info(String.format("[transaction-recovery] total= %s, success= %s", total, value));
	}

	public synchronized void recoverTransaction(Transaction transaction) throws CommitRequiredException,
			RollbackRequiredException, SystemException {

		TransactionContext transactionContext = transaction.getTransactionContext();
		boolean coordinator = transactionContext.isCoordinator();
		if (coordinator) {
			int status = transaction.getTransactionStatus();
			switch (status) {
			case Status.STATUS_ACTIVE:
			case Status.STATUS_MARKED_ROLLBACK:
			case Status.STATUS_PREPARING:
			case Status.STATUS_ROLLING_BACK:
			case Status.STATUS_UNKNOWN:
				transaction.recoveryRollback();
				break;
			case Status.STATUS_PREPARED:
			case Status.STATUS_COMMITTING:
				transaction.recoveryCommit();
				break;
			case Status.STATUS_COMMITTED:
			case Status.STATUS_ROLLEDBACK:
			default:
				// ignore
			}
		}// end-if (coordinator)

	}

	public synchronized void startRecovery() {
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		List<TransactionArchive> archives = transactionLogger.getTransactionArchiveList();
		for (int i = 0; i < archives.size(); i++) {
			TransactionArchive archive = archives.get(i);
			TransactionImpl transaction = null;
			try {
				transaction = this.reconstructTransaction(archive);
			} catch (IllegalStateException ex) {
				transactionLogger.deleteTransaction(archive);
				continue;
			}
			TransactionContext transactionContext = transaction.getTransactionContext();
			TransactionXid globalXid = transactionContext.getXid();
			transactionRepository.putTransaction(globalXid, transaction);
			transactionRepository.putErrorTransaction(globalXid, transaction);
		}
	}

	private TransactionImpl reconstructTransaction(TransactionArchive archive) throws IllegalStateException {
		TransactionContext transactionContext = new TransactionContext();
		TransactionXid xid = (TransactionXid) archive.getXid();
		transactionContext.setXid(xid.getGlobalXid());
		transactionContext.setRecoveried(true);
		transactionContext.setCoordinator(archive.isCoordinator());

		TransactionImpl transaction = new TransactionImpl(transactionContext);
		transaction.setBeanFactory(this.beanFactory);
		transaction.setTransactionStatus(archive.getStatus());

		XATerminator nativeTerminator = transaction.getNativeTerminator();
		List<XAResourceArchive> nativeResources = archive.getNativeResources();
		nativeTerminator.getResourceArchives().addAll(nativeResources);

		XATerminator remoteTerminator = transaction.getRemoteTerminator();
		List<XAResourceArchive> remoteResources = archive.getRemoteResources();
		remoteTerminator.getResourceArchives().addAll(remoteResources);

		if (archive.getVote() == XAResource.XA_RDONLY) {
			throw new IllegalStateException("Transaction has already been completed!");
		}

		return transaction;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}
}
