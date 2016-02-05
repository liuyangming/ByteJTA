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

import org.bytesoft.bytejta.aware.TransactionBeanFactoryAware;
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

	private boolean initialized;
	private TransactionBeanFactory beanFactory;

	public synchronized void timingRecover() {
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		List<Transaction> transactions = transactionRepository.getErrorTransactionList();
		for (int i = 0; i < transactions.size(); i++) {
			Transaction transaction = transactions.get(i);
			try {
				this.recoverTransaction(transaction);
				transaction.forgetQuietly();
			} catch (CommitRequiredException ex) {
				continue;
			} catch (RollbackRequiredException ex) {
				continue;
			} catch (SystemException ex) {
				continue;
			} catch (RuntimeException ex) {
				continue;
			}
		}
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
				this.deleteRecoveryTransaction(transaction);
				break;
			case Status.STATUS_PREPARED:
			case Status.STATUS_COMMITTING:
				transaction.recoveryCommit();
				this.deleteRecoveryTransaction(transaction);
				break;
			case Status.STATUS_COMMITTED:
			case Status.STATUS_ROLLEDBACK:
			default:
				// ignore
			}
		}// end-if (coordinator)

	}

	public synchronized void startupRecover(boolean recoverImmediately) {
		this.fireInitializationIfNecessary();
		if (recoverImmediately) {
			this.timingRecover();
		}
	}

	private void fireInitializationIfNecessary() {
		if (this.initialized == false) {
			this.processStartupRecover();
			this.initialized = true;
		}
	}

	private synchronized void processStartupRecover() {
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		List<TransactionArchive> archives = transactionLogger.getTransactionArchiveList();
		for (int i = 0; i < archives.size(); i++) {
			TransactionArchive archive = archives.get(i);
			TransactionImpl transaction = null;
			try {
				transaction = this.reconstructTransaction(archive);
			} catch (IllegalStateException ex) {
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
			throw new IllegalStateException();
		}

		return transaction;
	}

	private void deleteRecoveryTransaction(Transaction transaction) {

		TransactionArchive archive = transaction.getTransactionArchive();
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		transactionLogger.deleteTransaction(archive);

		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		TransactionXid globalXid = transaction.getTransactionContext().getXid();
		transactionRepository.removeTransaction(globalXid);
		transactionRepository.removeErrorTransaction(globalXid);

	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}
}
