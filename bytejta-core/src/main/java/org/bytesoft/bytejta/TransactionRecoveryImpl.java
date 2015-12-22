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

import javax.transaction.HeuristicMixedException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.common.TransactionBeanFactory;
import org.bytesoft.bytejta.common.TransactionRepository;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.logger.TransactionLogger;
import org.bytesoft.transaction.recovery.TransactionRecovery;
import org.bytesoft.transaction.xa.AbstractXid;
import org.bytesoft.transaction.xa.XATerminator;

public class TransactionRecoveryImpl implements TransactionRecovery {

	private boolean initialized;

	public synchronized void timingRecover() {
		TransactionBeanFactory beanFactory = TransactionBeanFactory.getInstance();
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		List<TransactionImpl> transactions = transactionRepository.getErrorTransactionList();
		for (int i = 0; i < transactions.size(); i++) {
			TransactionImpl transaction = transactions.get(i);
			this.recoverTransaction(transaction);
		}
	}

	public synchronized void forgetTransaction(TransactionImpl transaction) {
		// TODO
	}

	public synchronized void recoverTransaction(TransactionImpl transaction) {

		TransactionBeanFactory beanFactory = TransactionBeanFactory.getInstance();
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();

		TransactionContext transactionContext = transaction.getTransactionContext();
		boolean coordinator = transactionContext.isCoordinator();
		if (coordinator) {
			int status = transaction.getStatus();
			switch (status) {
			case Status.STATUS_ACTIVE:
			case Status.STATUS_MARKED_ROLLBACK:
			case Status.STATUS_PREPARING:
			case Status.STATUS_ROLLING_BACK:
			case Status.STATUS_UNKNOWN:
				try {
					transaction.recoveryRollback();
					AbstractXid globalXid = transactionContext.getXid();
					transactionRepository.removeErrorTransaction(globalXid);
					transactionRepository.removeTransaction(globalXid);

					this.deleteRecoveryTransaction(transaction);
				} catch (RollbackRequiredException ex) {
					// TODO
				} catch (SystemException ex) {
					// TODO
				}
				break;
			case Status.STATUS_PREPARED:
			case Status.STATUS_COMMITTING:
				try {
					transaction.recoveryCommit();
					AbstractXid globalXid = transactionContext.getXid();
					transactionRepository.removeErrorTransaction(globalXid);
					transactionRepository.removeTransaction(globalXid);

					this.deleteRecoveryTransaction(transaction);
				} catch (HeuristicMixedException ex) {
					// TODO
				} catch (CommitRequiredException ex) {
					// TODO
				} catch (SystemException ex) {
					// TODO
				}
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
		TransactionBeanFactory beanFactory = TransactionBeanFactory.getInstance();
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
			AbstractXid globalXid = transactionContext.getXid();
			transactionRepository.putTransaction(globalXid, transaction);
			transactionRepository.putErrorTransaction(globalXid, transaction);
		}
	}

	private TransactionImpl reconstructTransaction(TransactionArchive archive) throws IllegalStateException {
		TransactionContext transactionContext = new TransactionContext(/* TODO nonxaResourceAllowed */);
		transactionContext.setXid((AbstractXid) archive.getXid());
		transactionContext.setRecoveried(true);
		// transactionContext.setOptimized(archive.isOptimized());
		transactionContext.setCoordinator(archive.isCoordinator());

		TransactionImpl transaction = new TransactionImpl(transactionContext);
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

	private void deleteRecoveryTransaction(TransactionImpl transaction) {

		TransactionArchive archive = transaction.getTransactionArchive();
		TransactionBeanFactory beanFactory = TransactionBeanFactory.getInstance();
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		transactionLogger.deleteTransaction(archive);

		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		AbstractXid globalXid = transaction.getTransactionContext().getXid();
		transactionRepository.removeTransaction(globalXid);
		transactionRepository.removeErrorTransaction(globalXid);

	}
}
