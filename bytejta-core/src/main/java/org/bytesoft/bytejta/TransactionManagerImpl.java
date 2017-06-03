/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.supports.TransactionTimer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionManagerImpl implements TransactionManager, TransactionTimer, TransactionBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionManagerImpl.class);

	private TransactionBeanFactory beanFactory;
	private int timeoutSeconds = 5 * 60;
	private final Map<Thread, Transaction> associatedTxMap = new ConcurrentHashMap<Thread, Transaction>();

	public void begin() throws NotSupportedException, SystemException {
		if (this.getTransaction() != null) {
			throw new NotSupportedException();
		}

		XidFactory xidFactory = this.beanFactory.getXidFactory();
		RemoteCoordinator transactionCoordinator = this.beanFactory.getTransactionCoordinator();

		int timeoutSeconds = this.timeoutSeconds;

		TransactionContext transactionContext = new TransactionContext();
		transactionContext.setPropagatedBy(transactionCoordinator.getIdentifier());
		transactionContext.setCoordinator(true);
		long createdTime = System.currentTimeMillis();
		long expiredTime = createdTime + (timeoutSeconds * 1000L);
		transactionContext.setCreatedTime(createdTime);
		transactionContext.setExpiredTime(expiredTime);

		TransactionXid globalXid = xidFactory.createGlobalXid();
		transactionContext.setXid(globalXid);

		TransactionImpl transaction = new TransactionImpl(transactionContext);
		transaction.setBeanFactory(this.beanFactory);
		transaction.setTransactionTimeout(this.timeoutSeconds);

		this.associateThread(transaction);
		TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();
		transactionRepository.putTransaction(globalXid, transaction);
		// this.transactionStatistic.fireBeginTransaction(transaction);

		logger.info("[{}] begin-transaction", ByteUtils.byteArrayToString(globalXid.getGlobalTransactionId()));
	}

	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException,
			IllegalStateException, SystemException {
		Transaction transaction = this.desociateThread();
		if (transaction == null) {
			throw new IllegalStateException();
		} else if (transaction.getTransactionStatus() == Status.STATUS_ROLLEDBACK) {
			throw new RollbackException();
		} else if (transaction.getTransactionStatus() == Status.STATUS_COMMITTED) {
			return;
		} else if (transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK) {
			this.rollback(transaction);
			throw new HeuristicRollbackException();
		} else if (transaction.getTransactionStatus() != Status.STATUS_ACTIVE) {
			throw new IllegalStateException();
		}

		TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();
		TransactionContext transactionContext = transaction.getTransactionContext();
		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transaction.commit();
			transaction.forgetQuietly(); // forget transaction
		} catch (IllegalStateException ex) {
			logger.error("Error occurred while committing transaction.", ex);
			transactionRepository.putErrorTransaction(transactionXid, transaction);
			throw ex;
		} catch (SecurityException ex) {
			logger.error("Error occurred while committing transaction.", ex);
			transactionRepository.putErrorTransaction(transactionXid, transaction);
			throw ex;
		} catch (RollbackException rex) {
			logger.error("Error occurred while committing transaction.", rex);
			transaction.forgetQuietly(); // forget transaction
			throw rex;
		} catch (HeuristicMixedException hmex) {
			logger.error("Error occurred while committing transaction.", hmex);
			transaction.forgetQuietly(); // forget transaction
			throw hmex;
		} catch (HeuristicRollbackException hrex) {
			logger.error("Error occurred while committing transaction.", hrex);
			transaction.forgetQuietly(); // forget transaction
			throw hrex;
		} catch (SystemException ex) {
			logger.error("Error occurred while committing transaction.", ex);
			transactionRepository.putErrorTransaction(transactionXid, transaction);
			throw ex;
		} catch (RuntimeException rex) {
			logger.error("Error occurred while committing transaction.", rex);
			transactionRepository.putErrorTransaction(transactionXid, transaction);
			throw rex;
		}
	}

	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		Transaction transaction = this.desociateThread();
		this.rollback(transaction);
	}

	protected void rollback(Transaction transaction) throws IllegalStateException, SecurityException, SystemException {
		if (transaction == null) {
			throw new IllegalStateException();
		} else if (transaction.getTransactionStatus() == Status.STATUS_ROLLEDBACK) {
			return;
		} else if (transaction.getTransactionStatus() == Status.STATUS_COMMITTED) {
			throw new SystemException();
		}

		TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();
		TransactionContext transactionContext = transaction.getTransactionContext();
		TransactionXid transactionXid = transactionContext.getXid();
		try {
			transaction.rollback();
			transaction.forgetQuietly();
		} catch (IllegalStateException ex) {
			logger.error("Error occurred while rolling back transaction.", ex);
			transactionRepository.putErrorTransaction(transactionXid, transaction);
			throw ex;
		} catch (SecurityException ex) {
			logger.error("Error occurred while rolling back transaction.", ex);
			transactionRepository.putErrorTransaction(transactionXid, transaction);
			throw ex;
		} catch (SystemException ex) {
			logger.error("Error occurred while rolling back transaction.", ex);
			transactionRepository.putErrorTransaction(transactionXid, transaction);
			throw ex;
		} catch (RuntimeException ex) {
			logger.error("Error occurred while rolling back transaction.", ex);
			transactionRepository.putErrorTransaction(transactionXid, transaction);
			throw ex;
		}
	}

	public void associateThread(Transaction transaction) {
		this.associatedTxMap.put(Thread.currentThread(), transaction);
	}

	public Transaction desociateThread() {
		return this.associatedTxMap.remove(Thread.currentThread());
	}

	public Transaction suspend() throws RollbackRequiredException, SystemException {
		Transaction transaction = this.desociateThread();
		transaction.suspend();
		return transaction;
	}

	public void resume(javax.transaction.Transaction tobj)
			throws InvalidTransactionException, IllegalStateException, RollbackRequiredException, SystemException {

		if (TransactionImpl.class.isInstance(tobj) == false) {
			throw new InvalidTransactionException();
		} else if (this.getTransaction() != null) {
			throw new IllegalStateException();
		}

		TransactionImpl transaction = (TransactionImpl) tobj;
		transaction.resume();
		this.associateThread(transaction);

	}

	public int getStatus() throws SystemException {
		Transaction transaction = this.getTransaction();
		return transaction == null ? Status.STATUS_NO_TRANSACTION : transaction.getTransactionStatus();
	}

	public Transaction getTransactionQuietly() {
		try {
			return this.getTransaction();
		} catch (SystemException ex) {
			return null;
		} catch (RuntimeException ex) {
			return null;
		}
	}

	public Transaction getTransaction() throws SystemException {
		return this.associatedTxMap.get(Thread.currentThread());
	}

	public void setRollbackOnly() throws IllegalStateException, SystemException {
		Transaction transaction = this.getTransaction();
		if (transaction == null) {
			throw new SystemException();
		}
		transaction.setRollbackOnly();
	}

	public void setTransactionTimeout(int seconds) throws SystemException {
		Transaction transaction = this.getTransaction();
		if (transaction == null) {
			throw new SystemException();
		} else if (seconds < 0) {
			throw new SystemException();
		} else if (seconds == 0) {
			// ignore
		} else {
			((TransactionImpl) transaction).changeTransactionTimeout(seconds * 1000);
		}
	}

	public void timingExecution() {
		List<Transaction> expiredTransactions = new ArrayList<Transaction>();
		List<Transaction> activeTransactions = new ArrayList<Transaction>(this.associatedTxMap.values());
		long current = System.currentTimeMillis();
		Iterator<Transaction> activeItr = activeTransactions.iterator();
		while (activeItr.hasNext()) {
			Transaction transaction = activeItr.next();
			if (transaction.isTiming()) {
				TransactionContext transactionContext = transaction.getTransactionContext();
				if (transactionContext.getExpiredTime() <= current) {
					expiredTransactions.add(transaction);
				}
			} // end-if (transaction.isTiming())
		}

		Iterator<Transaction> expiredItr = expiredTransactions.iterator();
		while (activeItr.hasNext()) {
			Transaction transaction = expiredItr.next();
			if (transaction.getTransactionStatus() == Status.STATUS_ACTIVE
					|| transaction.getTransactionStatus() == Status.STATUS_MARKED_ROLLBACK) {
				this.timingRollback(transaction);
			}
		}

	}

	private void timingRollback(Transaction transaction) {
		TransactionContext transactionContext = transaction.getTransactionContext();
		TransactionXid globalXid = transactionContext.getXid();
		TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();

		try {
			transaction.rollback();
			transaction.forgetQuietly(); // forget transaction
		} catch (Exception ex) {
			transactionRepository.putErrorTransaction(globalXid, transaction);
		}

	}

	public void stopTiming(Transaction transaction) {
		if (TransactionImpl.class.isInstance(transaction)) {
			((TransactionImpl) transaction).stopTiming();
		}
	}

	public int getTimeoutSeconds() {
		return timeoutSeconds;
	}

	public void setTimeoutSeconds(int timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
