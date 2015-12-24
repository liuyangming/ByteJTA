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
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.bytesoft.bytejta.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.supports.TransactionTimer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;

public class TransactionManagerImpl implements TransactionManager, TransactionTimer, TransactionBeanFactoryAware {

	private TransactionBeanFactory beanFactory;
	private int timeoutSeconds = 5 * 60;
	private final Map<Thread, TransactionImpl> associateds = new ConcurrentHashMap<Thread, TransactionImpl>();

	public void begin() throws NotSupportedException, SystemException {
		if (this.getTransaction() != null) {
			throw new NotSupportedException();
		}

		int timeoutSeconds = this.timeoutSeconds;

		TransactionContext transactionContext = new TransactionContext();
		transactionContext.setCoordinator(true);
		long createdTime = System.currentTimeMillis();
		long expiredTime = createdTime + (timeoutSeconds * 1000L);
		transactionContext.setCreatedTime(createdTime);
		transactionContext.setExpiredTime(expiredTime);
		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionXid globalXid = xidFactory.createGlobalXid();
		transactionContext.setXid(globalXid); // TODO

		TransactionImpl transaction = new TransactionImpl(transactionContext);
		transaction.setTransactionBeanFactory(this.beanFactory);
		transaction.setTransactionTimeout(this.timeoutSeconds);

		// transaction.setThread(Thread.currentThread());
		this.associateds.put(Thread.currentThread(), transaction);
		TransactionRepository<TransactionImpl> transactionRepository = this.beanFactory.getTransactionRepository();
		transactionRepository.putTransaction(transactionContext.getXid(), transaction);
		// this.transactionStatistic.fireBeginTransaction(transaction);

	}

	public void begin(TransactionContext transactionContext) throws NotSupportedException, SystemException {
		if (this.getTransaction() != null) {
			throw new NotSupportedException();
		}

		TransactionRepository<TransactionImpl> transactionRepository = this.beanFactory.getTransactionRepository();

		TransactionImpl transaction = new TransactionImpl(transactionContext);
		transaction.setTransactionBeanFactory(this.beanFactory);

		long expired = transactionContext.getExpiredTime();
		long current = System.currentTimeMillis();
		long timeoutMillis = (expired - current) / 1000L;
		transaction.setTransactionTimeout((int) timeoutMillis);

		// transaction.setThread(Thread.currentThread());
		this.associateds.put(Thread.currentThread(), transaction);

		transactionRepository.putTransaction(transactionContext.getXid(), transaction);
		// this.transactionStatistic.fireBeginTransaction(transaction);

	}

	public void propagationBegin(TransactionContext transactionContext) throws NotSupportedException, SystemException {

		if (this.getTransaction() != null) {
			throw new NotSupportedException();
		}

		TransactionRepository<TransactionImpl> transactionRepository = this.beanFactory.getTransactionRepository();

		TransactionXid propagationXid = transactionContext.getXid();
		TransactionXid globalXid = propagationXid.getGlobalXid();
		TransactionImpl transaction = transactionRepository.getTransaction(globalXid);
		if (transaction == null) {
			transaction = new TransactionImpl(transactionContext);
			transaction.setTransactionBeanFactory(this.beanFactory);

			long expired = transactionContext.getExpiredTime();
			long current = System.currentTimeMillis();
			long timeoutMillis = (expired - current) / 1000L;
			transaction.setTransactionTimeout((int) timeoutMillis);

			// transaction.setThread(Thread.currentThread());
			transactionRepository.putTransaction(transactionContext.getXid(), transaction);
		}

		this.associateds.put(Thread.currentThread(), transaction);
		// this.transactionStatistic.fireBeginTransaction(transaction);

	}

	public void propagationFinish(TransactionContext transactionContext) throws SystemException {
		// TransactionImpl transaction =
		this.associateds.remove(Thread.currentThread());
		// transaction.setThread(null);
	}

	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, SystemException {
		TransactionImpl transaction = this.associateds.remove(Thread.currentThread());
		if (transaction == null) {
			throw new IllegalStateException();
		} else if (transaction.getStatus() == Status.STATUS_ROLLEDBACK) {
			throw new RollbackException();
		} else if (transaction.getStatus() == Status.STATUS_COMMITTED) {
			return;
		} else if (transaction.getStatus() == Status.STATUS_MARKED_ROLLBACK) {
			this.rollback();
			throw new HeuristicRollbackException();
		} else if (transaction.getStatus() != Status.STATUS_ACTIVE) {
			throw new IllegalStateException();
		}

		TransactionContext transactionContext = transaction.getTransactionContext();
		TransactionXid globalXid = transactionContext.getXid();
		TransactionRepository<TransactionImpl> transactionRepository = this.beanFactory.getTransactionRepository();
		boolean transactionDone = false;
		try {
			transaction.commit();
			transactionDone = true;
		} catch (CommitRequiredException crex) {
			transactionRepository.putErrorTransaction(globalXid, transaction);
			throw crex;
		} catch (HeuristicMixedException hmex) {
			transactionRepository.putErrorTransaction(globalXid, transaction);// TODO
			throw hmex;
		} catch (SystemException ex) {
			transactionRepository.putErrorTransaction(globalXid, transaction);
			throw ex;
		} catch (RuntimeException rrex) {
			transactionRepository.putErrorTransaction(globalXid, transaction);
			SystemException ex = new SystemException();
			ex.initCause(rrex);
			throw ex;
		} finally {
			if (transactionDone) {
				transactionRepository.removeErrorTransaction(globalXid);
				transactionRepository.removeTransaction(globalXid);
			}
		}

	}

	public int getStatus() throws SystemException {
		Transaction transaction = this.getTransaction();
		return transaction == null ? Status.STATUS_NO_TRANSACTION : transaction.getStatus();
	}

	public TransactionImpl getCurrentTransaction() {
		try {
			return this.getTransaction();
		} catch (Exception ex) {
			return null;
		}
	}

	public TransactionImpl getTransaction() throws SystemException {
		return this.associateds.get(Thread.currentThread());
	}

	public void resume(Transaction tobj) throws InvalidTransactionException, IllegalStateException, SystemException {

		if (TransactionImpl.class.isInstance(tobj) == false) {
			throw new InvalidTransactionException();
		} else if (this.getTransaction() != null) {
			throw new IllegalStateException();
		}

		TransactionImpl transaction = (TransactionImpl) tobj;
		// transaction.setThread(Thread.currentThread());
		transaction.resume();
		this.associateds.put(Thread.currentThread(), transaction);

	}

	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		TransactionImpl transaction = this.associateds.remove(Thread.currentThread());
		if (transaction == null) {
			throw new IllegalStateException();
		} else if (transaction.getStatus() == Status.STATUS_ROLLEDBACK) {
			return;
		} else if (transaction.getStatus() == Status.STATUS_COMMITTED) {
			throw new SystemException();
		}

		TransactionContext transactionContext = transaction.getTransactionContext();
		TransactionXid globalXid = transactionContext.getXid();
		TransactionRepository<TransactionImpl> transactionRepository = this.beanFactory.getTransactionRepository();
		boolean transactionDone = false;
		try {
			transaction.rollback();
			transactionDone = true;
		} catch (RollbackRequiredException rrex) {
			transactionRepository.putErrorTransaction(globalXid, transaction);
			throw rrex;
		} catch (SystemException ex) {
			transactionRepository.putErrorTransaction(globalXid, transaction);
			throw ex;
		} catch (RuntimeException rrex) {
			transactionRepository.putErrorTransaction(globalXid, transaction);
			SystemException ex = new SystemException();
			ex.initCause(rrex);
			throw ex;
		} finally {
			if (transactionDone) {
				transactionRepository.removeErrorTransaction(globalXid);
				transactionRepository.removeTransaction(globalXid);
			}
		}

	}

	public void setRollbackOnly() throws IllegalStateException, SystemException {
		TransactionImpl transaction = this.getTransaction();
		if (transaction == null) {
			throw new SystemException();
		}
		transaction.setRollbackOnly();
	}

	public void setTransactionTimeout(int seconds) throws SystemException {
		TransactionImpl transaction = this.getTransaction();
		if (transaction == null) {
			throw new SystemException();
		} else if (seconds < 0) {
			throw new SystemException();
		} else if (seconds == 0) {
			// ignore
		} else {
			synchronized (transaction) {
				TransactionContext transactionContext = transaction.getTransactionContext();
				long createdTime = transactionContext.getCreatedTime();
				transactionContext.setExpiredTime(createdTime + seconds * 1000L);
			}
		}
	}

	public TransactionImpl suspend() throws SystemException {
		TransactionImpl transaction = this.associateds.remove(Thread.currentThread());
		transaction.suspend();
		return transaction;
	}

	public void timingExecution() {
		List<TransactionImpl> expiredTransactions = new ArrayList<TransactionImpl>();
		List<TransactionImpl> activeTransactions = new ArrayList<TransactionImpl>(this.associateds.values());
		long current = System.currentTimeMillis();
		Iterator<TransactionImpl> activeItr = activeTransactions.iterator();
		while (activeItr.hasNext()) {
			TransactionImpl transaction = activeItr.next();
			synchronized (transaction) {
				if (transaction.isTiming()) {
					TransactionContext transactionContext = transaction.getTransactionContext();
					long expired = transactionContext.getExpiredTime();
					if (expired <= current) {
						expiredTransactions.add(transaction);
					}
				}// end-if (transaction.isTiming())
			}// end-synchronized
		}

		Iterator<TransactionImpl> expiredItr = expiredTransactions.iterator();
		while (activeItr.hasNext()) {
			TransactionImpl transaction = expiredItr.next();
			if (transaction.getStatus() == Status.STATUS_ACTIVE
					|| transaction.getStatus() == Status.STATUS_MARKED_ROLLBACK) {
				TransactionContext transactionContext = transaction.getTransactionContext();
				TransactionXid globalXid = transactionContext.getXid();
				TransactionRepository<TransactionImpl> transactionRepository = this.beanFactory
						.getTransactionRepository();
				try {
					transaction.rollback();
					transactionRepository.removeTransaction(globalXid);
				} catch (Exception ex) {
					transactionRepository.putErrorTransaction(globalXid, transaction);
				}
			}// end-else
		}// end-while
	}

	public void stopTiming(Transaction tx) {
		if (TransactionImpl.class.isInstance(tx) == false) {
			return;
		}

		TransactionImpl transaction = (TransactionImpl) tx;
		synchronized (transaction) {
			transaction.setTiming(false);
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
