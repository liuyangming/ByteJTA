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
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionException;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.remote.RemoteNode;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionCoordinator implements RemoteCoordinator, TransactionBeanFactoryAware, TransactionEndpointAware {
	static final Logger logger = LoggerFactory.getLogger(TransactionCoordinator.class);

	@javax.inject.Inject
	private TransactionBeanFactory beanFactory;
	private String endpoint;

	private transient boolean ready = false;
	private final Lock lock = new ReentrantLock();

	public Transaction getTransactionQuietly() {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		return transactionManager.getTransactionQuietly();
	}

	public Transaction start(TransactionContext transactionContext, int flags) throws XAException {

		TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		if (transactionManager.getTransactionQuietly() != null) {
			throw new XAException(XAException.XAER_PROTO);
		}

		TransactionXid globalXid = (TransactionXid) transactionContext.getXid();
		Transaction transaction = null;
		try {
			transaction = transactionRepository.getTransaction(globalXid);
		} catch (TransactionException tex) {
			throw new XAException(XAException.XAER_RMERR);
		}

		if (transaction == null) {
			transaction = new TransactionImpl(transactionContext);
			((TransactionImpl) transaction).setBeanFactory(this.beanFactory);

			long expired = transactionContext.getExpiredTime();
			long current = System.currentTimeMillis();
			long timeoutMillis = (expired - current) / 1000L;
			transaction.setTransactionTimeout((int) timeoutMillis);

			transactionRepository.putTransaction(globalXid, transaction);
			logger.info("{}> begin-participant", ByteUtils.byteArrayToString(globalXid.getGlobalTransactionId()));
		}

		transactionManager.associateThread(transaction);
		// this.transactionStatistic.fireBeginTransaction(transaction);

		return transaction;
	}

	public Transaction end(TransactionContext transactionContext, int flags) throws XAException {
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		return transactionManager.desociateThread();
	}

	/** supports resume only, for tcc transaction manager. */
	public void start(Xid xid, int flags) throws XAException {
		if (XAResource.TMRESUME != flags) {
			throw new XAException(XAException.XAER_INVAL);
		}
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		XidFactory xidFactory = this.beanFactory.getXidFactory();
		Transaction current = transactionManager.getTransactionQuietly();
		if (current != null) {
			throw new XAException(XAException.XAER_PROTO);
		}

		TransactionRepository transactionRepository = this.beanFactory.getTransactionRepository();

		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = xidFactory.createGlobalXid(branchXid.getGlobalTransactionId());

		Transaction transaction = null;
		try {
			transaction = transactionRepository.getTransaction(globalXid);
		} catch (TransactionException tex) {
			throw new XAException(XAException.XAER_RMERR);
		}

		if (transaction == null) {
			throw new XAException(XAException.XAER_NOTA);
		}
		transactionManager.associateThread(transaction);
	}

	/** supports suspend only, for tcc transaction manager. */
	public void end(Xid xid, int flags) throws XAException {
		if (XAResource.TMSUSPEND != flags) {
			throw new XAException(XAException.XAER_INVAL);
		}
		TransactionManager transactionManager = this.beanFactory.getTransactionManager();
		XidFactory xidFactory = this.beanFactory.getXidFactory();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction == null) {
			throw new XAException(XAException.XAER_NOTA);
		}
		TransactionContext transactionContext = transaction.getTransactionContext();
		TransactionXid transactionXid = transactionContext.getXid();

		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = xidFactory.createGlobalXid(branchXid.getGlobalTransactionId());

		if (CommonUtils.equals(globalXid, transactionXid) == false) {
			throw new XAException(XAException.XAER_INVAL);
		}
		transactionManager.desociateThread();
	}

	public void commit(Xid xid, boolean onePhaseCommit) throws XAException {
		this.checkParticipantReadyIfNecessary();

		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = xidFactory.createGlobalXid(branchXid.getGlobalTransactionId());
		TransactionRepository repository = beanFactory.getTransactionRepository();
		Transaction transaction = null;
		try {
			transaction = repository.getTransaction(globalXid);
		} catch (TransactionException tex) {
			throw new XAException(XAException.XAER_RMERR);
		}

		if (transaction == null) {
			throw new XAException(XAException.XAER_NOTA);
		}

		if (onePhaseCommit) {
			try {
				this.beanFactory.getTransactionManager().associateThread(transaction);
				transaction.fireBeforeTransactionCompletion();
				this.beanFactory.getTransactionTimer().stopTiming(transaction);
			} catch (RollbackRequiredException rrex) {
				this.rollback(xid);
				XAException xaex = new XAException(XAException.XA_HEURRB);
				xaex.initCause(rrex);
				throw xaex;
			} catch (SystemException ex) {
				this.rollback(xid);
				XAException xaex = new XAException(XAException.XA_HEURRB);
				xaex.initCause(ex);
				throw xaex;
			} catch (RuntimeException rex) {
				this.rollback(xid);
				XAException xaex = new XAException(XAException.XA_HEURRB);
				xaex.initCause(rex);
				throw xaex;
			} finally {
				this.beanFactory.getTransactionManager().desociateThread();
			}
		} // end-if (onePhaseCommit)

		try {
			transaction.participantCommit(onePhaseCommit);
			transaction.forgetQuietly(); // forget transaction
		} catch (SecurityException ex) {
			logger.error("{}> Error occurred while committing remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XAER_RMERR);
			xaex.initCause(ex);
			throw xaex;
		} catch (CommitRequiredException ex) {
			logger.error("{}> Error occurred while committing remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XAER_RMERR);
			xaex.initCause(ex);
			throw xaex;
		} catch (RollbackException ex) {
			logger.error("{}> Error occurred while committing remote coordinator, tx has been rolled back.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);

			// don't forget if branch-transaction has been hueristic completed.
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XA_HEURRB);
			xaex.initCause(ex);
			throw xaex;
		} catch (HeuristicMixedException ex) {
			logger.error("{}> Error occurred while committing remote coordinator, tx has been completed mixed.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);

			// don't forget if branch-transaction has been hueristic completed.
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XA_HEURMIX);
			xaex.initCause(ex);
			throw xaex;
		} catch (HeuristicRollbackException ex) {
			logger.error("{}> Error occurred while committing remote coordinator, tx has been rolled back heuristically.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);

			// don't forget if branch-transaction has been hueristic completed.
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XA_HEURRB);
			xaex.initCause(ex);
			throw xaex;
		} catch (SystemException ex) {
			logger.error("{}> Error occurred while committing remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XAER_RMERR);
			xaex.initCause(ex);
			throw xaex;
		} catch (RuntimeException ex) {
			logger.error("{}> Error occurred while committing remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XAER_RMERR);
			xaex.initCause(ex);
			throw xaex;
		} finally {
			transaction.fireAfterTransactionCompletion();
		}
	}

	public void forgetQuietly(Xid xid) {
		try {
			this.forget(xid);
		} catch (XAException ex) {
			switch (ex.errorCode) {
			case XAException.XAER_NOTA:
				break;
			default:
				logger.error("{}> Error occurred while forgeting remote coordinator.",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);
			}
		} catch (RuntimeException ex) {
			logger.error("{}> Error occurred while forgeting remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);
		}
	}

	public void forget(Xid xid) throws XAException {
		this.checkParticipantReadyIfNecessary();

		if (xid == null) {
			throw new XAException(XAException.XAER_INVAL);
		}

		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = xidFactory.createGlobalXid(branchXid.getGlobalTransactionId());
		TransactionRepository transactionRepository = beanFactory.getTransactionRepository();
		Transaction transaction = null;
		try {
			transaction = transactionRepository.getErrorTransaction(globalXid);
		} catch (TransactionException tex) {
			throw new XAException(XAException.XAER_RMERR);
		}

		if (transaction == null) {
			throw new XAException(XAException.XAER_NOTA);
		}

		try {
			transaction.forget();
		} catch (SystemException ex) {
			logger.error("{}> Error occurred while forgeting remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);
			throw new XAException(XAException.XAER_RMERR);
		} catch (RuntimeException rex) {
			logger.error("{}> Error occurred while forgeting remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), rex);
			throw new XAException(XAException.XAER_RMERR);
		}
	}

	public int getTransactionTimeout() throws XAException {
		return 0;
	}

	public boolean isSameRM(XAResource xares) throws XAException {
		throw new XAException(XAException.XAER_RMERR);
	}

	public int prepare(Xid xid) throws XAException {
		this.checkParticipantReadyIfNecessary();

		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = xidFactory.createGlobalXid(branchXid.getGlobalTransactionId());
		TransactionRepository repository = beanFactory.getTransactionRepository();
		Transaction transaction = null;
		try {
			transaction = repository.getTransaction(globalXid);
		} catch (TransactionException tex) {
			throw new XAException(XAException.XAER_RMERR);
		}

		if (transaction == null) {
			throw new XAException(XAException.XAER_NOTA);
		}

		try {
			this.beanFactory.getTransactionManager().associateThread(transaction);
			transaction.fireBeforeTransactionCompletion();
			this.beanFactory.getTransactionTimer().stopTiming(transaction);
		} catch (RollbackRequiredException rrex) {
			throw new XAException(XAException.XAER_RMERR);
		} catch (SystemException ex) {
			throw new XAException(XAException.XAER_RMERR);
		} catch (RuntimeException rex) {
			throw new XAException(XAException.XAER_RMERR);
		} finally {
			this.beanFactory.getTransactionManager().desociateThread();
		}

		int participantVote = XAResource.XA_OK;
		try {
			participantVote = transaction.participantPrepare();
		} catch (CommitRequiredException crex) {
			participantVote = XAResource.XA_OK;
		} catch (RollbackRequiredException rrex) {
			throw new XAException(XAException.XAER_RMERR);
		} finally {
			if (participantVote == XAResource.XA_RDONLY) {
				transaction.fireAfterTransactionCompletion();
			} // end-if (participantVote == XAResource.XA_RDONLY)
		}

		return participantVote;
	}

	public Xid[] recover(int flag) throws XAException {
		this.checkParticipantReadyIfNecessary();

		TransactionRepository repository = beanFactory.getTransactionRepository();
		List<Transaction> allTransactionList = repository.getActiveTransactionList();

		List<Transaction> transactions = new ArrayList<Transaction>();
		for (int i = 0; i < allTransactionList.size(); i++) {
			Transaction transaction = allTransactionList.get(i);
			int transactionStatus = transaction.getTransactionStatus();
			if (transactionStatus == Status.STATUS_PREPARED || transactionStatus == Status.STATUS_COMMITTING
					|| transactionStatus == Status.STATUS_ROLLING_BACK || transactionStatus == Status.STATUS_COMMITTED
					|| transactionStatus == Status.STATUS_ROLLEDBACK) {
				transactions.add(transaction);
			} else if (transaction.getTransactionContext().isRecoveried()) {
				transactions.add(transaction);
			}
		}

		TransactionXid[] xidArray = new TransactionXid[transactions.size()];
		for (int i = 0; i < transactions.size(); i++) {
			Transaction transaction = transactions.get(i);
			xidArray[i] = transaction.getTransactionContext().getXid();
		}

		return xidArray;
	}

	public void rollback(Xid xid) throws XAException {
		this.checkParticipantReadyIfNecessary();

		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = xidFactory.createGlobalXid(branchXid.getGlobalTransactionId());
		TransactionRepository repository = beanFactory.getTransactionRepository();
		Transaction transaction = null;
		try {
			transaction = repository.getTransaction(globalXid);
		} catch (TransactionException tex) {
			throw new XAException(XAException.XAER_RMERR);
		}

		if (transaction == null) {
			throw new XAException(XAException.XAER_NOTA);
		}

		try {
			this.beanFactory.getTransactionManager().associateThread(transaction);
			transaction.fireBeforeTransactionCompletionQuietly();
			this.beanFactory.getTransactionManager().desociateThread();

			this.beanFactory.getTransactionTimer().stopTiming(transaction);

			transaction.participantRollback();
			transaction.forgetQuietly(); // forget transaction
		} catch (RollbackRequiredException rrex) {
			logger.error("{}> Error occurred while rolling back remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), rrex);
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XAER_RMERR);
			xaex.initCause(rrex);
			throw xaex;
		} catch (SystemException ex) {
			logger.error("{}> Error occurred while rolling back remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), ex);
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XAER_RMERR);
			xaex.initCause(ex);
			throw xaex;
		} catch (RuntimeException rrex) {
			logger.error("{}> Error occurred while rolling back remote coordinator.",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()), rrex);
			repository.putErrorTransaction(globalXid, transaction);

			XAException xaex = new XAException(XAException.XAER_RMERR);
			xaex.initCause(rrex);
			throw xaex;
		} finally {
			transaction.fireAfterTransactionCompletion();
		}
	}

	public void markParticipantReady() {
		try {
			this.lock.lock();
			this.ready = true;
		} finally {
			this.lock.unlock();
		}
	}

	private void checkParticipantReadyIfNecessary() throws XAException {
		if (this.ready == false) {
			this.checkParticipantReady();
		}
	}

	private void checkParticipantReady() throws XAException {
		try {
			this.lock.lock();
			if (this.ready == false) {
				throw new XAException(XAException.XAER_RMFAIL);
			}
		} finally {
			this.lock.unlock();
		}
	}

	public boolean setTransactionTimeout(int seconds) throws XAException {
		return false;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

	public RemoteAddr getRemoteAddr() {
		return CommonUtils.getRemoteAddr(this.endpoint);
	}

	public RemoteNode getRemoteNode() {
		return CommonUtils.getRemoteNode(this.endpoint);
	}

	public String getIdentifier() {
		return this.endpoint;
	}

	public String getApplication() {
		return CommonUtils.getApplication(this.endpoint);
	}

	public TransactionBeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
