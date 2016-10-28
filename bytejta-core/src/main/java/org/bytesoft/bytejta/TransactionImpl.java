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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.resource.XATerminatorImpl;
import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.UnidentifiedResourceDescriptor;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionRepository;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.internal.SynchronizationList;
import org.bytesoft.transaction.internal.TransactionException;
import org.bytesoft.transaction.internal.TransactionListenerList;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.resource.XATerminator;
import org.bytesoft.transaction.supports.TransactionListener;
import org.bytesoft.transaction.supports.TransactionResourceListener;
import org.bytesoft.transaction.supports.TransactionTimer;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.xa.TransactionXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionImpl implements Transaction {
	static final Logger logger = LoggerFactory.getLogger(TransactionImpl.class.getSimpleName());

	private transient boolean timing = true;
	private TransactionBeanFactory beanFactory;

	private int transactionStatus;
	private int transactionTimeout;
	private int transactionVote;
	private Object transactionalExtra;
	private final TransactionContext transactionContext;
	private final XATerminator nativeTerminator;
	private final XATerminator remoteTerminator;

	private final SynchronizationList synchronizationList = new SynchronizationList();
	private final TransactionListenerList transactionListenerList = new TransactionListenerList();

	public TransactionImpl(TransactionContext txContext) {
		this.transactionContext = txContext;

		this.nativeTerminator = new XATerminatorImpl(this.transactionContext);
		this.remoteTerminator = new XATerminatorImpl(this.transactionContext);

		this.synchronizationList.registerSynchronizationQuietly(this.remoteTerminator);
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
		((XATerminatorImpl) this.nativeTerminator).setBeanFactory(tbf);
		((XATerminatorImpl) this.remoteTerminator).setBeanFactory(tbf);
	}

	public boolean isLocalTransaction() {
		int nativeResNum = this.nativeTerminator.getResourceArchives().size();
		int remoteResNum = this.remoteTerminator.getResourceArchives().size();
		int sizeOfResNum = nativeResNum + remoteResNum;
		return sizeOfResNum <= 1;
	}

	private synchronized void checkBeforeCommit()
			throws RollbackException, IllegalStateException, RollbackRequiredException, CommitRequiredException {

		if (this.transactionStatus == Status.STATUS_ROLLEDBACK) {
			throw new RollbackException();
		} else if (this.transactionStatus == Status.STATUS_ROLLING_BACK) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_ACTIVE) {
			throw new CommitRequiredException();
		} else if (this.transactionStatus == Status.STATUS_COMMITTED) {
			logger.debug("Current transaction has already been committed.");
		} else {
			throw new IllegalStateException();
		}
	}

	public synchronized void participantPrepare() throws RollbackRequiredException, CommitRequiredException {

		if (this.transactionStatus == Status.STATUS_ROLLEDBACK) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_ROLLING_BACK) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_UNKNOWN) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_NO_TRANSACTION) {
			// it's impossible
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_PREPARED) {
			throw new CommitRequiredException();
		} else if (this.transactionStatus == Status.STATUS_COMMITTING) {
			throw new CommitRequiredException();
		} else if (this.transactionStatus == Status.STATUS_COMMITTED) {
			throw new CommitRequiredException();
		} /* else active, marked_rollback, preparing {} */

		// stop-timing
		TransactionTimer transactionTimer = beanFactory.getTransactionTimer();
		transactionTimer.stopTiming(this);

		// before-completion
		this.synchronizationList.beforeCompletion();

		// delist all resources
		try {
			this.delistAllResource();
		} catch (RollbackRequiredException rrex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			throw new RollbackRequiredException();
		} catch (SystemException ex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			throw new RollbackRequiredException();
		} catch (RuntimeException rex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			throw new RollbackRequiredException();
		}

		TransactionXid xid = this.transactionContext.getXid();

		TransactionArchive archive = this.getTransactionArchive();

		int firstVote = XAResource.XA_RDONLY;
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		try {
			this.transactionStatus = Status.STATUS_PREPARING;
			archive.setStatus(this.transactionStatus);
			transactionLogger.createTransaction(archive);

			this.transactionListenerList.onPrepareStart(xid);

			// firstVote = this.firstTerminator.prepare(xid);
			firstVote = this.nativeTerminator.prepare(xid);
		} catch (XAException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			throw new RollbackRequiredException();
		} catch (RuntimeException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			throw new RollbackRequiredException();
		}

		int lastVote = XAResource.XA_RDONLY;
		try {
			// lastVote = this.lastTerminator.prepare(xid);
			lastVote = this.remoteTerminator.prepare(xid);
		} catch (XAException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			this.transactionListenerList.onPrepareFailure(xid);
			throw new RollbackRequiredException();
		} catch (RuntimeException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			this.transactionListenerList.onPrepareFailure(xid);
			throw new RollbackRequiredException();
		}

		this.transactionStatus = Status.STATUS_PREPARED;
		archive.setStatus(this.transactionStatus);
		this.transactionListenerList.onPrepareSuccess(xid);

		if (firstVote == XAResource.XA_OK || lastVote == XAResource.XA_OK) {
			this.transactionVote = XAResource.XA_OK;
			archive.setVote(XAResource.XA_OK);
			transactionLogger.updateTransaction(archive);
			throw new CommitRequiredException();
		} else {
			this.transactionVote = XAResource.XA_RDONLY;
			archive.setVote(XAResource.XA_RDONLY);
			transactionLogger.deleteTransaction(archive);
		}

	}

	public synchronized void participantCommit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, CommitRequiredException, SystemException {

		if (this.transactionStatus == Status.STATUS_ACTIVE) {
			throw new IllegalStateException();
		} else if (this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			this.rollback();
			throw new HeuristicRollbackException();
		} else if (this.transactionStatus == Status.STATUS_ROLLING_BACK) {
			throw new IllegalStateException();
		} else if (this.transactionStatus == Status.STATUS_ROLLEDBACK) {
			throw new RollbackException();
		} else if (this.transactionStatus == Status.STATUS_UNKNOWN) {
			throw new IllegalStateException();
		} else if (this.transactionStatus == Status.STATUS_NO_TRANSACTION) {
			throw new IllegalStateException();
		} else if (this.transactionStatus == Status.STATUS_COMMITTED) {
			return;
		} /* else preparing, prepared, committing {} */

		TransactionXid xid = this.transactionContext.getXid();
		TransactionArchive archive = this.getTransactionArchive();
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();

		logger.info("[{}] commit-transaction start", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));

		this.transactionStatus = Status.STATUS_COMMITTING;
		this.transactionListenerList.onCommitStart(xid);

		boolean mixedExists = false;
		boolean unFinishExists = false;
		try {
			this.nativeTerminator.commit(xid, false);
		} catch (XAException xaex) {
			unFinishExists = TransactionException.class.isInstance(xaex);

			switch (xaex.errorCode) {
			case XAException.XA_HEURCOM:
				break;
			case XAException.XA_HEURMIX:
				mixedExists = true;
				break;
			case XAException.XA_HEURRB:
				this.rollback();
				throw new HeuristicRollbackException();
			default:
				logger.warn("Unknown state in committing transaction phase.");
			}
		}

		boolean transactionCompleted = false;
		try {
			this.remoteTerminator.commit(xid, false);
			if (mixedExists) {
				this.transactionListenerList.onCommitHeuristicMixed(xid);
				throw new HeuristicMixedException();
			} else if (unFinishExists == false) {
				transactionCompleted = true;
				this.transactionListenerList.onCommitSuccess(xid);
			} else {
				this.transactionListenerList.onCommitFailure(xid);
				throw new CommitRequiredException();
			}
		} catch (TransactionException xaex) {
			this.transactionListenerList.onCommitFailure(xid);
			CommitRequiredException ex = new CommitRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.transactionListenerList.onCommitFailure(xid);
				CommitRequiredException ex = new CommitRequiredException();
				ex.initCause(xaex);
				throw ex;
			} else if (mixedExists) {
				this.transactionListenerList.onCommitHeuristicMixed(xid);
				throw new HeuristicMixedException();
			} else {
				switch (xaex.errorCode) {
				case XAException.XA_HEURMIX:
					this.transactionListenerList.onCommitHeuristicMixed(xid);
					throw new HeuristicMixedException();
				case XAException.XA_HEURCOM:
					transactionCompleted = true;
					this.transactionListenerList.onCommitSuccess(xid);
					break;
				case XAException.XA_HEURRB:
					this.transactionListenerList.onCommitHeuristicMixed(xid);
					throw new HeuristicMixedException();
				default:
					this.transactionListenerList.onCommitFailure(xid);
					logger.warn("Unknown state in committing transaction phase.");
					CommitRequiredException ex = new CommitRequiredException();
					ex.initCause(xaex);
					throw ex;
				}
			}
		} finally {
			if (transactionCompleted) {
				this.transactionStatus = Status.STATUS_COMMITTED;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);

				logger.info("[{}] commit-transaction complete successfully",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
			}
		}

	}

	public synchronized void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, CommitRequiredException, SystemException {

		try {
			this.checkBeforeCommit();
		} catch (RollbackRequiredException rrex) {
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (CommitRequiredException crex) {
			this.delegateCommit();
		}

	}

	private synchronized void delegateCommit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, CommitRequiredException, SystemException {
		// stop-timing
		TransactionTimer transactionTimer = beanFactory.getTransactionTimer();
		transactionTimer.stopTiming(this);

		// before-completion
		this.synchronizationList.beforeCompletion();

		// delist all resources
		try {
			this.delistAllResource();
		} catch (RollbackRequiredException rrex) {
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (SystemException ex) {
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (RuntimeException rex) {
			this.rollback();
			throw new HeuristicRollbackException();
		}

		try {
			TransactionXid xid = this.transactionContext.getXid();
			logger.info("[{}] commit-transaction start", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));

			int nativeResNum = this.nativeTerminator.getResourceArchives().size();
			int remoteResNum = this.remoteTerminator.getResourceArchives().size();
			int sizeOfResNum = nativeResNum + remoteResNum;
			if (sizeOfResNum == 1) {
				this.fireOnePhaseCommit();
			} else if (sizeOfResNum > 1) {
				this.fireTwoPhaseCommit();
			} else {
				this.transactionListenerList.onCommitStart(xid);
				this.transactionListenerList.onCommitSuccess(xid);
			}

			logger.info("[{}] commit-transaction complete successfully",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
		} finally {
			this.synchronizationList.afterCompletion(this.transactionStatus);
		}
	}

	public synchronized void fireOnePhaseCommit()
			throws HeuristicRollbackException, HeuristicMixedException, CommitRequiredException, SystemException {
		TransactionXid xid = this.transactionContext.getXid();
		try {
			this.transactionListenerList.onCommitStart(xid);
			if (this.nativeTerminator.getResourceArchives().size() > 0) {
				this.nativeTerminator.commit(xid, true);
			} else {
				this.remoteTerminator.commit(xid, true);
			}
			this.transactionListenerList.onCommitSuccess(xid);
		} catch (TransactionException xaex) {
			this.transactionListenerList.onCommitFailure(xid);
			CommitRequiredException ex = new CommitRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURMIX:
				this.transactionListenerList.onCommitHeuristicMixed(xid);
				throw new HeuristicMixedException();
			case XAException.XA_HEURCOM:
				this.transactionListenerList.onCommitSuccess(xid);
				return;
			case XAException.XA_HEURRB:
				this.transactionListenerList.onCommitHeuristicRolledback(xid);
				throw new HeuristicRollbackException();
			default:
				this.transactionListenerList.onCommitFailure(xid);
				logger.warn("Unknown state in committing transaction phase.");
				SystemException ex = new SystemException();
				ex.initCause(xaex);
				throw ex;
			}
		}
	}

	public synchronized void fireTwoPhaseCommit()
			throws HeuristicRollbackException, HeuristicMixedException, CommitRequiredException, SystemException {
		TransactionXid xid = this.transactionContext.getXid();

		TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();

		int firstVote = XAResource.XA_RDONLY;
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		try {
			this.transactionStatus = Status.STATUS_PREPARING;// .setStatusPreparing();
			archive.setStatus(this.transactionStatus);
			transactionLogger.createTransaction(archive);

			this.transactionListenerList.onPrepareStart(xid);

			firstVote = this.nativeTerminator.prepare(xid);
		} catch (XAException xaex) {
			this.transactionListenerList.onPrepareFailure(xid);
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (RuntimeException xaex) {
			this.transactionListenerList.onPrepareFailure(xid);
			this.rollback();
			throw new HeuristicRollbackException();
		}

		int lastVote = XAResource.XA_RDONLY;
		try {
			lastVote = this.remoteTerminator.prepare(xid);
			this.transactionListenerList.onPrepareSuccess(xid);
		} catch (XAException xaex) {
			this.transactionListenerList.onPrepareFailure(xid);
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (RuntimeException xaex) {
			this.transactionListenerList.onPrepareFailure(xid);
			this.rollback();
			throw new HeuristicRollbackException();
		}

		if (firstVote == XAResource.XA_OK || lastVote == XAResource.XA_OK) {
			this.transactionStatus = Status.STATUS_PREPARED;// .setStatusPrepared();
			this.transactionVote = XAResource.XA_OK;
			archive.setVote(XAResource.XA_OK);
			archive.setStatus(this.transactionStatus);
			transactionLogger.updateTransaction(archive);

			this.transactionStatus = Status.STATUS_COMMITTING;// .setStatusCommiting();
			this.transactionListenerList.onCommitStart(xid);

			boolean mixedExists = false;
			boolean unFinishExists = false;
			try {
				this.remoteTerminator.commit(xid, false);
			} catch (XAException xaex) {
				unFinishExists = TransactionException.class.isInstance(xaex);

				switch (xaex.errorCode) {
				case XAException.XA_HEURCOM:
					break;
				case XAException.XA_HEURMIX:
					mixedExists = true;
					break;
				case XAException.XA_HEURRB:
					this.rollback();
					throw new HeuristicRollbackException();
				default:
					logger.warn("Unknown state in committing transaction phase.");
				}
			}

			boolean transactionCompleted = false;
			try {
				this.nativeTerminator.commit(xid, false);
				if (mixedExists) {
					this.transactionListenerList.onCommitHeuristicMixed(xid);
					throw new HeuristicMixedException();
				} else if (unFinishExists == false) {
					transactionCompleted = true;
					this.transactionListenerList.onCommitSuccess(xid);
				} else {
					this.transactionListenerList.onCommitFailure(xid);
					throw new CommitRequiredException();
				}
			} catch (TransactionException xaex) {
				this.transactionListenerList.onCommitFailure(xid);
				CommitRequiredException ex = new CommitRequiredException();
				ex.initCause(xaex);
				throw ex;
			} catch (XAException xaex) {
				if (unFinishExists) {
					this.transactionListenerList.onCommitFailure(xid);
					CommitRequiredException ex = new CommitRequiredException();
					ex.initCause(xaex);
					throw ex;
				} else if (mixedExists) {
					this.transactionListenerList.onCommitHeuristicMixed(xid);
					throw new HeuristicMixedException();
				} else {
					switch (xaex.errorCode) {
					case XAException.XA_HEURMIX:
						this.transactionListenerList.onCommitHeuristicMixed(xid);
						throw new HeuristicMixedException();
					case XAException.XA_HEURCOM:
						transactionCompleted = true;
						this.transactionListenerList.onCommitSuccess(xid);
						break;
					case XAException.XA_HEURRB:
						if (firstVote == XAResource.XA_RDONLY) {
							this.transactionListenerList.onCommitHeuristicRolledback(xid);
							// transactionCompleted = true; // TODO
							throw new HeuristicRollbackException();
						} else {
							this.transactionListenerList.onCommitHeuristicMixed(xid);
							throw new HeuristicMixedException();
						}
					default:
						this.transactionListenerList.onCommitFailure(xid);
						logger.warn("Unknown state in committing transaction phase.");
						CommitRequiredException ex = new CommitRequiredException();
						ex.initCause(xaex);
						throw ex;
					}
				}
			} finally {
				if (transactionCompleted) {
					this.transactionStatus = Status.STATUS_COMMITTED;// .setStatusCommitted();
					archive.setStatus(this.transactionStatus);
					transactionLogger.deleteTransaction(archive);
				}
			}
		} else {
			this.transactionStatus = Status.STATUS_PREPARED;// .setStatusPrepared();
			this.transactionVote = XAResource.XA_RDONLY;
			archive.setVote(XAResource.XA_RDONLY);
			archive.setStatus(this.transactionStatus);
			this.transactionListenerList.onCommitSuccess(xid);
			transactionLogger.deleteTransaction(archive);
		}

	}

	public synchronized boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
		if (this.transactionStatus != Status.STATUS_ACTIVE && this.transactionStatus != Status.STATUS_MARKED_ROLLBACK) {
			throw new IllegalStateException();
		}

		try {
			return this.invokeDelistResource(xaRes, flag);
		} finally {
			if (flag == XAResource.TMFAIL) {
				this.setRollbackOnlyQuietly();
			}
		}

	}

	private boolean invokeDelistResource(XAResource xaRes, int flag) throws IllegalStateException, SystemException {
		XATerminator terminator = null;
		XAResourceDescriptor descriptor = null;
		if (XAResourceDescriptor.class.isInstance(xaRes)) {
			descriptor = (XAResourceDescriptor) xaRes;
			if (CommonResourceDescriptor.class.isInstance(xaRes)) {
				terminator = this.nativeTerminator;
			} else if (RemoteResourceDescriptor.class.isInstance(xaRes)) {
				terminator = this.remoteTerminator;
			} else {
				terminator = this.remoteTerminator;
			}
		} else {
			descriptor = new UnidentifiedResourceDescriptor();
			((UnidentifiedResourceDescriptor) descriptor).setDelegate(xaRes);
			terminator = this.remoteTerminator;
		}

		return terminator.delistResource(descriptor, flag);
	}

	public synchronized boolean enlistResource(XAResource xaRes)
			throws RollbackException, IllegalStateException, SystemException {

		if (this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackException();
		} else if (this.transactionStatus != Status.STATUS_ACTIVE) {
			throw new IllegalStateException();
		}

		XATerminator terminator = null;
		XAResourceDescriptor descriptor = null;
		if (XAResourceDescriptor.class.isInstance(xaRes)) {
			descriptor = (XAResourceDescriptor) xaRes;
			descriptor.setTransactionTimeoutQuietly(this.transactionTimeout);

			if (CommonResourceDescriptor.class.isInstance(xaRes)) {
				terminator = this.nativeTerminator;
			} else if (RemoteResourceDescriptor.class.isInstance(xaRes)) {
				terminator = this.remoteTerminator;
			} else {
				terminator = this.remoteTerminator;
			}
		} else if (this.transactionContext.isCoordinator()) {
			descriptor = new UnidentifiedResourceDescriptor();
			((UnidentifiedResourceDescriptor) descriptor).setDelegate(xaRes);
			descriptor.setTransactionTimeoutQuietly(this.transactionTimeout);

			terminator = this.remoteTerminator;
		} else {
			throw new SystemException("Unknown xa resource!");
		}

		return terminator.enlistResource(descriptor);
	}

	public int getStatus() /* throws SystemException */ {
		return this.transactionStatus;
	}

	public synchronized void registerSynchronization(Synchronization sync)
			throws RollbackException, IllegalStateException, SystemException {

		if (this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackException();
		} else if (this.transactionStatus == Status.STATUS_ACTIVE) {
			this.synchronizationList.registerSynchronizationQuietly(sync);
			logger.debug("[{}] register-sync: sync= {}"//
					, ByteUtils.byteArrayToString(this.transactionContext.getXid().getGlobalTransactionId()), sync);
		} else {
			throw new IllegalStateException();
		}

	}

	private void checkBeforeRollback() throws IllegalStateException, RollbackRequiredException {

		if (this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_ACTIVE) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_ROLLEDBACK) {
			logger.debug("Current transaction has already been rolled back.");
		} else if (this.transactionStatus == Status.STATUS_COMMITTED) {
			throw new IllegalStateException();
		} else {
			throw new RollbackRequiredException();
		}

	}

	public synchronized void rollback() throws IllegalStateException, RollbackRequiredException, SystemException {

		try {
			this.checkBeforeRollback();
		} catch (RollbackRequiredException rrex) {
			this.delegateRollback();
		}

	}

	private synchronized void delegateRollback() throws IllegalStateException, RollbackRequiredException, SystemException {
		// stop-timing
		TransactionTimer transactionTimer = beanFactory.getTransactionTimer();
		transactionTimer.stopTiming(this);

		// before-completion
		this.synchronizationList.beforeCompletion();

		// delist all resources
		this.delistAllResourceQuietly();

		try {
			TransactionXid xid = this.transactionContext.getXid();
			logger.info("[{}] rollback-transaction start", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));

			this.invokeRollback();

			logger.info("[{}] rollback-transaction complete successfully",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
		} finally {
			this.synchronizationList.afterCompletion(this.transactionStatus);
		}
	}

	public synchronized void invokeRollback() throws IllegalStateException, RollbackRequiredException, SystemException {

		boolean unFinishExists = false;
		boolean commitExists = false;
		boolean mixedExists = false;
		boolean transactionCompleted = false;
		TransactionXid xid = this.transactionContext.getXid();

		TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();

		this.transactionStatus = Status.STATUS_ROLLING_BACK;
		archive.setStatus(this.transactionStatus);

		this.transactionListenerList.onRollbackStart(xid);

		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		transactionLogger.createTransaction(archive);

		// rollback the native-resource
		try {
			this.nativeTerminator.rollback(xid);
		} catch (TransactionException xaex) {
			unFinishExists = true;
		} catch (XAException xaex) {
			unFinishExists = TransactionException.class.isInstance(xaex);

			switch (xaex.errorCode) {
			case XAException.XA_HEURRB:
				break;
			case XAException.XA_HEURMIX:
				mixedExists = true;
				break;
			case XAException.XA_HEURCOM:
				commitExists = true;
				break;
			default:
				logger.warn("Unknown state in rollingback transaction phase.");
			}
		}

		// rollback the remote-resource
		try {
			this.remoteTerminator.rollback(xid);
			if (mixedExists) {
				this.transactionListenerList.onRollbackFailure(xid);
				throw new SystemException();
			} else if (unFinishExists == false) {
				transactionCompleted = true;
				this.transactionListenerList.onRollbackSuccess(xid);
			} else {
				this.transactionListenerList.onRollbackFailure(xid);
				throw new RollbackRequiredException();
			}
		} catch (TransactionException xaex) {
			this.transactionListenerList.onRollbackFailure(xid);
			RollbackRequiredException ex = new RollbackRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.transactionListenerList.onRollbackFailure(xid);
				RollbackRequiredException ex = new RollbackRequiredException();
				ex.initCause(xaex);
				throw ex;
			} else if (mixedExists) {
				this.transactionListenerList.onRollbackFailure(xid);
				SystemException systemErr = new SystemException();
				systemErr.initCause(new XAException(XAException.XA_HEURMIX));
				throw systemErr;
			} else {
				switch (xaex.errorCode) {
				case XAException.XA_HEURRB:
					if (commitExists) {
						this.transactionListenerList.onRollbackFailure(xid);
						SystemException systemErr = new SystemException();
						systemErr.initCause(new XAException(XAException.XA_HEURMIX));
						throw systemErr;
					} else {
						transactionCompleted = true;
						this.transactionListenerList.onRollbackSuccess(xid);
						break;
					}
				case XAException.XA_HEURMIX:
					this.transactionListenerList.onRollbackFailure(xid);
					SystemException systemErr = new SystemException();
					systemErr.initCause(new XAException(XAException.XA_HEURMIX));
					throw systemErr;
				case XAException.XA_HEURCOM:
					if (commitExists) {
						this.transactionListenerList.onRollbackFailure(xid);
						SystemException committedErr = new SystemException();
						committedErr.initCause(new XAException(XAException.XA_HEURCOM));
						throw committedErr;
					} else {
						this.transactionListenerList.onRollbackFailure(xid);
						SystemException mixedErr = new SystemException();
						mixedErr.initCause(new XAException(XAException.XA_HEURMIX));
						throw mixedErr;
					}
				default:
					this.transactionListenerList.onRollbackFailure(xid);
					logger.warn("Unknown state in rollingback transaction phase.");
					throw new SystemException();
				}
			}
		} finally {
			if (transactionCompleted) {
				this.transactionStatus = Status.STATUS_ROLLEDBACK;// .setStatusCommitted();
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);
			}
		}

	}

	public void suspend() throws SystemException {
		SystemException throwable = null;
		if (this.nativeTerminator != null) {
			try {
				this.nativeTerminator.suspendAllResource();
			} catch (RollbackException rex) {
				this.setRollbackOnlyQuietly();
				throwable = new SystemException();
				throwable.initCause(rex);
			} catch (SystemException ex) {
				throwable = ex;
			}
		}

		if (this.remoteTerminator != null) {
			try {
				this.remoteTerminator.suspendAllResource();
			} catch (RollbackException rex) {
				this.setRollbackOnlyQuietly();
				throwable = new SystemException();
				throwable.initCause(rex);
			} catch (SystemException ex) {
				throwable = ex;
			}
		}

		if (throwable != null) {
			throw throwable;
		}

	}

	public void resume() throws SystemException {
		SystemException throwable = null;
		if (this.nativeTerminator != null) {
			try {
				this.nativeTerminator.resumeAllResource();
			} catch (RollbackException rex) {
				this.setRollbackOnlyQuietly();
				throwable = new SystemException();
				throwable.initCause(rex);
			} catch (SystemException ex) {
				throwable = ex;
			}
		}

		if (this.remoteTerminator != null) {
			try {
				this.remoteTerminator.resumeAllResource();
			} catch (RollbackException rex) {
				this.setRollbackOnlyQuietly();
				throwable = new SystemException();
				throwable.initCause(rex);
			} catch (SystemException ex) {
				throwable = ex;
			}
		}

		if (throwable != null) {
			throw throwable;
		}

	}

	private void delistAllResourceQuietly() {
		try {
			this.delistAllResource();
		} catch (RollbackRequiredException rrex) {
			logger.warn(rrex.getMessage());
		} catch (SystemException ex) {
			logger.warn(ex.getMessage());
		} catch (RuntimeException rex) {
			logger.warn(rex.getMessage());
		}
	}

	private void delistAllResource() throws RollbackRequiredException, SystemException {
		RollbackRequiredException rrex = null;
		SystemException systemEx = null;
		if (this.nativeTerminator != null) {
			try {
				this.nativeTerminator.delistAllResource();
			} catch (RollbackException rex) {
				rrex = new RollbackRequiredException();
			} catch (SystemException ex) {
				systemEx = ex;
			}
		}

		if (this.remoteTerminator != null) {
			try {
				this.remoteTerminator.delistAllResource();
			} catch (RollbackException rex) {
				rrex = new RollbackRequiredException();
			} catch (SystemException ex) {
				systemEx = ex;
			}
		}

		if (rrex != null) {
			throw rrex;
		} else if (systemEx != null) {
			throw systemEx;
		}

	}

	public void setRollbackOnlyQuietly() {
		try {
			this.setRollbackOnly();
		} catch (Exception ex) {
			logger.debug(ex.getMessage(), ex);
		}
	}

	public synchronized void setRollbackOnly() throws IllegalStateException, SystemException {
		if (this.transactionStatus == Status.STATUS_ACTIVE || this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			this.transactionStatus = Status.STATUS_MARKED_ROLLBACK;
		} else {
			throw new IllegalStateException();
		}
	}

	public synchronized void cleanup() {

		TransactionXid xid = this.transactionContext.getXid();

		try {
			this.nativeTerminator.forget(xid);
		} catch (XAException ex) {
			logger.debug(ex.getMessage(), ex);
		}

		try {
			this.remoteTerminator.forget(xid);
		} catch (XAException ex) {
			logger.debug(ex.getMessage(), ex);
		}

	}

	public synchronized void recoveryInit() throws SystemException {
		SystemException systemEx = null;
		try {
			this.nativeTerminator.recover(this);
		} catch (SystemException ex) {
			systemEx = ex;
		}

		this.remoteTerminator.recover(this);
		if (systemEx != null) {
			throw systemEx;
		}
	}

	public synchronized void recoveryCommit() throws CommitRequiredException, SystemException {

		if (this.transactionContext.isRecoveried()) {
			this.recoveryInit();
		}

		TransactionXid xid = this.transactionContext.getXid();

		this.transactionStatus = Status.STATUS_COMMITTING;// .setStatusCommiting();
		boolean committedExists = false;
		boolean rolledbackExists = false;

		this.transactionListenerList.onCommitStart(xid);

		boolean unFinishExists = false;
		try {
			this.nativeTerminator.recoveryCommit(xid);
			committedExists = true;
		} catch (TransactionException xaex) {
			unFinishExists = true;
			logger.debug(xaex.getMessage(), xaex);
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURMIX:
				committedExists = true;
				rolledbackExists = true;
				break;
			case XAException.XA_HEURCOM:
				committedExists = true;
				break;
			case XAException.XA_HEURRB:
				rolledbackExists = true;
				break;
			default:
				unFinishExists = true;
				logger.warn("Unknown state in recovery-committing phase.", xaex);
			}
		}

		try {
			this.remoteTerminator.recoveryCommit(xid);
			committedExists = true;
		} catch (TransactionException xaex) {
			unFinishExists = true;
			logger.debug(xaex.getMessage(), xaex);
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURMIX:
				committedExists = true;
				rolledbackExists = true;
				break;
			case XAException.XA_HEURCOM:
				committedExists = true;
				break;
			case XAException.XA_HEURRB:
				rolledbackExists = true;
				break;
			default:
				unFinishExists = true;
				logger.warn("Unknown state in recovery-committing phase.", xaex);
			}
		} finally {
			if (unFinishExists) {
				this.transactionListenerList.onCommitFailure(xid);
				throw new CommitRequiredException();
			} else {
				TransactionArchive archive = this.getTransactionArchive();
				TransactionLogger transactionLogger = beanFactory.getTransactionLogger();

				this.transactionStatus = Status.STATUS_COMMITTED;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive); // TODO

				if (committedExists && rolledbackExists) {
					this.transactionListenerList.onCommitHeuristicMixed(xid);
					logger.error("[{}] recovery-commit: committedExists= true, rolledbackExists= true",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
				} else if (rolledbackExists) {
					this.transactionListenerList.onCommitHeuristicRolledback(xid);
					logger.info("[{}] recovery-commit: rolled back successfully",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
				} else {
					this.transactionListenerList.onCommitSuccess(xid);
					logger.info("[{}] recovery-commit: committed successfully",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
				}
			}
		}

	}

	public synchronized void recoveryRollback() throws RollbackRequiredException, SystemException {

		if (this.transactionContext.isRecoveried()) {
			this.recoveryInit();
		}

		TransactionXid xid = this.transactionContext.getXid();

		this.transactionStatus = Status.STATUS_ROLLING_BACK;

		this.transactionListenerList.onRollbackStart(xid);

		boolean committedExists = false;
		boolean rolledbackExists = false;
		boolean unFinishExists = false;
		try {
			this.nativeTerminator.recoveryRollback(xid);
			rolledbackExists = true;
		} catch (TransactionException xaex) {
			unFinishExists = true;
			logger.debug(xaex.getMessage(), xaex);
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURMIX:
				committedExists = true;
				rolledbackExists = true;
				break;
			case XAException.XA_HEURCOM:
				committedExists = true;
				break;
			case XAException.XA_HEURRB:
				rolledbackExists = true;
				break;
			default:
				unFinishExists = true;
				logger.warn("Unknown state in recovery-rollingback phase.", xaex);
			}
		}

		try {
			this.remoteTerminator.recoveryRollback(xid);
			rolledbackExists = true;
		} catch (TransactionException xaex) {
			unFinishExists = true;
			logger.debug(xaex.getMessage(), xaex);
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURMIX:
				committedExists = true;
				rolledbackExists = true;
				break;
			case XAException.XA_HEURCOM:
				committedExists = true;
				break;
			case XAException.XA_HEURRB:
				rolledbackExists = true;
				break;
			default:
				unFinishExists = true;
				logger.warn("Unknown state in recovery-committing phase.", xaex);
			}
		} finally {
			if (unFinishExists) {
				this.transactionListenerList.onRollbackFailure(xid);
				throw new RollbackRequiredException();
			} else {
				TransactionArchive archive = this.getTransactionArchive();
				TransactionLogger transactionLogger = beanFactory.getTransactionLogger();

				this.transactionStatus = Status.STATUS_ROLLEDBACK;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive); // TODO

				if (committedExists && rolledbackExists) {
					this.transactionListenerList.onRollbackFailure(xid);
					logger.error("[{}] recovery-rollback: committedExists= true, rolledbackExists= true",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
				} else if (rolledbackExists) {
					this.transactionListenerList.onRollbackSuccess(xid);
					logger.info("[{}] recovery-rollback: rolled back successfully",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
				} else {
					this.transactionListenerList.onRollbackFailure(xid);
					logger.info("[{}] recovery-rollback: committed successfully",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
				}
			}

		}

	}

	public synchronized void recoveryForget() throws SystemException {

		TransactionXid xid = this.transactionContext.getXid();

		try {
			this.nativeTerminator.forget(xid);
			logger.info("[{}] forget native terminator successfully",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
		} catch (XAException xaex) {
			logger.info("[{}] forget native terminator failued", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
		}

		try {
			this.remoteTerminator.forget(xid);
			logger.info("[{}] forget remote terminator successfully",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
		} catch (XAException xaex) {
			logger.info("[{}] forget remote terminator failed", ByteUtils.byteArrayToString(xid.getGlobalTransactionId()));
		}

		TransactionRepository repository = beanFactory.getTransactionRepository();
		repository.removeErrorTransaction(xid);
		repository.removeTransaction(xid);

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();
		transactionLogger.deleteTransaction(this.getTransactionArchive());

	}

	public TransactionArchive getTransactionArchive() {
		TransactionArchive transactionArchive = new TransactionArchive();
		transactionArchive.setVote(this.transactionVote);
		transactionArchive.setXid(this.transactionContext.getXid());
		transactionArchive.setCoordinator(this.transactionContext.isCoordinator());
		transactionArchive.getNativeResources().addAll(this.nativeTerminator.getResourceArchives());
		transactionArchive.getRemoteResources().addAll(this.remoteTerminator.getResourceArchives());
		transactionArchive.setStatus(this.transactionStatus);
		return transactionArchive;
	}

	public int hashCode() {
		TransactionXid transactionXid = this.transactionContext == null ? null : this.transactionContext.getXid();
		int hash = transactionXid == null ? 0 : transactionXid.hashCode();
		return hash;
	}

	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (TransactionImpl.class.equals(obj.getClass()) == false) {
			return false;
		}
		TransactionImpl that = (TransactionImpl) obj;
		TransactionContext thisContext = this.transactionContext;
		TransactionContext thatContext = that.transactionContext;
		TransactionXid thisXid = thisContext == null ? null : thisContext.getXid();
		TransactionXid thatXid = thatContext == null ? null : thatContext.getXid();
		return CommonUtils.equals(thisXid, thatXid);
	}

	public void registerTransactionListener(TransactionListener listener) {
		this.transactionListenerList.registerTransactionListener(listener);
	}

	public void registerTransactionResourceListener(TransactionResourceListener listener) {
		this.nativeTerminator.registerTransactionResourceListener(listener);
		this.remoteTerminator.registerTransactionResourceListener(listener);
	}

	public Object getTransactionalExtra() {
		return transactionalExtra;
	}

	public void setTransactionalExtra(Object transactionalExtra) {
		this.transactionalExtra = transactionalExtra;
	}

	public TransactionContext getTransactionContext() {
		return transactionContext;
	}

	public XATerminator getNativeTerminator() {
		return nativeTerminator;
	}

	public XATerminator getRemoteTerminator() {
		return remoteTerminator;
	}

	public boolean isTiming() {
		return timing;
	}

	public void setTiming(boolean timing) {
		this.timing = timing;
	}

	public int getTransactionStatus() {
		return transactionStatus;
	}

	public void setTransactionStatus(int transactionStatus) {
		this.transactionStatus = transactionStatus;
	}

	public int getTransactionTimeout() {
		return transactionTimeout;
	}

	public void setTransactionTimeout(int transactionTimeout) {
		this.transactionTimeout = transactionTimeout;
	}

}
