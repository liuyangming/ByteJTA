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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.apache.log4j.Logger;
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
import org.bytesoft.transaction.resource.XATerminator;
import org.bytesoft.transaction.supports.TransactionListener;
import org.bytesoft.transaction.supports.TransactionTimer;
import org.bytesoft.transaction.supports.logger.TransactionLogger;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionImpl implements Transaction {
	static final Logger logger = Logger.getLogger(TransactionImpl.class.getSimpleName());

	private transient boolean timing = true;
	private TransactionBeanFactory beanFactory;

	private int transactionStatus;
	private int transactionTimeout;
	private int transactionVote;
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

	private synchronized void checkBeforeCommit() throws RollbackException, IllegalStateException,
			RollbackRequiredException, CommitRequiredException {

		if (this.transactionStatus == Status.STATUS_ROLLEDBACK) {
			throw new RollbackException();
		} else if (this.transactionStatus == Status.STATUS_ROLLING_BACK) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackRequiredException();
		} else if (this.transactionStatus == Status.STATUS_ACTIVE) {
			throw new CommitRequiredException();
		} else if (this.transactionStatus == Status.STATUS_COMMITTED) {
			// ignore
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

			this.transactionListenerList.prepareStart();

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
			this.transactionListenerList.prepareFailure();
			throw new RollbackRequiredException();
		} catch (RuntimeException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			this.transactionListenerList.prepareFailure();
			throw new RollbackRequiredException();
		}

		this.transactionStatus = Status.STATUS_PREPARED;
		archive.setStatus(this.transactionStatus);
		this.transactionListenerList.prepareSuccess();

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

	public synchronized void participantCommit() throws RollbackException, HeuristicMixedException,
			HeuristicRollbackException, SecurityException, IllegalStateException, CommitRequiredException,
			SystemException {

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

		logger.info(String.format("[%s] commit-transaction start",
				ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));

		this.transactionStatus = Status.STATUS_COMMITTING;
		this.transactionListenerList.commitStart();

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
			if (unFinishExists == false) {
				transactionCompleted = true;
				this.transactionListenerList.commitSuccess();
			} else {
				this.transactionListenerList.commitFailure();
			}
		} catch (TransactionException xaex) {
			this.transactionListenerList.commitFailure();
			CommitRequiredException ex = new CommitRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.transactionListenerList.commitFailure();
				CommitRequiredException ex = new CommitRequiredException();
				ex.initCause(xaex);
				throw ex;
			} else if (mixedExists) {
				this.transactionListenerList.commitHeuristicMixed();
				transactionCompleted = true;
				throw new HeuristicMixedException();
			} else {
				transactionCompleted = true;
				switch (xaex.errorCode) {
				case XAException.XA_HEURMIX:
					this.transactionListenerList.commitHeuristicMixed();
					throw new HeuristicMixedException();
				case XAException.XA_HEURCOM:
					this.transactionListenerList.commitSuccess();
					// this.transactionListenerList.commitFailure(TransactionListener.OPT_HEURCOM);
					break;
				case XAException.XA_HEURRB:
					this.transactionListenerList.commitHeuristicMixed();
					throw new HeuristicMixedException();
				default:
					this.transactionListenerList.commitFailure();
					logger.warn("Unknown state in committing transaction phase.");
				}
			}
		} finally {
			if (transactionCompleted) {
				this.transactionStatus = Status.STATUS_COMMITTED;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);

				logger.info(String.format("[%s] commit-transaction complete successfully",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
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

	private synchronized void delegateCommit() throws RollbackException, HeuristicMixedException,
			HeuristicRollbackException, SecurityException, IllegalStateException, CommitRequiredException,
			SystemException {
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
			logger.info(String.format("[%s] commit-transaction start",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));

			int nativeResNum = this.nativeTerminator.getResourceArchives().size();
			int remoteResNum = this.remoteTerminator.getResourceArchives().size();
			boolean onePhaseCommitAllowed = (nativeResNum + remoteResNum) <= 1;
			if (onePhaseCommitAllowed) {
				this.fireOnePhaseCommit();
			} else {
				this.fireTwoPhaseCommit();
			}

			logger.info(String.format("[%s] commit-transaction complete successfully",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
		} finally {
			this.synchronizationList.afterCompletion(this.transactionStatus);
		}
	}

	public synchronized void fireOnePhaseCommit() throws HeuristicRollbackException, HeuristicMixedException,
			CommitRequiredException, SystemException {
		TransactionXid xid = this.transactionContext.getXid();
		try {
			this.transactionListenerList.commitStart();
			if (this.nativeTerminator.getResourceArchives().size() > 0) {
				this.nativeTerminator.commit(xid, true);
			} else {
				this.remoteTerminator.commit(xid, true);
			}
			this.transactionListenerList.commitSuccess();
		} catch (TransactionException xaex) {
			this.transactionListenerList.commitFailure();
			CommitRequiredException ex = new CommitRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURMIX:
				this.transactionListenerList.commitHeuristicMixed();
				throw new HeuristicMixedException();
			case XAException.XA_HEURCOM:
				this.transactionListenerList.commitSuccess();
				return;
			case XAException.XA_HEURRB:
				// this.transactionListenerList.rollbackStart();
				// this.transactionListenerList.rollbackSuccess();
				this.transactionListenerList.commitHeuristicRolledback();
				throw new HeuristicRollbackException();
			default:
				this.transactionListenerList.commitFailure();
				logger.warn("Unknown state in committing transaction phase.");
				SystemException ex = new SystemException();
				ex.initCause(xaex);
				throw ex;
			}
		}
	}

	public synchronized void fireTwoPhaseCommit() throws HeuristicRollbackException, HeuristicMixedException,
			CommitRequiredException, SystemException {
		TransactionXid xid = this.transactionContext.getXid();

		TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();

		int firstVote = XAResource.XA_RDONLY;
		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		try {
			this.transactionStatus = Status.STATUS_PREPARING;// .setStatusPreparing();
			archive.setStatus(this.transactionStatus);
			transactionLogger.createTransaction(archive);

			this.transactionListenerList.prepareStart();

			firstVote = this.nativeTerminator.prepare(xid);
		} catch (XAException xaex) {
			this.transactionListenerList.prepareFailure();
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (RuntimeException xaex) {
			this.transactionListenerList.prepareFailure();
			this.rollback();
			throw new HeuristicRollbackException();
		}

		int lastVote = XAResource.XA_RDONLY;
		try {
			lastVote = this.remoteTerminator.prepare(xid);
			this.transactionListenerList.prepareSuccess();
		} catch (XAException xaex) {
			this.transactionListenerList.prepareFailure();
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (RuntimeException xaex) {
			this.transactionListenerList.prepareFailure();
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
			this.transactionListenerList.commitStart();

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
				if (unFinishExists == false) {
					transactionCompleted = true;
					this.transactionListenerList.commitSuccess();
				} else {
					this.transactionListenerList.commitFailure();
				}
			} catch (TransactionException xaex) {
				this.transactionListenerList.commitFailure();
				CommitRequiredException ex = new CommitRequiredException();
				ex.initCause(xaex);
				throw ex;
			} catch (XAException xaex) {
				if (unFinishExists) {
					this.transactionListenerList.commitFailure();
					CommitRequiredException ex = new CommitRequiredException();
					ex.initCause(xaex);
					throw ex;
				} else if (mixedExists) {
					this.transactionListenerList.commitHeuristicMixed();
					transactionCompleted = true;
					throw new HeuristicMixedException();
				} else {
					transactionCompleted = true;

					switch (xaex.errorCode) {
					case XAException.XA_HEURMIX:
						this.transactionListenerList.commitHeuristicMixed();
						throw new HeuristicMixedException();
					case XAException.XA_HEURCOM:
						this.transactionListenerList.commitSuccess();
						break;
					case XAException.XA_HEURRB:
						if (firstVote == XAResource.XA_RDONLY) {
							this.transactionListenerList.commitHeuristicRolledback();
							throw new HeuristicRollbackException();
						} else {
							this.transactionListenerList.commitHeuristicMixed();
							throw new HeuristicMixedException();
						}
					default:
						this.transactionListenerList.commitFailure();
						logger.warn("Unknown state in committing transaction phase.");
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
			this.transactionListenerList.commitSuccess();
			transactionLogger.deleteTransaction(archive);
		}

	}

	public synchronized boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException,
			SystemException {
		if (this.transactionStatus != Status.STATUS_ACTIVE && this.transactionStatus != Status.STATUS_MARKED_ROLLBACK) {
			throw new IllegalStateException();
		}

		if (XAResourceDescriptor.class.isInstance(xaRes)) {
			XAResourceDescriptor descriptor = (XAResourceDescriptor) xaRes;
			if (CommonResourceDescriptor.class.isInstance(xaRes)) {
				return this.nativeTerminator.delistResource(descriptor, flag);
			} else if (RemoteResourceDescriptor.class.isInstance(xaRes)) {
				return this.remoteTerminator.delistResource(descriptor, flag);
			} else {
				return this.remoteTerminator.delistResource(descriptor, flag);
			}
		} else {
			UnidentifiedResourceDescriptor descriptor = new UnidentifiedResourceDescriptor(xaRes);
			return this.remoteTerminator.delistResource(descriptor, flag);
		}

	}

	public synchronized boolean enlistResource(XAResource xaRes) throws RollbackException, IllegalStateException,
			SystemException {

		if (this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackException();
		} else if (this.transactionStatus != Status.STATUS_ACTIVE) {
			throw new IllegalStateException();
		}

		if (XAResourceDescriptor.class.isInstance(xaRes)) {
			XAResourceDescriptor descriptor = (XAResourceDescriptor) xaRes;
			descriptor.setTransactionTimeoutQuietly(this.transactionTimeout);

			if (CommonResourceDescriptor.class.isInstance(xaRes)) {
				return this.nativeTerminator.enlistResource(descriptor);
			} else if (RemoteResourceDescriptor.class.isInstance(xaRes)) {
				return this.remoteTerminator.enlistResource(descriptor);
			} else {
				return this.remoteTerminator.enlistResource(descriptor);
			}
		} else {
			UnidentifiedResourceDescriptor descriptor = new UnidentifiedResourceDescriptor(xaRes);
			descriptor.setTransactionTimeoutQuietly(this.transactionTimeout);

			return this.remoteTerminator.enlistResource(descriptor);
		}

	}

	public int getStatus() /* throws SystemException */{
		return this.transactionStatus;
	}

	public synchronized void registerSynchronization(Synchronization sync) throws RollbackException,
			IllegalStateException, SystemException {

		if (this.transactionStatus == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackException();
		} else if (this.transactionStatus == Status.STATUS_ACTIVE) {
			this.synchronizationList.registerSynchronizationQuietly(sync);
			logger.debug(String.format("[%s] register-sync: sync= %s"//
					, ByteUtils.byteArrayToString(this.transactionContext.getXid().getGlobalTransactionId()), sync));
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
			// ignore
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

	private synchronized void delegateRollback() throws IllegalStateException, RollbackRequiredException,
			SystemException {
		// stop-timing
		TransactionTimer transactionTimer = beanFactory.getTransactionTimer();
		transactionTimer.stopTiming(this);

		// before-completion
		this.synchronizationList.beforeCompletion();

		// delist all resources
		this.delistAllResourceQuietly();

		try {
			TransactionXid xid = this.transactionContext.getXid();
			logger.info(String.format("[%s] rollback-transaction start",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));

			this.invokeRollback();

			logger.info(String.format("[%s] rollback-transaction complete successfully",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
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

		this.transactionListenerList.rollbackStart();

		TransactionLogger transactionLogger = beanFactory.getTransactionLogger();
		transactionLogger.createTransaction(archive);

		// rollback the native-resource
		try {
			this.nativeTerminator.rollback(xid);
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
			if (unFinishExists == false) {
				transactionCompleted = true;
				this.transactionListenerList.rollbackSuccess();
			} else {
				this.transactionListenerList.rollbackFailure();
			}
		} catch (TransactionException xaex) {
			this.transactionListenerList.rollbackFailure();
			RollbackRequiredException ex = new RollbackRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.transactionListenerList.rollbackFailure();
				RollbackRequiredException ex = new RollbackRequiredException();
				ex.initCause(xaex);
				throw ex;
			} else if (mixedExists) {
				this.transactionListenerList.rollbackFailure(); // TransactionListener.OPT_HEURMIX
				transactionCompleted = true;
				SystemException systemErr = new SystemException();
				systemErr.initCause(new XAException(XAException.XA_HEURMIX));
				throw systemErr;
			} else {
				transactionCompleted = true;

				switch (xaex.errorCode) {
				case XAException.XA_HEURRB:
					if (commitExists) {
						this.transactionListenerList.rollbackFailure(); // TransactionListener.OPT_HEURMIX
						SystemException systemErr = new SystemException();
						systemErr.initCause(new XAException(XAException.XA_HEURMIX));
						throw systemErr;
					}
					this.transactionListenerList.rollbackSuccess();
					break;
				case XAException.XA_HEURMIX:
					this.transactionListenerList.rollbackFailure(); // TransactionListener.OPT_HEURMIX
					SystemException systemErr = new SystemException();
					systemErr.initCause(new XAException(XAException.XA_HEURMIX));
					throw systemErr;
				case XAException.XA_HEURCOM:
					if (commitExists) {
						this.transactionListenerList.rollbackFailure(); // TransactionListener.OPT_HEURMIX
						SystemException committedErr = new SystemException();
						committedErr.initCause(new XAException(XAException.XA_HEURCOM));
						throw committedErr;
					} else {
						this.transactionListenerList.rollbackFailure(); // TransactionListener.OPT_HEURMIX
						SystemException mixedErr = new SystemException();
						mixedErr.initCause(new XAException(XAException.XA_HEURMIX));
						throw mixedErr;
					}
				default:
					this.transactionListenerList.rollbackFailure();
					logger.warn("Unknown state in rollingback transaction phase.");
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

	public synchronized void forgetQuietly() {
		TransactionXid globalXid = this.transactionContext.getXid();
		try {
			this.nativeTerminator.forget(globalXid);
		} catch (XAException ex) {
			// ignore
		}

		try {
			this.remoteTerminator.forget(globalXid);
		} catch (XAException ex) {
			// ignore
		}

		TransactionRepository repository = beanFactory.getTransactionRepository();
		repository.removeErrorTransaction(globalXid);
		repository.removeTransaction(globalXid);

		try {
			TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();
			transactionLogger.deleteTransaction(this.getTransactionArchive());
		} catch (RuntimeException ex) {
			// ignore
		}
	}

	public void setRollbackOnlyQuietly() {
		try {
			this.setRollbackOnly();
		} catch (Exception ignore) {
			// ignore
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
			// ignore
		}

		try {
			this.remoteTerminator.forget(xid);
		} catch (XAException ex) {
			// ignore
		}

	}

	public synchronized void recoveryInit() throws RollbackRequiredException, SystemException {
		RollbackRequiredException rollbackRequiredEx = null;
		SystemException systemEx = null;
		try {
			this.nativeTerminator.recover(this);
		} catch (RollbackRequiredException ex) {
			rollbackRequiredEx = ex;
		} catch (SystemException ex) {
			systemEx = ex;
		}

		this.remoteTerminator.recover(this);
		if (rollbackRequiredEx != null) {
			throw rollbackRequiredEx;
		} else if (systemEx != null) {
			throw systemEx;
		}
	}

	public synchronized void recoveryCommit() throws CommitRequiredException, SystemException {

		this.recoveryInit();

		TransactionXid xid = this.transactionContext.getXid();

		this.transactionStatus = Status.STATUS_COMMITTING;// .setStatusCommiting();
		boolean committedExists = false;
		boolean rolledbackExists = false;

		this.transactionListenerList.commitStart();

		boolean unFinishExists = false;
		try {
			this.nativeTerminator.commit(xid, false);
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
			this.remoteTerminator.commit(xid, false);
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
				this.transactionListenerList.commitFailure();
				throw new CommitRequiredException();
			} else {
				TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();
				TransactionLogger transactionLogger = beanFactory.getTransactionLogger();

				this.transactionStatus = Status.STATUS_COMMITTED;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);

				if (committedExists && rolledbackExists) {
					this.transactionListenerList.commitHeuristicMixed();
					logger.error(String.format("[%s] recovery-commit: committedExists= true, rolledbackExists= true",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
				} else if (rolledbackExists) {
					this.transactionListenerList.commitHeuristicRolledback();
					logger.info(String.format("[%s] recovery-commit: rolled back successfully",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
				} else {
					this.transactionListenerList.commitSuccess();
					logger.info(String.format("[%s] recovery-commit: committed successfully",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
				}
			}
		}

	}

	public synchronized void recoveryRollback() throws RollbackRequiredException, SystemException {

		this.recoveryInit();

		TransactionXid xid = this.transactionContext.getXid();

		this.transactionStatus = Status.STATUS_ROLLING_BACK;

		this.transactionListenerList.rollbackStart();

		boolean committedExists = false;
		boolean rolledbackExists = false;
		boolean unFinishExists = false;
		try {
			this.nativeTerminator.rollback(xid);
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
			this.remoteTerminator.rollback(xid);
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
				this.transactionListenerList.rollbackFailure();
				throw new RollbackRequiredException();
			} else {
				TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();
				TransactionLogger transactionLogger = beanFactory.getTransactionLogger();

				this.transactionStatus = Status.STATUS_ROLLEDBACK;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);

				if (committedExists && rolledbackExists) {
					this.transactionListenerList.rollbackFailure();
					logger.error(String.format("[%s] recovery-rollback: committedExists= true, rolledbackExists= true",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
				} else if (rolledbackExists) {
					this.transactionListenerList.rollbackSuccess();
					logger.info(String.format("[%s] recovery-rollback: rolled back successfully",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
				} else {
					this.transactionListenerList.rollbackFailure();
					logger.info(String.format("[%s] recovery-rollback: committed successfully",
							ByteUtils.byteArrayToString(xid.getGlobalTransactionId())));
				}
			}

		}

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

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
