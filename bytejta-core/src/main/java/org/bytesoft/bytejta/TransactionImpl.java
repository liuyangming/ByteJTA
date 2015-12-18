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
import java.util.List;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.apache.log4j.Logger;
import org.bytesoft.bytejta.common.TransactionConfigurator;
import org.bytesoft.bytejta.utils.ByteUtils;
import org.bytesoft.bytejta.utils.CommonUtils;
import org.bytesoft.bytejta.xa.XATerminatorImpl;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RemoteSystemException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.SynchronizationImpl;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionListener;
import org.bytesoft.transaction.TransactionTimer;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.logger.TransactionLogger;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XAInternalException;
import org.bytesoft.transaction.xa.XAResourceDescriptor;
import org.bytesoft.transaction.xa.XATerminator;

public class TransactionImpl implements Transaction {
	static final Logger logger = Logger.getLogger(TransactionImpl.class.getSimpleName());

	private transient boolean timing = true;

	private transient XATerminator firstTerminator;
	private transient XATerminator lastTerminator;

	private int transactionStatus;
	private final TransactionContext transactionContext;
	private final XATerminatorImpl nativeTerminator;
	private final XATerminatorImpl remoteTerminator;

	private final List<SynchronizationImpl> synchronizations = new ArrayList<SynchronizationImpl>();
	private final List<TransactionListener> listeners = new ArrayList<TransactionListener>();

	public TransactionImpl(TransactionContext txContext) {
		this.transactionContext = txContext;
		this.nativeTerminator = new XATerminatorImpl(this.transactionContext);
		this.remoteTerminator = new XATerminatorImpl(this.transactionContext);
	}

	private void beforeCompletion() {
		int length = this.synchronizations.size();
		for (int i = 0; i < length; i++) {
			SynchronizationImpl synchronization = this.synchronizations.get(i);
			try {
				synchronization.beforeCompletion();
			} catch (RuntimeException rex) {
				// ignore
			}
		}// end-for
	}

	private void afterCompletion() {
		int length = this.synchronizations.size();
		// int status = this.transactionStatus.getTransactionStatus();
		for (int i = 0; i < length; i++) {
			SynchronizationImpl synchronization = this.synchronizations.get(i);
			try {
				synchronization.afterCompletion(this.transactionStatus);
			} catch (RuntimeException rex) {
				// ignore
			}
		}// end-for
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

	private boolean currentOpcNecessary() {
		int nativeResNum = this.nativeTerminator.getResourceArchives().size();
		int remoteResNum = this.remoteTerminator.getResourceArchives().size();
		return (nativeResNum + remoteResNum) <= 1;
	}

	private synchronized void analysisTerminator() {

		if (this.transactionContext.isOptimized() == false) {

			// int nativeResNum = this.nativeTerminator.getResourceArchives().size();
			int remoteResNum = this.remoteTerminator.getResourceArchives().size();

			boolean nativeSupportsXA = false;
			boolean remoteSupportsXA = false;
			try {
				nativeSupportsXA = this.nativeTerminator.xaSupports();
			} catch (RemoteSystemException rsex) {
				nativeSupportsXA = false;
			}

			try {
				remoteSupportsXA = this.remoteTerminator.xaSupports();
			} catch (RemoteSystemException rsex) {
				remoteSupportsXA = false;
			}

			if (nativeSupportsXA && remoteSupportsXA) {
				if (remoteResNum > 0) {
					this.firstTerminator = this.nativeTerminator;
					this.lastTerminator = this.remoteTerminator;
				} else {
					this.firstTerminator = this.remoteTerminator;
					this.lastTerminator = this.nativeTerminator;
				}
			} else if (nativeSupportsXA) {
				this.firstTerminator = this.nativeTerminator;
				this.lastTerminator = this.remoteTerminator;
			} else {
				this.firstTerminator = this.remoteTerminator;
				this.lastTerminator = this.nativeTerminator;
			}

			this.transactionContext.setOptimized(true);

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
		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionTimer transactionTimer = transactionConfigurator.getTransactionTimer();
		transactionTimer.stopTiming(this);

		// before-completion
		this.beforeCompletion();

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

		this.analysisTerminator();

		TransactionXid xid = this.transactionContext.getGlobalXid();

		TransactionArchive archive = this.getTransactionArchive();

		int firstVote = XAResource.XA_RDONLY;
		TransactionLogger transactionLogger = transactionConfigurator.getTransactionLogger();
		try {
			this.transactionStatus = Status.STATUS_PREPARING;
			archive.setStatus(this.transactionStatus);
			transactionLogger.createTransaction(archive);

			this.firePrepareStart();

			firstVote = this.firstTerminator.prepare(xid);
		} catch (XAException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			throw new RollbackRequiredException();
		} catch (RuntimeException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			throw new RollbackRequiredException();
		}

		int lastVote = XAResource.XA_RDONLY;
		try {
			lastVote = this.lastTerminator.prepare(xid);
		} catch (XAException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			this.firePrepareComplete(false);
			throw new RollbackRequiredException();
		} catch (RuntimeException xaex) {
			this.transactionStatus = Status.STATUS_ROLLING_BACK;
			this.firePrepareComplete(false);
			throw new RollbackRequiredException();
		}

		this.transactionStatus = Status.STATUS_PREPARED;
		archive.setStatus(this.transactionStatus);
		this.firePrepareComplete(true);

		if (firstVote == XAResource.XA_OK || lastVote == XAResource.XA_OK) {
			this.transactionContext.setPrepareVote(XAResource.XA_OK);
			archive.setVote(XAResource.XA_OK);
			transactionLogger.updateTransaction(archive);
			throw new CommitRequiredException();
		} else {
			this.transactionContext.setPrepareVote(XAResource.XA_RDONLY);
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

		TransactionXid xid = this.transactionContext.getGlobalXid();
		TransactionArchive archive = this.getTransactionArchive();
		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionLogger transactionLogger = transactionConfigurator.getTransactionLogger();

		this.transactionStatus = Status.STATUS_COMMITTING;
		this.fireCommitStart();

		boolean mixedExists = false;
		boolean unFinishExists = false;
		try {
			firstTerminator.commit(xid, false);
		} catch (XAException xaex) {
			unFinishExists = XAInternalException.class.isInstance(xaex);

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
			lastTerminator.commit(xid, false);
			if (unFinishExists == false) {
				transactionCompleted = true;
				this.fireCommitSuccess();
			} else {
				this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
			}
		} catch (XAInternalException xaex) {
			this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
			CommitRequiredException ex = new CommitRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
				CommitRequiredException ex = new CommitRequiredException();
				ex.initCause(xaex);
				throw ex;
			} else if (mixedExists) {
				this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
				transactionCompleted = true;
				throw new HeuristicMixedException();
			} else {
				transactionCompleted = true;
				switch (xaex.errorCode) {
				case XAException.XA_HEURMIX:
					this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
					throw new HeuristicMixedException();
				case XAException.XA_HEURCOM:
					this.fireCommitSuccess();
					// this.fireCommitFailure(TransactionListener.OPT_HEURCOM);
					break;
				case XAException.XA_HEURRB:
					this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
					throw new HeuristicMixedException();
				default:
					this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
					logger.warn("Unknown state in committing transaction phase.");
				}
			}
		} finally {
			if (transactionCompleted) {
				this.transactionStatus = Status.STATUS_COMMITTED;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);
			}
		}

	}

	public synchronized void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException,
			SecurityException, IllegalStateException, CommitRequiredException, SystemException {

		try {
			this.checkBeforeCommit();
			return;
		} catch (RollbackRequiredException rrex) {
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (CommitRequiredException crex) {
			// ignore
		}

		// stop-timing
		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionTimer transactionTimer = transactionConfigurator.getTransactionTimer();
		transactionTimer.stopTiming(this);

		// before-completion
		this.beforeCompletion();

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
			if (this.currentOpcNecessary()) {
				this.opcCommit();
			} else {
				if (transactionConfigurator.isOptimizeEnabled()) {
					this.analysisTerminator();
					this.optimizeCommit();
				} else {
					this.regularCommit();
				}
			}
		} finally {
			this.afterCompletion();
		}

	}

	public synchronized void opcCommit() throws HeuristicRollbackException, HeuristicMixedException,
			CommitRequiredException, SystemException {
		TransactionXid xid = this.transactionContext.getGlobalXid();
		try {
			this.fireCommitStart();
			if (this.nativeTerminator.getResourceArchives().size() > 0) {
				this.nativeTerminator.commit(xid, true);
			} else {
				this.remoteTerminator.commit(xid, true);
			}
			this.fireCommitSuccess();
		} catch (XAInternalException xaex) {
			this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
			CommitRequiredException ex = new CommitRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURMIX:
				this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
				throw new HeuristicMixedException();
			case XAException.XA_HEURCOM:
				this.fireCommitSuccess();
				return;
			case XAException.XA_HEURRB:
				// this.fireRollbackStart();
				// this.fireRollbackSuccess();
				this.fireCommitFailure(TransactionListener.OPT_HEURRB);
				throw new HeuristicRollbackException();
			default:
				this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
				logger.warn("Unknown state in committing transaction phase.");
				SystemException ex = new SystemException();
				ex.initCause(xaex);
				throw ex;
			}
		}
	}

	public synchronized void regularCommit() throws HeuristicRollbackException, HeuristicMixedException,
			CommitRequiredException, SystemException {
		TransactionXid xid = this.transactionContext.getGlobalXid();

		TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();

		int firstVote = XAResource.XA_RDONLY;
		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionLogger transactionLogger = transactionConfigurator.getTransactionLogger();
		try {
			this.transactionStatus = Status.STATUS_PREPARING;// .setStatusPreparing();
			archive.setStatus(this.transactionStatus);
			transactionLogger.createTransaction(archive);

			this.firePrepareStart();

			firstVote = this.nativeTerminator.prepare(xid);
		} catch (XAException xaex) {
			this.firePrepareComplete(false);
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (RuntimeException xaex) {
			this.firePrepareComplete(false);
			this.rollback();
			throw new HeuristicRollbackException();
		}

		int lastVote = XAResource.XA_RDONLY;
		try {
			lastVote = this.remoteTerminator.prepare(xid);
			this.firePrepareComplete(true);
		} catch (XAException xaex) {
			this.firePrepareComplete(false);
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (RuntimeException xaex) {
			this.firePrepareComplete(false);
			this.rollback();
			throw new HeuristicRollbackException();
		}

		if (firstVote == XAResource.XA_OK || lastVote == XAResource.XA_OK) {
			this.transactionStatus = Status.STATUS_PREPARED;// .setStatusPrepared();
			this.transactionContext.setPrepareVote(XAResource.XA_OK);
			archive.setVote(XAResource.XA_OK);
			archive.setStatus(this.transactionStatus);
			transactionLogger.updateTransaction(archive);

			this.transactionStatus = Status.STATUS_COMMITTING;// .setStatusCommiting();
			this.fireCommitStart();

			boolean mixedExists = false;
			boolean unFinishExists = false;
			try {
				this.nativeTerminator.commit(xid, false);
			} catch (XAException xaex) {
				unFinishExists = XAInternalException.class.isInstance(xaex);

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
					this.fireCommitSuccess();
				} else {
					this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
				}
			} catch (XAInternalException xaex) {
				this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
				CommitRequiredException ex = new CommitRequiredException();
				ex.initCause(xaex);
				throw ex;
			} catch (XAException xaex) {
				if (unFinishExists) {
					this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
					CommitRequiredException ex = new CommitRequiredException();
					ex.initCause(xaex);
					throw ex;
				} else if (mixedExists) {
					this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
					transactionCompleted = true;
					throw new HeuristicMixedException();
				} else {
					transactionCompleted = true;

					switch (xaex.errorCode) {
					case XAException.XA_HEURMIX:
						this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
						throw new HeuristicMixedException();
					case XAException.XA_HEURCOM:
						this.fireCommitSuccess();
						break;
					case XAException.XA_HEURRB:
						if (firstVote == XAResource.XA_RDONLY) {
							// this.fireRollbackStart();
							// this.fireRollbackSuccess();
							this.fireCommitFailure(TransactionListener.OPT_HEURRB);
							throw new HeuristicRollbackException();
						} else {
							this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
							throw new HeuristicMixedException();
						}
					default:
						this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
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
			this.transactionContext.setPrepareVote(XAResource.XA_RDONLY);
			archive.setVote(XAResource.XA_RDONLY);
			archive.setStatus(this.transactionStatus);
			this.fireCommitSuccess();
			transactionLogger.deleteTransaction(archive);
		}

	}

	public synchronized void optimizeCommit() throws HeuristicRollbackException, HeuristicMixedException,
			CommitRequiredException, SystemException {
		TransactionXid xid = this.transactionContext.getGlobalXid();

		TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();

		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionLogger transactionLogger = transactionConfigurator.getTransactionLogger();
		try {
			this.transactionStatus = Status.STATUS_PREPARING;
			archive.setStatus(this.transactionStatus);
			transactionLogger.createTransaction(archive);
			this.firePrepareStart();

			this.firstTerminator.prepare(xid);
		} catch (XAException xaex) {
			this.firePrepareComplete(false);
			this.rollback();
			throw new HeuristicRollbackException();
		} catch (RuntimeException xaex) {
			this.firePrepareComplete(false);
			this.rollback();
			throw new HeuristicRollbackException();
		}

		boolean unFinishExists = false;
		boolean mixedExists = false;
		try {
			lastTerminator.commit(xid, true);
		} catch (XAException xaex) {
			unFinishExists = XAInternalException.class.isInstance(xaex);

			switch (xaex.errorCode) {
			case XAException.XA_HEURMIX:
				mixedExists = true;
				break;
			case XAException.XA_HEURCOM:
				break;
			case XAException.XA_HEURRB:
				this.rollback();
				throw new HeuristicRollbackException();
			default:
				logger.warn("Unknown state in committing transaction phase.");
			}
		}

		this.transactionStatus = Status.STATUS_COMMITTING;
		archive.setStatus(this.transactionStatus);
		archive.setVote(XAResource.XA_OK);
		this.transactionContext.setPrepareVote(XAResource.XA_OK);
		transactionLogger.updateTransaction(archive);

		this.fireCommitStart();

		boolean transactionCompleted = false;
		try {
			firstTerminator.commit(xid, false);
			if (unFinishExists == false) {
				transactionCompleted = true;
				this.fireCommitSuccess();
			} else {
				this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
			}
		} catch (XAInternalException xaex) {
			this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
			CommitRequiredException ex = new CommitRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
				CommitRequiredException ex = new CommitRequiredException();
				ex.initCause(xaex);
				throw ex;
			} else if (mixedExists) {
				this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
				transactionCompleted = true;
				throw new HeuristicMixedException();
			} else {
				transactionCompleted = true;

				switch (xaex.errorCode) {
				case XAException.XA_HEURMIX:
					this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
					throw new HeuristicMixedException();
				case XAException.XA_HEURCOM:
					this.fireCommitSuccess();
					break;
				case XAException.XA_HEURRB:
					this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
					throw new HeuristicMixedException();
				default:
					this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
					logger.warn("Unknown state in committing transaction phase.");
				}
			}
		} finally {
			if (transactionCompleted) {
				this.transactionStatus = Status.STATUS_COMMITTED;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);
			}
		}

	}

	public synchronized boolean delistResource(XAResource xaRes, int flag) throws IllegalStateException,
			SystemException {
		if (this.getStatus() != Status.STATUS_ACTIVE && this.getStatus() != Status.STATUS_MARKED_ROLLBACK) {
			throw new IllegalStateException();
		}

		XAResourceDescriptor descriptor = null;
		if (XAResourceDescriptor.class.isInstance(xaRes)) {
			descriptor = (XAResourceDescriptor) xaRes;
		} else {
			descriptor = this.recognizeResource(xaRes);
		}

		if (descriptor.isRemote()) {
			return this.remoteTerminator.delistResource(descriptor, flag);
		} else {
			return this.nativeTerminator.delistResource(descriptor, flag);
		}

	}

	private XAResourceDescriptor recognizeResource(XAResource xaRes) {
		XAResourceDescriptor descriptor;
		descriptor = new XAResourceDescriptor();
		descriptor.setDelegate(xaRes);
		descriptor.setRemote(false);
		descriptor.setSupportsXA(false);
		return descriptor;
	}

	public synchronized boolean enlistResource(XAResource xaRes) throws RollbackException, IllegalStateException,
			SystemException {

		if (this.getStatus() == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackException();
		} else if (this.getStatus() == Status.STATUS_ACTIVE) {
			XAResourceDescriptor descriptor = null;
			if (XAResourceDescriptor.class.isInstance(xaRes)) {
				descriptor = (XAResourceDescriptor) xaRes;
			} else {
				descriptor = this.recognizeResource(xaRes);
			}

			if (descriptor.isRemote()) {
				return this.enlistRemoteResource(descriptor);
			} else {
				return this.enlistNativeResource(descriptor);
			}
		} else {
			throw new IllegalStateException();
		}

	}

	public synchronized boolean enlistNativeResource(XAResourceDescriptor descriptor) throws RollbackException,
			IllegalStateException, SystemException {
		if (descriptor.isSupportsXA()) {
			return this.nativeTerminator.enlistResource(descriptor);
		} else if (this.transactionContext.isNonxaResourceAllowed()) {
			boolean remoteSupportsXA = false;
			try {
				remoteSupportsXA = this.remoteTerminator.xaSupports();
			} catch (RemoteSystemException rsex) {
				// logger.warn("Error occurred in enlist resource.");
				throw new SystemException("There already has a non-xa resource exists.");
			}
			if (remoteSupportsXA) {
				boolean enlisted = this.nativeTerminator.enlistResource(descriptor);
				this.transactionContext.setOptimized(true);
				this.transactionContext.setNonxaResourceAllowed(false);
				this.firstTerminator = this.remoteTerminator;
				this.lastTerminator = this.nativeTerminator;
				return enlisted;
			} else {
				throw new SystemException("There already has a non-xa resource exists.");
			}
		} else {
			throw new SystemException("Non-xa resource is not supported.");
		}
	}

	public synchronized boolean enlistRemoteResource(XAResourceDescriptor descriptor) throws RollbackException,
			IllegalStateException, SystemException {
		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		if (descriptor.isSupportsXA()) {
			return this.remoteTerminator.enlistResource(descriptor);
		} else if (transactionConfigurator.isOptimizeEnabled()) {
			boolean nativeSupportsXA = false;
			try {
				nativeSupportsXA = this.nativeTerminator.xaSupports();
			} catch (RemoteSystemException rsex) {
				// logger.warn("Error occurred in enlist resource.");
				throw new SystemException("There already has a non-xa resource exists.");
			}
			if (nativeSupportsXA) {
				boolean enlisted = this.remoteTerminator.enlistResource(descriptor);
				this.transactionContext.setOptimized(true);
				this.transactionContext.setNonxaResourceAllowed(false);
				this.firstTerminator = this.nativeTerminator;
				this.lastTerminator = this.remoteTerminator;
				return enlisted;
			} else {
				throw new SystemException("There already has a non-xa resource exists.");
			}
		} else {
			throw new SystemException("Non-xa resource is not supported.");
		}
	}

	public int getStatus() /* throws SystemException */{
		return this.transactionStatus;
	}

	public synchronized void registerSynchronization(Synchronization sync) throws RollbackException,
			IllegalStateException, SystemException {

		if (this.getStatus() == Status.STATUS_MARKED_ROLLBACK) {
			throw new RollbackException();
		} else if (this.getStatus() == Status.STATUS_ACTIVE) {
			SynchronizationImpl synchronization = new SynchronizationImpl(sync);
			this.synchronizations.add(synchronization);
			logger.info(String.format(
					"[%s] register-sync: sync= %s"//
					, ByteUtils.byteArrayToString(this.transactionContext.getCurrentXid().getGlobalTransactionId()),
					sync));
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
			return;
		} catch (RollbackRequiredException rrex) {
			// ignore
		}

		// stop-timing
		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionTimer transactionTimer = transactionConfigurator.getTransactionTimer();
		transactionTimer.stopTiming(this);

		// before-completion
		this.beforeCompletion();

		// delist all resources
		this.delistAllResourceQuietly();

		try {
			this.invokeRollback();
		} finally {
			this.afterCompletion();
		}

	}

	public synchronized void invokeRollback() throws IllegalStateException, RollbackRequiredException, SystemException {

		boolean unFinishExists = false;
		boolean commitExists = false;
		boolean mixedExists = false;
		boolean transactionCompleted = false;
		TransactionXid xid = this.transactionContext.getGlobalXid();

		TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();

		this.transactionStatus = Status.STATUS_ROLLING_BACK;
		archive.setStatus(this.transactionStatus);

		this.fireRollbackStart();

		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionLogger transactionLogger = transactionConfigurator.getTransactionLogger();
		transactionLogger.createTransaction(archive);

		// rollback the native-resource
		try {
			this.nativeTerminator.rollback(xid);
		} catch (XAException xaex) {
			unFinishExists = XAInternalException.class.isInstance(xaex);

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
				this.fireRollbackSuccess();
			} else {
				this.fireRollbackFailure(TransactionListener.OPT_DEFAULT);
			}
		} catch (XAInternalException xaex) {
			this.fireRollbackFailure(TransactionListener.OPT_DEFAULT);
			RollbackRequiredException ex = new RollbackRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.fireRollbackFailure(TransactionListener.OPT_DEFAULT);
				RollbackRequiredException ex = new RollbackRequiredException();
				ex.initCause(xaex);
				throw ex;
			} else if (mixedExists) {
				this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
				transactionCompleted = true;
				SystemException systemErr = new SystemException();
				systemErr.initCause(new XAException(XAException.XA_HEURMIX));
				throw systemErr;
			} else {
				transactionCompleted = true;

				switch (xaex.errorCode) {
				case XAException.XA_HEURRB:
					if (commitExists) {
						this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
						SystemException systemErr = new SystemException();
						systemErr.initCause(new XAException(XAException.XA_HEURMIX));
						throw systemErr;
					}
					this.fireRollbackSuccess();
					break;
				case XAException.XA_HEURMIX:
					this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
					SystemException systemErr = new SystemException();
					systemErr.initCause(new XAException(XAException.XA_HEURMIX));
					throw systemErr;
				case XAException.XA_HEURCOM:
					if (commitExists) {
						this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
						SystemException committedErr = new SystemException();
						committedErr.initCause(new XAException(XAException.XA_HEURCOM));
						throw committedErr;
					} else {
						this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
						SystemException mixedErr = new SystemException();
						mixedErr.initCause(new XAException(XAException.XA_HEURMIX));
						throw mixedErr;
					}
				default:
					this.fireRollbackFailure(TransactionListener.OPT_DEFAULT);
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

		TransactionXid xid = this.transactionContext.getGlobalXid();

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

	public synchronized void recoveryRollback() throws RollbackRequiredException, SystemException {

		TransactionXid xid = this.transactionContext.getGlobalXid();

		boolean optimized = this.transactionContext.isOptimized();
		boolean committedExists = optimized && this.transactionStatus == Status.STATUS_COMMITTING;
		boolean rolledbackExists = false;
		this.transactionStatus = Status.STATUS_ROLLING_BACK;

		this.fireRollbackStart();

		boolean unFinishExists = false;
		boolean transactionCompleted = false;
		try {
			this.nativeTerminator.rollback(xid);
			rolledbackExists = true;
		} catch (XAException xaex) {
			unFinishExists = XAInternalException.class.isInstance(xaex);

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
				logger.warn("Unknown state in recovery-rollingback phase.");
			}
		}

		try {
			this.remoteTerminator.rollback(xid);
			rolledbackExists = true;
			if (unFinishExists == false) {
				transactionCompleted = true;
				this.fireRollbackSuccess();
			} else {
				this.fireRollbackFailure(TransactionListener.OPT_DEFAULT);
			}
		} catch (XAInternalException xaex) {
			this.fireRollbackFailure(TransactionListener.OPT_DEFAULT);
			RollbackRequiredException rrex = new RollbackRequiredException();
			rrex.initCause(xaex);
			throw rrex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.fireRollbackFailure(TransactionListener.OPT_DEFAULT);
				RollbackRequiredException rrex = new RollbackRequiredException();
				rrex.initCause(xaex);
				throw rrex;
			} else if (committedExists && rolledbackExists) {
				this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
				transactionCompleted = true;
				SystemException ex = new SystemException(XAException.XA_HEURMIX);
				ex.initCause(xaex);
				throw ex;
			} else {
				transactionCompleted = true;

				switch (xaex.errorCode) {
				case XAException.XA_HEURMIX:
					this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
					SystemException ex = new SystemException(XAException.XA_HEURMIX);
					ex.initCause(xaex);
					throw ex;
				case XAException.XA_HEURCOM:
					if (rolledbackExists) {
						this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
						SystemException mixedErr = new SystemException(XAException.XA_HEURMIX);
						mixedErr.initCause(xaex);
						throw mixedErr;
					} else {
						this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
						SystemException committedErr = new SystemException(XAException.XA_HEURCOM);
						committedErr.initCause(xaex);
						throw committedErr;
					}
				case XAException.XA_HEURRB:
					if (committedExists) {
						this.fireRollbackFailure(TransactionListener.OPT_HEURMIX);
						SystemException mixedErr = new SystemException(XAException.XA_HEURMIX);
						mixedErr.initCause(xaex);
						throw mixedErr;
					}
					this.fireRollbackSuccess();
					break;
				default:
					this.fireRollbackFailure(TransactionListener.OPT_DEFAULT);
					logger.warn("Unknown state in recovery-committing phase.");
				}
			}
		} finally {

			if (transactionCompleted) {
				TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();
				TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
				TransactionLogger transactionLogger = transactionConfigurator.getTransactionLogger();

				this.transactionStatus = Status.STATUS_ROLLEDBACK;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);
			}

		}

	}

	public synchronized void recoveryCommit() throws HeuristicMixedException, CommitRequiredException, SystemException {

		TransactionXid xid = this.transactionContext.getGlobalXid();

		this.transactionStatus = Status.STATUS_COMMITTING;// .setStatusCommiting();
		// boolean nativeReadOnly = this.nativeTerminator.checkReadOnlyForRecovery();
		// boolean remoteReadOnly = this.remoteTerminator.checkReadOnlyForRecovery();
		boolean committedExists = this.transactionContext.isOptimized();
		boolean rolledbackExists = false;

		this.fireCommitStart();

		boolean unFinishExists = false;
		boolean transactionCompleted = false;
		try {
			this.nativeTerminator.commit(xid, false);
			committedExists = true;
		} catch (XAException xaex) {
			unFinishExists = XAInternalException.class.isInstance(xaex);

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
				logger.warn("Unknown state in recovery-committing phase.");
			}
		}

		try {
			this.remoteTerminator.commit(xid, false);
			committedExists = true;
			if (unFinishExists == false) {
				transactionCompleted = true;
				this.fireCommitSuccess();
			} else {
				this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
			}
		} catch (XAInternalException xaex) {
			this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
			CommitRequiredException ex = new CommitRequiredException();
			ex.initCause(xaex);
			throw ex;
		} catch (XAException xaex) {
			if (unFinishExists) {
				this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
				CommitRequiredException ex = new CommitRequiredException();
				ex.initCause(xaex);
				throw ex;
			} else if (committedExists && rolledbackExists) {
				this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
				transactionCompleted = true;
				throw new HeuristicMixedException();
			} else {
				transactionCompleted = true;

				switch (xaex.errorCode) {
				case XAException.XA_HEURMIX:
					// committedExists = true;
					// rolledbackExists = true;
					this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
					break;
				case XAException.XA_HEURCOM:
					// committedExists = true;
					if (rolledbackExists) {
						this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
					} else {
						this.fireCommitFailure(TransactionListener.OPT_HEURCOM);
					}
					break;
				case XAException.XA_HEURRB:
					// rolledbackExists = true;
					if (committedExists) {
						this.fireCommitFailure(TransactionListener.OPT_HEURMIX);
					} else {
						this.fireCommitFailure(TransactionListener.OPT_HEURRB);
					}
					break;
				default:
					this.fireCommitFailure(TransactionListener.OPT_DEFAULT);
					logger.warn("Unknown state in recovery-committing phase.");
				}
			}
		} finally {

			if (transactionCompleted) {
				TransactionArchive archive = this.getTransactionArchive();// new TransactionArchive();

				TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
				TransactionLogger transactionLogger = transactionConfigurator.getTransactionLogger();

				this.transactionStatus = Status.STATUS_COMMITTED;
				archive.setStatus(this.transactionStatus);
				transactionLogger.deleteTransaction(archive);
			}

		}

	}

	public TransactionArchive getTransactionArchive() {
		TransactionArchive transactionArchive = new TransactionArchive();
		transactionArchive.setOptimized(this.transactionContext.isOptimized());
		transactionArchive.setVote(this.transactionContext.getPrepareVote());
		transactionArchive.setXid(this.transactionContext.getGlobalXid());
		transactionArchive.setCompensable(this.transactionContext.isCompensable());
		transactionArchive.setCoordinator(this.transactionContext.isCoordinator());
		transactionArchive.getNativeResources().addAll(this.nativeTerminator.getResourceArchives());
		transactionArchive.getRemoteResources().addAll(this.remoteTerminator.getResourceArchives());
		transactionArchive.setStatus(this.transactionStatus);
		return transactionArchive;
	}

	public void firePrepareStart() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.prepareStart();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void firePrepareComplete(boolean success) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.prepareComplete(success);
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void fireCommitStart() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.commitStart();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void fireCommitSuccess() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.commitSuccess();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void fireCommitFailure(int optcode) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.commitFailure(optcode);
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void fireRollbackStart() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.rollbackStart();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void fireRollbackSuccess() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.rollbackSuccess();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void fireRollbackFailure(int optcode) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.rollbackFailure(optcode);
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public int hashCode() {
		TransactionXid transactionXid = this.transactionContext == null ? null : this.transactionContext.getGlobalXid();
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
		TransactionXid thisXid = thisContext == null ? null : thisContext.getGlobalXid();
		TransactionXid thatXid = thatContext == null ? null : thatContext.getGlobalXid();
		return CommonUtils.equals(thisXid, thatXid);
	}

	public void registerTransactionListener(TransactionListener listener) {
		this.listeners.add(listener);
	}

	public TransactionContext getTransactionContext() {
		return transactionContext;
	}

	public XATerminatorImpl getNativeTerminator() {
		return nativeTerminator;
	}

	public XATerminatorImpl getRemoteTerminator() {
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

}
