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
package org.bytesoft.bytejta.resource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.supports.jdbc.RecoveredResource;
import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.LocalXAResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.UnidentifiedResourceDescriptor;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.internal.TransactionException;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.resource.XATerminator;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.supports.serialize.XAResourceDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XATerminatorOptd implements XATerminator {
	static final Logger logger = LoggerFactory.getLogger(XATerminatorOptd.class);

	private TransactionBeanFactory beanFactory;
	private XAResourceArchive archive;

	public synchronized int prepare(Xid xid) throws XAException {
		if (this.archive == null) {
			return XAResource.XA_RDONLY;
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		Xid branchXid = this.archive.getXid();
		int branchVote = this.archive.prepare(branchXid);
		if (branchVote == XAResource.XA_RDONLY) {
			this.archive.setReadonly(true);
			this.archive.setCompleted(true);
			this.archive.setVote(branchVote);
		} else {
			this.archive.setVote(branchVote);
		}

		transactionLogger.updateResource(this.archive);

		logger.info("[{}] prepare: xares= {}, branch= {}, vote= {}",
				ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
				ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), branchVote);

		return branchVote;
	}

	public synchronized void commit(Xid xid, boolean onePhase) throws TransactionException, XAException {
		if (this.archive == null) {
			return;
		} else if (onePhase) {
			this.fireOnePhaseCommit(xid);
		} else {
			this.fireTwoPhaseCommit(xid);
		}
	}

	private void fireOnePhaseCommit(Xid xid) throws XAException {
		try {
			Xid branchXid = this.archive.getXid();
			this.archive.commit(branchXid, true);
			this.archive.setCommitted(true);
			this.archive.setCompleted(true);

			logger.info("[{}] commit: xares= {}, branch= {}, opc= {}",
					ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), this.archive,
					ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), true);
		} catch (XAException xa) {
			switch (xa.errorCode) {
			case XAException.XA_RBCOMMFAIL:
			case XAException.XA_RBDEADLOCK:
			case XAException.XA_RBINTEGRITY:
			case XAException.XA_RBOTHER:
			case XAException.XA_RBPROTO:
			case XAException.XA_RBROLLBACK:
			case XAException.XA_RBTIMEOUT:
			case XAException.XA_RBTRANSIENT:
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
				throw new XAException(XAException.XA_RBROLLBACK);
			case XAException.XA_HEURCOM:
			case XAException.XA_HEURHAZ:
			case XAException.XA_HEURMIX:
			case XAException.XA_HEURRB:
			case XAException.XAER_RMERR:
				// should never happen
				logger.warn("An unexpected error occurred in one phase commit: code = " + xa.errorCode);
				break;
			case XAException.XAER_RMFAIL: {
				String txid = ByteUtils.byteArrayToString(xid.getGlobalTransactionId());
				logger.warn("An error occurred in one phase commit: txid = {}", txid);
				throw xa;
			}
			case XAException.XAER_NOTA:
			case XAException.XAER_INVAL:
			case XAException.XAER_PROTO: {
				String txid = ByteUtils.byteArrayToString(xid.getGlobalTransactionId());
				logger.warn("An error occurred in one phase commit: txid = {}", txid);
				throw xa;
			}
			} // end-switch (xa.errorCode)
		}
	}

	private void fireTwoPhaseCommit(Xid xid) throws XAException {
		Xid branchXid = this.archive.getXid();
		if (this.archive.isCommitted() || this.archive.isReadonly()) {
			return;
		} else if (this.archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURRB);
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();
		try {
			this.archive.commit(branchXid, false);
			this.archive.setCommitted(true);
			this.archive.setCompleted(true);

			logger.info("[%s] commit: xares= {}, branch= {}, onePhaseCommit= {}",
					ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), this.archive,
					ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), false);
		} catch (XAException xaex) {
			// * @exception XAException An error has occurred. Possible XAExceptions
			// * are XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR,
			// * XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
			// * <P>If the resource manager did not commit the transaction and the
			// * parameter onePhase is set to true, the resource manager may throw
			// * one of the XA_RB* exceptions. Upon return, the resource manager has
			// * rolled back the branch's work and has released all held resources.
			switch (xaex.errorCode) {
			case XAException.XA_HEURHAZ:
				// Due to some failure, the work done on behalf of the specified
				// transaction branch may have been heuristically completed.
				this.archive.setCommitted(true);
				// archive.setRolledback(true); // TODO
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURMIX:
				// Due to a heuristic decision, the work done on behalf of the specified
				// transaction branch was partially committed and partially rolled back.
				this.archive.setCommitted(true);
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURCOM:
				// Due to a heuristic decision, the work done on behalf of
				// the specified transaction branch was committed.
				this.archive.setCommitted(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURRB:
				// Due to a heuristic decision, the work done on behalf of
				// the specified transaction branch was rolled back.
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XAER_RMERR:
				// An error occurred in committing the work performed on behalf of the transaction
				// branch and the branch’s work has been rolled back. Note that returning this error
				// signals a catastrophic event to a transaction manager since other resource
				// managers may successfully commit their work on behalf of this branch. This error
				// should be returned only when a resource manager concludes that it can never
				// commit the branch and that it cannot hold the branch’s resources in a prepared
				// state. Otherwise, [XA_RETRY] should be returned.

				// TODO There's no XA_RETRY in jta.
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.
				int vote = this.archive.getVote();
				if (this.archive.isCompleted()) {
					// ignore
				} else if (vote == XAResource.XA_RDONLY) {
					this.archive.setCompleted(true);
					this.archive.setReadonly(true);
				} else if (vote == XAResource.XA_OK) {
					this.archive.setCommitted(true); // TODO
					this.archive.setCompleted(true);
				} else {
					// should never happen
					this.archive.setRolledback(true);
					this.archive.setCompleted(true);
				}
				break;
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
				break;
			default:// XA_RB*
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
			}
		} finally {
			transactionLogger.updateResource(this.archive);
		}

		// TODO throw exception

	}

	private void fireRecoveryPrepare(Xid xid) throws TransactionException, XAException {
		XAResourceDeserializer deserializer = this.beanFactory.getResourceDeserializer();
		XAResourceDescriptor descriptor = this.archive.getDescriptor();
		// LocalXAResourceDescriptor descriptor = (LocalXAResourceDescriptor) xardesc;

		XAResource oldResource = descriptor.getDelegate();
		if (RecoveredResource.class.isInstance(oldResource) == false) {
			XAResource newXAResource = deserializer.deserialize(descriptor.getIdentifier());
			if (CommonResourceDescriptor.class.isInstance(descriptor)) {
				((CommonResourceDescriptor) descriptor).setDelegate(newXAResource);
			} else if (LocalXAResourceDescriptor.class.isInstance(descriptor)) {
				((LocalXAResourceDescriptor) descriptor).setDelegate(newXAResource);
			} else if (RemoteResourceDescriptor.class.isInstance(descriptor)) {
				((RemoteResourceDescriptor) descriptor).setDelegate((RemoteCoordinator) newXAResource);
			} else if (UnidentifiedResourceDescriptor.class.isInstance(descriptor)) {
				((UnidentifiedResourceDescriptor) descriptor).setDelegate(newXAResource);
			}
		}

		try {
			if (descriptor.isTransactionCommitted(this.archive.getXid())) {
				this.archive.setCommitted(true);
				this.archive.setCompleted(true);
			} else {
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
			}
		} catch (IllegalStateException ex) {
			logger.warn("Error occurred while recovering transaction branch: {}", xid, ex);
		}
	}

	public synchronized void recoveryCommit(Xid xid) throws TransactionException, XAException {
		if (this.archive == null) {
			return;
		} else {
			this.fireRecoveryPrepare(xid);
			this.fireRecoveryCommit(xid);
		}
	}

	private void fireRecoveryCommit(Xid xid) throws TransactionException, XAException {
		if (this.archive.isCommitted() || this.archive.isReadonly()) {
			return;
		} else if (this.archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURRB);
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		Xid branchXid = this.archive.getXid();
		try {
			this.archive.recoveryCommit(branchXid);
			this.archive.setCommitted(true);
			this.archive.setCompleted(true);

			logger.info("[%s] commit: xares= {}, branch= {}, onePhaseCommit= {}",
					ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), this.archive,
					ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), false);
		} catch (XAException xaex) {
			// * @exception XAException An error has occurred. Possible XAExceptions
			// * are XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR,
			// * XAER_RMFAIL, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
			// * <P>If the resource manager did not commit the transaction and the
			// * parameter onePhase is set to true, the resource manager may throw
			// * one of the XA_RB* exceptions. Upon return, the resource manager has
			// * rolled back the branch's work and has released all held resources.
			switch (xaex.errorCode) {
			case XAException.XA_HEURHAZ:
				// Due to some failure, the work done on behalf of the specified
				// transaction branch may have been heuristically completed.
				this.archive.setCommitted(true);
				// this.archive.setRolledback(true); // TODO
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURMIX:
				// Due to a heuristic decision, the work done on behalf of the specified
				// transaction branch was partially committed and partially rolled back.
				this.archive.setCommitted(true);
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURCOM:
				// Due to a heuristic decision, the work done on behalf of
				// the specified transaction branch was committed.
				this.archive.setCommitted(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURRB:
				// Due to a heuristic decision, the work done on behalf of
				// the specified transaction branch was rolled back.
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XAER_RMERR:
				// An error occurred in committing the work performed on behalf of the transaction
				// branch and the branch’s work has been rolled back. Note that returning this error
				// signals a catastrophic event to a transaction manager since other resource
				// managers may successfully commit their work on behalf of this branch. This error
				// should be returned only when a resource manager concludes that it can never
				// commit the branch and that it cannot hold the branch’s resources in a prepared
				// state. Otherwise, [XA_RETRY] should be returned.

				// TODO There's no XA_RETRY in jta.
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.
				int vote = this.archive.getVote();
				if (this.archive.isCompleted()) {
					// ignore
				} else if (vote == XAResource.XA_RDONLY) {
					this.archive.setCompleted(true);
					this.archive.setReadonly(true);
				} else if (vote == XAResource.XA_OK) {
					this.archive.setCommitted(true); // TODO
					this.archive.setCompleted(true);
				} else {
					// should never happen
					this.archive.setRolledback(true);
					this.archive.setCompleted(true);
				}
				break;
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
				break;
			default:// XA_RB*
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
			}
		} finally {
			transactionLogger.updateResource(this.archive);
		}

		// TODO throw exception

	}

	public synchronized void rollback(Xid xid) throws TransactionException, XAException {
		if (this.archive == null) {
			return;
		} else if (this.archive.isCommitted()) {
			throw new XAException(XAException.XA_HEURCOM);
		} else if (archive.isRolledback() || this.archive.isReadonly()) {
			return;
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		Xid branchXid = this.archive.getXid();
		try {
			this.archive.rollback(branchXid);
			this.archive.setRolledback(true);
			this.archive.setCompleted(true);
			logger.info("[{}] rollback: xares= {}, branch= {}", ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()),
					this.archive, ByteUtils.byteArrayToString(branchXid.getBranchQualifier()));
		} catch (XAException xaex) {
			// * @exception XAException An error has occurred. Possible XAExceptions are
			// * XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR, XAER_RMFAIL,
			// * XAER_NOTA, XAER_INVAL, or XAER_PROTO.
			// * <p>If the transaction branch is already marked rollback-only the
			// * resource manager may throw one of the XA_RB* exceptions. Upon return,
			// * the resource manager has rolled back the branch's work and has released
			// * all held resources.
			switch (xaex.errorCode) {
			case XAException.XA_HEURHAZ:
				// Due to some failure, the work done on behalf of the specified transaction branch
				// may have been heuristically completed. A resource manager may return this
				// value only if it has successfully prepared xid.

				// archive.setCommitted(true); // TODO
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURMIX:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was partially committed and partially rolled back. A resource manager
				// may return this value only if it has successfully prepared xid.
				this.archive.setCommitted(true);
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURCOM:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was committed. A resource manager may return this value only if it has
				// successfully prepared xid.
				this.archive.setCommitted(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURRB:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was rolled back. A resource manager may return this value only if it has
				// successfully prepared xid.
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.
				int vote = this.archive.getVote();
				if (this.archive.isCompleted()) {
					// ignore
				} else if (vote == XAResource.XA_RDONLY) {
					this.archive.setCompleted(true);
					this.archive.setReadonly(true);
				} else if (vote == XAResource.XA_OK) {
					this.archive.setCommitted(true); // TODO
					this.archive.setCompleted(true);
				} else {
					this.archive.setRolledback(true);
					this.archive.setCompleted(true);
				}
				break;
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
			case XAException.XAER_RMERR:
				// An error occurred in rolling back the transaction branch. The resource manager is
				// free to forget about the branch when returning this error so long as all accessing
				// threads of control have been notified of the branch’s state.
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
				break;
			default:// XA_RB*
				// The resource manager has rolled back the transaction branch’s work and has
				// released all held resources. These values are typically returned when the
				// branch was already marked rollback-only.
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
			}
		} finally {
			transactionLogger.updateResource(this.archive);
		}

		// TODO throw exception

	}

	public synchronized void recoveryRollback(Xid xid) throws TransactionException, XAException {
		if (this.archive == null) {
			return;
		} else {
			this.fireRecoveryPrepare(xid);
			this.fireRecoveryRollback(xid);
		}
	}

	public void recoveryForget(Xid xid) throws XAException {
		this.forget(xid); // TODO
	}

	private void fireRecoveryRollback(Xid xid) throws TransactionException, XAException {
		if (this.archive.isCommitted()) {
			throw new XAException(XAException.XA_HEURCOM);
		} else if (archive.isRolledback() || this.archive.isReadonly()) {
			return;
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		Xid branchXid = this.archive.getXid();
		try {
			this.archive.recoveryRollback(branchXid);
			this.archive.setRolledback(true);
			this.archive.setCompleted(true);
			logger.info("[{}] rollback: xares= {}, branch= {}", ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()),
					this.archive, ByteUtils.byteArrayToString(branchXid.getBranchQualifier()));
		} catch (XAException xaex) {
			// * @exception XAException An error has occurred. Possible XAExceptions are
			// * XA_HEURHAZ, XA_HEURCOM, XA_HEURRB, XA_HEURMIX, XAER_RMERR, XAER_RMFAIL,
			// * XAER_NOTA, XAER_INVAL, or XAER_PROTO.
			// * <p>If the transaction branch is already marked rollback-only the
			// * resource manager may throw one of the XA_RB* exceptions. Upon return,
			// * the resource manager has rolled back the branch's work and has released
			// * all held resources.
			switch (xaex.errorCode) {
			case XAException.XA_HEURHAZ:
				// Due to some failure, the work done on behalf of the specified transaction branch
				// may have been heuristically completed. A resource manager may return this
				// value only if it has successfully prepared xid.

				// archive.setCommitted(true); // TODO
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURMIX:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was partially committed and partially rolled back. A resource manager
				// may return this value only if it has successfully prepared xid.
				this.archive.setCommitted(true);
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURCOM:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was committed. A resource manager may return this value only if it has
				// successfully prepared xid.
				this.archive.setCommitted(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XA_HEURRB:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was rolled back. A resource manager may return this value only if it has
				// successfully prepared xid.
				this.archive.setRolledback(true);
				this.archive.setHeuristic(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.

				int vote = this.archive.getVote();
				if (this.archive.isCompleted()) {
					// ignore
				} else if (vote == XAResource.XA_RDONLY) {
					this.archive.setCompleted(true);
					this.archive.setReadonly(true);
				} else if (vote == XAResource.XA_OK) {
					this.archive.setCommitted(true); // TODO
					this.archive.setCompleted(true);
				} else {
					this.archive.setRolledback(true);
					this.archive.setCompleted(true);
				}
				break;

			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
			case XAException.XAER_RMERR:
				// An error occurred in rolling back the transaction branch. The resource manager is
				// free to forget about the branch when returning this error so long as all accessing
				// threads of control have been notified of the branch’s state.
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
				break;
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
				break;
			default:// XA_RB*
				// The resource manager has rolled back the transaction branch’s work and has
				// released all held resources. These values are typically returned when the
				// branch was already marked rollback-only.
				this.archive.setRolledback(true);
				this.archive.setCompleted(true);
			}
		} finally {
			transactionLogger.updateResource(this.archive);
		}

		// TODO throw exception

	}

	public int getTransactionTimeout() throws XAException {
		throw new XAException(XAException.XAER_RMFAIL);
	}

	public boolean setTransactionTimeout(int seconds) throws XAException {
		throw new XAException(XAException.XAER_RMFAIL);
	}

	public void start(Xid xid, int flags) throws XAException {
		throw new XAException(XAException.XAER_RMFAIL);
	}

	public void end(Xid xid, int flags) throws XAException {
		throw new XAException(XAException.XAER_RMFAIL);
	}

	public boolean isSameRM(XAResource xares) throws XAException {
		throw new XAException(XAException.XAER_RMFAIL);
	}

	public Xid[] recover(int flag) throws XAException {
		throw new XAException(XAException.XAER_RMFAIL);
	}

	public void recover(Transaction transaction) throws SystemException {
		TransactionContext transactionContext = transaction.getTransactionContext();
		TransactionXid globalXid = transactionContext.getXid();
		int transactionStatus = transaction.getTransactionStatus();

		if (transactionStatus != Status.STATUS_PREPARING && transactionStatus != Status.STATUS_PREPARED
				&& transactionStatus != Status.STATUS_COMMITTING && transactionStatus != Status.STATUS_ROLLING_BACK) {
			return;
		}

		if (this.archive == null || this.archive.isRecovered() || this.archive.isReadonly() //
				|| this.archive.isCommitted() || this.archive.isRolledback()) {
			return;
		}

		boolean xidRecovered = false;
		if (this.archive.isIdentified()) {
			Xid thisXid = this.archive.getXid();
			byte[] thisGlobalTransactionId = thisXid.getGlobalTransactionId();
			byte[] thisBranchQualifier = thisXid.getBranchQualifier();
			try {
				Xid[] array = this.archive.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
				for (int j = 0; array != null && j < array.length; j++) {
					Xid thatXid = array[j];
					byte[] thatGlobalTransactionId = thatXid.getGlobalTransactionId();
					byte[] thatBranchQualifier = thatXid.getBranchQualifier();
					if (thisXid.getFormatId() == thatXid.getFormatId()
							&& Arrays.equals(thisGlobalTransactionId, thatGlobalTransactionId)
							&& Arrays.equals(thisBranchQualifier, thatBranchQualifier)) {
						xidRecovered = true;
						break;
					}
				}
			} catch (Exception ex) {
				logger.error("[{}] recover-transaction failed. branch= {}",
						ByteUtils.byteArrayToString(globalXid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(globalXid.getBranchQualifier()));
			}
		}

		if (transactionStatus == Status.STATUS_PREPARING) {
			this.recoverForPreparingTransaction(archive, xidRecovered);
		} else if (transactionStatus == Status.STATUS_PREPARED) {
			try {
				this.recoverForPreparedTransaction(archive, xidRecovered);
			} catch (IllegalStateException ex) {
				transactionStatus = Status.STATUS_PREPARING;
				transaction.setTransactionStatus(Status.STATUS_PREPARING);
			}
		} else if (transactionStatus == Status.STATUS_COMMITTING) {
			this.recoverForCommittingTransaction(archive, xidRecovered);
		} else if (transactionStatus == Status.STATUS_ROLLING_BACK) {
			this.recoverForRollingBackTransaction(archive, xidRecovered);
		}

		archive.setRecovered(true);
	}

	protected void recoverForPreparingTransaction(XAResourceArchive archive, boolean xidRecovered) {
		TransactionXid xid = (TransactionXid) archive.getXid();
		boolean branchPrepared = archive.getVote() != -1; // default value

		if (branchPrepared && xidRecovered) {
			// ignore
		} else if (branchPrepared && xidRecovered == false) {
			if (archive.isIdentified()) {
				logger.error("[{}] recover failed: branch= {}, status= preparing, branchPrepared= true, xidRecovered= false",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()));
			} else {
				// TODO
			}
		} else if (branchPrepared == false && xidRecovered) {
			archive.setVote(XAResource.XA_OK);
		} else if (branchPrepared == false && xidRecovered == false) {
			// rollback required
		}
	}

	protected void recoverForPreparedTransaction(XAResourceArchive archive, boolean xidRecovered) throws IllegalStateException {
		TransactionXid xid = (TransactionXid) archive.getXid();
		boolean branchPrepared = archive.getVote() != XAResourceArchive.DEFAULT_VOTE; // default value
		if (branchPrepared == false) {
			logger.error("[{}] recover failed: branch= {}, status= prepared, branchPrepared= false",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
					ByteUtils.byteArrayToString(xid.getBranchQualifier()));
			throw new IllegalStateException();
		} else if (xidRecovered == false) {
			if (archive.isIdentified()) {
				logger.error("[{}] recover failed: branch= {}, status= prepared, branchPrepared= true, xidRecovered= false",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()));
			} else {
				archive.setVote(XAResourceArchive.DEFAULT_VOTE); // vote of unidentified resource will be reset
				logger.error("[{}] recover failed: branch= {}, status= prepared, branchPrepared= true, identified= false",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()));
				// rollback required
				throw new IllegalStateException();
			}
		}
	}

	protected void recoverForCommittingTransaction(XAResourceArchive archive, boolean xidRecovered) {
		TransactionXid xid = (TransactionXid) archive.getXid();
		// boolean branchCompleted = archive.isCompleted();
		boolean branchCommitted = archive.isCommitted();
		if (branchCommitted && xidRecovered) {
			logger.warn("[{}] recover failed: branch= {}, status= committing, branchCommitted= true, xidRecovered= true",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
					ByteUtils.byteArrayToString(xid.getBranchQualifier()));
			archive.forgetQuietly(xid); // Branch has already been committed.
		} else if (branchCommitted && xidRecovered == false) {
			// ignore
		} else if (branchCommitted == false && xidRecovered) {
			// ignore
		} else if (branchCommitted == false && xidRecovered == false) {
			if (archive.isIdentified()) {
				logger.warn("[{}] recover failed: branch= {}, status= committing, branchCommitted= false, xidRecovered= false",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()));
				archive.setCommitted(true);
				archive.setCompleted(true);
			} else {
				// TODO upgrade
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setRolledback(true);
				archive.setCompleted(true);
			}
		}
	}

	protected void recoverForRollingBackTransaction(XAResourceArchive archive, boolean xidRecovered) {
		TransactionXid xid = (TransactionXid) archive.getXid();
		// boolean branchCompleted = archive.isCompleted();
		boolean branchRolledback = archive.isRolledback();
		if (branchRolledback && xidRecovered) {
			logger.warn("[{}] recover failed: branch= {}, status= rollingback, branchRolledback= true, xidRecovered= true",
					ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
					ByteUtils.byteArrayToString(xid.getBranchQualifier()));
			archive.forgetQuietly(xid); // Branch has already been committed.
		} else if (branchRolledback && xidRecovered == false) {
			// ignore
		} else if (branchRolledback == false && xidRecovered) {
			// ignore
		} else if (branchRolledback == false && xidRecovered == false) {
			if (archive.isIdentified()) {
				logger.warn(
						"[{}] recover failed: branch= {}, status= rollingback, branchRolledback= false, xidRecovered= false",
						ByteUtils.byteArrayToString(xid.getGlobalTransactionId()),
						ByteUtils.byteArrayToString(xid.getBranchQualifier()));
				archive.setRolledback(true);
				archive.setCompleted(true);
			} else {
				// TODO upgrade
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setRolledback(true);
				archive.setCompleted(true);
			}
		}
	}

	public void forget(Xid xid) throws XAException {
		if (this.archive == null) {
			return;
		}

		Xid currentXid = archive.getXid();
		if (archive.isHeuristic()) {
			try {
				Xid branchXid = archive.getXid();
				archive.forget(branchXid);
			} catch (XAException xae) {
				// Possible exception values are XAER_RMERR, XAER_RMFAIL
				// , XAER_NOTA, XAER_INVAL, or XAER_PROTO.
				switch (xae.errorCode) {
				case XAException.XAER_RMERR:
					logger.error("[{}] forget: xares= {}, branch={}, error= {}",
							ByteUtils.byteArrayToString(currentXid.getGlobalTransactionId()), archive,
							ByteUtils.byteArrayToString(currentXid.getBranchQualifier()), xae.errorCode);
					break;
				case XAException.XAER_RMFAIL:
					logger.error("[{}] forget: xares= {}, branch={}, error= {}",
							ByteUtils.byteArrayToString(currentXid.getGlobalTransactionId()), archive,
							ByteUtils.byteArrayToString(currentXid.getBranchQualifier()), xae.errorCode);
					break;
				case XAException.XAER_NOTA:
				case XAException.XAER_INVAL:
				case XAException.XAER_PROTO:
					break;
				default:
					logger.error("[{}] forget: xares= {}, branch={}, error= {}",
							ByteUtils.byteArrayToString(currentXid.getGlobalTransactionId()), archive,
							ByteUtils.byteArrayToString(currentXid.getBranchQualifier()), xae.errorCode);
				}
			}
		} // end-if
	}

	public List<XAResourceArchive> getResourceArchives() {
		return new List<XAResourceArchive>() {

			public int size() {
				return archive == null ? 0 : 1;
			}

			public boolean isEmpty() {
				return archive == null ? true : false;
			}

			public boolean contains(Object o) {
				throw new IllegalStateException("Not supported yet!");
			}

			public Iterator<XAResourceArchive> iterator() {
				throw new IllegalStateException("Not supported yet!");
			}

			public Object[] toArray() {
				return archive == null ? new Object[0] : new Object[] { archive };
			}

			public <T> T[] toArray(T[] a) {
				throw new IllegalStateException("Not supported yet!");
			}

			public boolean add(XAResourceArchive e) {
				if (XATerminatorOptd.this.archive != null && e != null) {
					throw new IllegalStateException("Not supported yet!");
				} else if (e != null) {
					XATerminatorOptd.this.archive = e;
					return true;
				} else {
					throw new IllegalStateException("Not supported yet!");
				}
			}

			public boolean remove(Object o) {
				throw new IllegalStateException("Not supported yet!");
			}

			public boolean containsAll(Collection<?> c) {
				throw new IllegalStateException("Not supported yet!");
			}

			public boolean addAll(Collection<? extends XAResourceArchive> c) {
				if (c == null || c.size() > 1) {
					throw new IllegalStateException("Not supported yet!");
				} else if (XATerminatorOptd.this.archive != null && c.isEmpty() == false) {
					throw new IllegalStateException("Not supported yet!");
				} else if (c.isEmpty() == false) {
					Object[] array = c.toArray();
					XATerminatorOptd.this.archive = (XAResourceArchive) array[0];
				}
				return true;
			}

			public boolean addAll(int index, Collection<? extends XAResourceArchive> c) {
				throw new IllegalStateException("Not supported yet!");
			}

			public boolean removeAll(Collection<?> c) {
				throw new IllegalStateException("Not supported yet!");
			}

			public boolean retainAll(Collection<?> c) {
				throw new IllegalStateException("Not supported yet!");
			}

			public void clear() {
				throw new IllegalStateException("Not supported yet!");
			}

			public XAResourceArchive get(int index) {
				if (index > 0 || index < 0) {
					throw new IndexOutOfBoundsException(String.format("index: %s, size: %s", index, this.size()));
				} else if (archive == null) {
					throw new IndexOutOfBoundsException(String.format("index: %s, size: 0", index));
				}
				return archive;
			}

			public XAResourceArchive set(int index, XAResourceArchive element) {
				throw new IllegalStateException("Not supported yet!");
			}

			public void add(int index, XAResourceArchive element) {
				throw new IllegalStateException("Not supported yet!");
			}

			public XAResourceArchive remove(int index) {
				throw new IllegalStateException("Not supported yet!");
			}

			public int indexOf(Object o) {
				throw new IllegalStateException("Not supported yet!");
			}

			public int lastIndexOf(Object o) {
				throw new IllegalStateException("Not supported yet!");
			}

			public ListIterator<XAResourceArchive> listIterator() {
				throw new IllegalStateException("Not supported yet!");
			}

			public ListIterator<XAResourceArchive> listIterator(int index) {
				throw new IllegalStateException("Not supported yet!");
			}

			public List<XAResourceArchive> subList(int fromIndex, int toIndex) {
				throw new IllegalStateException("Not supported yet!");
			}
		};
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
