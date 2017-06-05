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

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.logging.TransactionLogger;
import org.bytesoft.transaction.resource.XATerminator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XATerminatorImpl implements XATerminator {
	static final Logger logger = LoggerFactory.getLogger(XATerminatorImpl.class);

	private TransactionBeanFactory beanFactory;
	private final List<XAResourceArchive> resources = new ArrayList<XAResourceArchive>();

	public synchronized int prepare(Xid xid) throws XAException {
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		int globalVote = XAResource.XA_RDONLY;
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);

			boolean prepared = archive.getVote() != XAResourceArchive.DEFAULT_VOTE;
			if (prepared) {
				globalVote = archive.getVote() == XAResource.XA_RDONLY ? globalVote : XAResource.XA_OK;
			} else {
				int branchVote = archive.prepare(archive.getXid());
				archive.setVote(branchVote);

				if (branchVote == XAResource.XA_RDONLY) {
					archive.setReadonly(true);
					archive.setCompleted(true);
				} else {
					globalVote = XAResource.XA_OK;
				}

				transactionLogger.updateResource(archive);
			}

			logger.info("[{}] prepare: xares= {}, branch= {}, vote= {}",
					ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), archive.getVote());
		}

		return globalVote;
	}

	/** error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR */
	public synchronized void commit(Xid xid, boolean onePhase) throws XAException {
		if (onePhase) {
			this.fireOnePhaseCommit(xid);
		} else {
			this.fireTwoPhaseCommit(xid);
		}
	}

	private void fireOnePhaseCommit(Xid xid) throws XAException {

		if (this.resources.size() == 0) {
			throw new XAException(XAException.XA_RDONLY);
		} else if (this.resources.size() > 1) {
			this.rollback(xid);
			throw new XAException(XAException.XA_HEURRB);
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		XAResourceArchive archive = this.resources.get(0);

		if (archive.isCommitted() && archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURMIX);
		} else if (archive.isCommitted()) {
			return;
		} else if (archive.isReadonly()) {
			throw new XAException(XAException.XA_RDONLY);
		} else if (archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURRB);
		}

		boolean updateRequired = true;
		try {
			this.invokeOnePhaseCommit(archive);

			archive.setCommitted(true);
			archive.setCompleted(true);

			logger.info("[{}] commit: xares= {}, branch= {}, opc= {}",
					ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), true);
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURCOM:
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setCompleted(true);
				break;
			case XAException.XA_HEURHAZ:
				archive.setHeuristic(true);
				throw xaex;
			case XAException.XA_HEURMIX:
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setRolledback(true);
				archive.setCompleted(true);
				throw xaex;
			case XAException.XA_HEURRB:
				archive.setHeuristic(true);
				archive.setRolledback(true);
				archive.setCompleted(true);
				throw xaex;
			case XAException.XAER_RMFAIL:
				updateRequired = false;
				throw new XAException(XAException.XA_HEURHAZ);
			case XAException.XAER_RMERR:
			default:
				updateRequired = false;
				throw new XAException(XAException.XAER_RMERR);
			}
		} catch (RuntimeException rex) {
			logger.error("[{}] Error occurred while committing xa-resource: xares= {}, branch= {}",
					ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()));
			updateRequired = false;
			throw new XAException(XAException.XA_HEURHAZ);
		} finally {
			if (updateRequired) {
				transactionLogger.updateResource(archive);
			}
		}
	}

	private void fireTwoPhaseCommit(Xid xid) throws XAException {
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		boolean committedExists = false;
		boolean rolledbackExists = false;
		boolean unFinishExists = false;
		boolean errorExists = false;

		for (int i = this.resources.size() - 1; i >= 0; i--) {
			XAResourceArchive archive = this.resources.get(i);

			if (archive.isCommitted() && archive.isRolledback()) {
				committedExists = true;
				rolledbackExists = true;
				continue;
			} else if (archive.isCommitted()) {
				committedExists = true;
				continue;
			} else if (archive.isReadonly()) {
				continue;
			} else if (archive.isRolledback()) {
				rolledbackExists = true;
				continue;
			}

			Xid branchXid = archive.getXid();
			boolean updateRequired = true;
			try {
				this.invokeTwoPhaseCommit(archive);
				committedExists = true;
				archive.setCommitted(true);
				archive.setCompleted(true);
				logger.info("[{}] commit: xares= {}, branch= {}, onePhaseCommit= {}",
						ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
						ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), false);
			} catch (XAException xaex) {
				switch (xaex.errorCode) {
				case XAException.XA_HEURHAZ:
					archive.setHeuristic(true);
					unFinishExists = true;
					break;
				case XAException.XA_HEURMIX:
					committedExists = true;
					rolledbackExists = true;

					archive.setCommitted(true);
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURCOM:
					committedExists = true;
					archive.setCommitted(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURRB:
					rolledbackExists = true;
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XAER_RMFAIL:
					unFinishExists = true;
					updateRequired = false;
					break;
				case XAException.XA_RDONLY:
					archive.setReadonly(true);
					break;
				case XAException.XAER_RMERR:
				default:
					errorExists = true;
					updateRequired = false;
				}
			} catch (RuntimeException rex) {
				logger.error("[{}] Error occurred while committing xa-resource: xares= {}, branch= {}",
						ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
						ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()));
				unFinishExists = true;
				updateRequired = false;
			} finally {
				if (updateRequired) {
					transactionLogger.updateResource(archive);
				}
			}

		} // end-for

		if (committedExists && rolledbackExists) {
			throw new XAException(XAException.XA_HEURMIX);
		} else if (unFinishExists) {
			throw new XAException(XAException.XA_HEURHAZ);
		} else if (errorExists) {
			throw new XAException(XAException.XAER_RMERR);
		} else if (rolledbackExists) {
			throw new XAException(XAException.XA_HEURRB);
		} else if (committedExists == false) {
			throw new XAException(XAException.XA_RDONLY);
		}

	}

	private void invokeOnePhaseCommit(XAResourceArchive archive) throws XAException {
		try {
			archive.commit(archive.getXid(), true);
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURCOM:
			case XAException.XA_HEURHAZ:
			case XAException.XA_HEURMIX:
			case XAException.XA_HEURRB:
				throw xaex;
			case XAException.XAER_RMFAIL:
				logger.warn("An error occurred in one phase commit: {}",
						ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()));
				throw xaex;
			case XAException.XAER_NOTA:
			case XAException.XAER_INVAL:
			case XAException.XAER_PROTO:
				logger.warn("An error occurred in one phase commit: {}",
						ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()));
				throw new XAException(XAException.XAER_RMERR);
			case XAException.XAER_RMERR:
			case XAException.XA_RBCOMMFAIL:
			case XAException.XA_RBDEADLOCK:
			case XAException.XA_RBINTEGRITY:
			case XAException.XA_RBOTHER:
			case XAException.XA_RBPROTO:
			case XAException.XA_RBROLLBACK:
			case XAException.XA_RBTIMEOUT:
			case XAException.XA_RBTRANSIENT:
			default:
				throw new XAException(XAException.XA_HEURRB);
			}
		}
	}

	private void invokeTwoPhaseCommit(XAResourceArchive archive) throws XAException {
		try {
			archive.commit(archive.getXid(), false);
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
				// OSI-TP: The condition that arises when, as a result of communication failure with a
				// subordinate, the bound data of the subordinate's subtree are in an unknown state.

				// XA: Due to some failure, the work done on behalf of the specified
				// transaction branch may have been heuristically completed.
			case XAException.XA_HEURMIX:
				// Due to a heuristic decision, the work done on behalf of the specified
				// transaction branch was partially committed and partially rolled back.
			case XAException.XA_HEURCOM:
				// Due to a heuristic decision, the work done on behalf of
				// the specified transaction branch was committed.
			case XAException.XA_HEURRB:
				// Due to a heuristic decision, the work done on behalf of
				// the specified transaction branch was rolled back.
				throw xaex;
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.
				throw new XAException(XAException.XA_RDONLY); // read-only
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.
				throw xaex;
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
				throw new XAException(XAException.XAER_RMERR);
			case XAException.XAER_RMERR:
				// An error occurred in committing the work performed on behalf of the transaction
				// branch and the branch’s work has been rolled back. Note that returning this error
				// signals a catastrophic event to a transaction manager since other resource
				// managers may successfully commit their work on behalf of this branch. This error
				// should be returned only when a resource manager concludes that it can never
				// commit the branch and that it cannot hold the branch’s resources in a prepared
				// state. Otherwise, [XA_RETRY] should be returned.
			default:// XA_RB*
				throw new XAException(XAException.XA_HEURRB);
			}
		}
	}

	/** error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR */
	public synchronized void rollback(Xid xid) throws XAException {
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		boolean committedExists = false;
		boolean rolledbackExists = false;
		boolean unFinishExists = false;
		boolean errorExists = false;
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);

			if (archive.isCommitted() && archive.isRolledback()) {
				committedExists = true;
				rolledbackExists = true;
				continue;
			} else if (archive.isRolledback()) {
				rolledbackExists = true;
				continue;
			} else if (archive.isReadonly()) {
				continue;
			} else if (archive.isCommitted()) {
				committedExists = true;
				continue;
			}

			boolean updateRequired = true;
			try {
				this.invokeRollback(archive);
				rolledbackExists = true;
				archive.setRolledback(true);
				archive.setCompleted(true);
				logger.info("[{}] rollback: xares= {}, branch= {}",
						ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
						ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()));
			} catch (XAException xaex) {
				switch (xaex.errorCode) {
				case XAException.XA_HEURHAZ:
					unFinishExists = true;
					archive.setHeuristic(true);
					break;
				case XAException.XA_HEURMIX:
					committedExists = true;
					rolledbackExists = true;
					archive.setCommitted(true);
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURCOM:
					committedExists = true;
					archive.setCommitted(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURRB:
					rolledbackExists = true;
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_RDONLY:
					archive.setReadonly(true);
					archive.setCompleted(true);
					break;
				case XAException.XAER_RMFAIL:
					unFinishExists = true;
					updateRequired = false;
					break;
				case XAException.XAER_RMERR:
				default:
					errorExists = true;
					updateRequired = false;
				}
			} catch (RuntimeException rex) {
				unFinishExists = true;
				updateRequired = false;
				logger.error("[{}] Error occurred while rolling back xa-resource: xares= {}, branch= {}",
						ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
						ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()));
			} finally {
				if (updateRequired) {
					transactionLogger.updateResource(archive);
				}
			}
		}

		if (committedExists && rolledbackExists) {
			throw new XAException(XAException.XA_HEURMIX);
		} else if (unFinishExists) {
			throw new XAException(XAException.XA_HEURHAZ);
		} else if (errorExists) {
			throw new XAException(XAException.XAER_RMERR);
		} else if (committedExists) {
			throw new XAException(XAException.XA_HEURCOM);
		} else if (rolledbackExists == false) {
			throw new XAException(XAException.XA_RDONLY);
		}

	}

	private void invokeRollback(XAResourceArchive archive) throws XAException {
		try {
			archive.rollback(archive.getXid());
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
			case XAException.XA_HEURMIX:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was partially committed and partially rolled back. A resource manager
				// may return this value only if it has successfully prepared xid.
			case XAException.XA_HEURCOM:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was committed. A resource manager may return this value only if it has
				// successfully prepared xid.
			case XAException.XA_HEURRB:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was rolled back. A resource manager may return this value only if it has
				// successfully prepared xid.
				throw xaex;
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.
				throw new XAException(XAException.XA_HEURHAZ);
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.
				if (archive.isReadonly()) {
					throw new XAException(XAException.XA_RDONLY);
				} else if (archive.getVote() == XAResourceArchive.DEFAULT_VOTE) {
					break; // rolled back
				} else if (archive.getVote() == XAResource.XA_RDONLY) {
					throw new XAException(XAException.XA_RDONLY);
				} else if (archive.getVote() == XAResource.XA_OK) {
					throw new XAException(XAException.XAER_RMERR);
				} else {
					throw new XAException(XAException.XAER_RMERR);
				}
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
				throw new XAException(XAException.XAER_RMERR);
			case XAException.XAER_RMERR:
				// An error occurred in rolling back the transaction branch. The resource manager is
				// free to forget about the branch when returning this error so long as all accessing
				// threads of control have been notified of the branch’s state.
			default: // XA_RB*
				// The resource manager has rolled back the transaction branch’s work and has
				// released all held resources. These values are typically returned when the
				// branch was already marked rollback-only.
				throw new XAException(XAException.XA_HEURRB);
			}
		}
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

	public void forget(Xid xid) throws XAException {
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);
			Xid currentXid = archive.getXid();
			if (archive.isHeuristic() == false) {
				continue;
			}

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

		} // end-for
	}

	public List<XAResourceArchive> getResourceArchives() {
		return this.resources;
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
