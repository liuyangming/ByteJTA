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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.supports.jdbc.RecoveredResource;
import org.bytesoft.bytejta.supports.resource.LocalXAResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.UnidentifiedResourceDescriptor;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.internal.TransactionException;
import org.bytesoft.transaction.resource.XATerminator;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.supports.serialize.XAResourceDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XATerminatorImpl implements XATerminator, Comparator<XAResourceArchive> {
	static final Logger logger = LoggerFactory.getLogger(XATerminatorImpl.class.getSimpleName());
	private TransactionContext transactionContext;
	private int transactionTimeout;
	private TransactionBeanFactory beanFactory;
	private final List<XAResourceArchive> resources = new ArrayList<XAResourceArchive>();

	public XATerminatorImpl(TransactionContext txContext) {
		this.transactionContext = txContext;
	}

	public void beforeCompletion() {
		Collections.sort(this.resources, this);
	}

	public void afterCompletion(int status) {
	}

	public int compare(XAResourceArchive xa1, XAResourceArchive xa2) {
		if (xa1 == null || xa1.getDescriptor() == null) {
			return 1;
		} else if (UnidentifiedResourceDescriptor.class.isInstance(xa1.getDescriptor())) {
			return 1;
		} else {
			return -1;
		}
	}

	public synchronized int prepare(Xid xid) throws XAException {
		int globalVote = XAResource.XA_RDONLY;
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);
			Xid branchXid = archive.getXid();
			int branchVote = archive.prepare(branchXid);
			if (branchVote == XAResource.XA_RDONLY) {
				archive.setReadonly(true);
				archive.setCompleted(true);
			} else {
				globalVote = XAResource.XA_OK;
			}
			logger.info("[{}] prepare: xares= {}, branch= {}, vote= {}",
					ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), branchVote);
		}
		return globalVote;

	}

	public synchronized void commit(Xid xid, boolean onePhase) throws TransactionException, XAException {
		if (onePhase) {
			this.fireOnePhaseCommit(xid);
		} else {
			this.fireTwoPhaseCommit(xid);
		}
	}

	private void fireOnePhaseCommit(Xid xid) throws XAException {
		XAResourceArchive archive = this.resources.get(0);
		try {
			Xid branchXid = archive.getXid();
			archive.commit(branchXid, true);
			archive.setCommitted(true);
			archive.setCompleted(true);

			logger.info("[{}] commit: xares= {}, branch= {}, opc= {}",
					ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
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
				archive.setRolledback(true);
				archive.setCompleted(true);
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
				String txid = ByteUtils.byteArrayToString(this.transactionContext.getXid().getGlobalTransactionId());
				logger.warn("An error occurred in one phase commit: txid = {}", txid);
				throw xa;
			}
			case XAException.XAER_NOTA:
			case XAException.XAER_INVAL:
			case XAException.XAER_PROTO: {
				String txid = ByteUtils.byteArrayToString(this.transactionContext.getXid().getGlobalTransactionId());
				logger.warn("An error occurred in one phase commit: txid = {}", txid);
				throw xa;
			}
			}
		}
	}

	private void throwCommitExceptionIfNecessary(boolean commitExists, boolean rollbackExists) throws XAException {
		if (commitExists && rollbackExists) {
			throw new XAException(XAException.XA_HEURMIX);
		} else if (rollbackExists) {
			throw new XAException(XAException.XA_HEURRB);
		}
	}

	private void fireTwoPhaseCommit(Xid xid) throws XAException {
		boolean commitExists = false;
		boolean rollbackExists = false;
		boolean unFinishExists = false;
		for (int i = this.resources.size() - 1; i >= 0; i--) {
			XAResourceArchive archive = this.resources.get(i);
			Xid branchXid = archive.getXid();
			try {
				if (archive.isCompleted()) {
					if (archive.isCommitted()) {
						commitExists = true;
					} else if (archive.isRolledback()) {
						rollbackExists = true;
					} else {
						// read-only, ignore.
					}
				} else {
					archive.commit(branchXid, false);
					commitExists = true;
					archive.setCommitted(true);
					archive.setCompleted(true);
					logger.info("[%s] commit: xares= {}, branch= {}, onePhaseCommit= {}",
							ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
							ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), false);
				}
			} catch (XAException xaex) {
				if (commitExists) {
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
						commitExists = true;
						// rollbackExists = true;
						archive.setCommitted(true);
						// archive.setRolledback(true); // TODO
						archive.setHeuristic(true);
						archive.setCompleted(true);
						break;
					case XAException.XA_HEURMIX:
						// Due to a heuristic decision, the work done on behalf of the specified
						// transaction branch was partially committed and partially rolled back.
						commitExists = true;
						rollbackExists = true;
						archive.setCommitted(true);
						archive.setRolledback(true);
						archive.setHeuristic(true);
						archive.setCompleted(true);
						break;
					case XAException.XA_HEURCOM:
						// Due to a heuristic decision, the work done on behalf of
						// the specified transaction branch was committed.
						commitExists = true;
						archive.setCommitted(true);
						archive.setHeuristic(true);
						archive.setCompleted(true);
						break;
					case XAException.XA_HEURRB:
						// Due to a heuristic decision, the work done on behalf of
						// the specified transaction branch was rolled back.
						rollbackExists = true;
						archive.setRolledback(true);
						archive.setHeuristic(true);
						archive.setCompleted(true);
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
						rollbackExists = true;
						archive.setRolledback(true);
						archive.setCompleted(true);
						break;
					case XAException.XAER_NOTA:
						// The specified XID is not known by the resource manager.
						if (archive.isCompleted() == false) {
							if (archive.isCommitted()) {
								commitExists = true;
							}
							if (archive.isRolledback()) {
								rollbackExists = true;
							}
						} else {
							int vote = archive.getVote();
							if (vote == XAResource.XA_RDONLY) {
								archive.setCompleted(true);
							} else if (vote == XAResource.XA_OK) {
								commitExists = true;
								archive.setCommitted(true);
								archive.setCompleted(true);
							} else {
								// should never happen
								rollbackExists = true;
								archive.setRolledback(true);
								archive.setCompleted(true);
							}
						}
						break;
					case XAException.XAER_RMFAIL:
						// An error occurred that makes the resource manager unavailable.
					case XAException.XAER_INVAL:
						// Invalid arguments were specified.
					case XAException.XAER_PROTO:
						// The routine was invoked in an improper context.
						unFinishExists = true;
						break;
					default:// XA_RB*
						rollbackExists = true;
						archive.setRolledback(true);
						archive.setCompleted(true);
						rollbackExists = true;
					}
				} else {
					try {
						this.rollback(xid);
						throw new XAException(XAException.XA_HEURRB);
					} catch (XAException xae) {
						switch (xae.errorCode) {
						case XAException.XA_HEURCOM:
							return;
						case XAException.XA_HEURMIX:
							throw xae;
						default:
							logger.warn("Unknown state in committing transaction phase.");
						}
					}
				}
			}
		} // end-for

		try {
			this.throwCommitExceptionIfNecessary(commitExists, rollbackExists);
		} catch (XAException xae) {
			if (unFinishExists) {
				throw new TransactionException(xae.errorCode);
			} else {
				throw xae;
			}
		}

	}

	private void fireRecoveryPrepare(Xid xid) throws TransactionException, XAException {
		XAResourceDeserializer deserializer = this.beanFactory.getResourceDeserializer();
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);
			XAResourceDescriptor xardesc = archive.getDescriptor();
			if (LocalXAResourceDescriptor.class.isInstance(xardesc) == false) {
				continue;
			}
			LocalXAResourceDescriptor descriptor = (LocalXAResourceDescriptor) xardesc;

			XAResource oldResource = descriptor.getDelegate();
			if (RecoveredResource.class.isInstance(oldResource) == false) {
				XAResource newXAResource = deserializer.deserialize(descriptor.getIdentifier());
				descriptor.setDelegate(newXAResource);
			}

			try {
				if (descriptor.isTransactionCommitted(archive.getXid())) {
					archive.setCommitted(true);
					archive.setCompleted(true);
				} else {
					archive.setRolledback(true);
					archive.setCompleted(true);
				}
			} catch (IllegalStateException ex) {
				logger.warn("Error occurred while recovering transaction branch: {}", xid, ex);
			}
		}
	}

	public synchronized void recoveryCommit(Xid xid) throws TransactionException, XAException {
		this.fireRecoveryPrepare(xid);

		this.fireRecoveryCommit(xid);
	}

	private void fireRecoveryCommit(Xid xid) throws TransactionException, XAException {
		boolean commitExists = false;
		boolean rollbackExists = false;
		boolean unFinishExists = false;
		for (int i = this.resources.size() - 1; i >= 0; i--) {
			XAResourceArchive archive = this.resources.get(i);
			Xid branchXid = archive.getXid();
			try {
				if (archive.isCompleted()) {
					if (archive.isCommitted()) {
						commitExists = true;
					} else if (archive.isRolledback()) {
						rollbackExists = true;
					} else {
						// read-only, ignore.
					}
				} else {
					archive.recoveryCommit(branchXid);
					commitExists = true;
					archive.setCommitted(true);
					archive.setCompleted(true);
					logger.info("[%s] commit: xares= {}, branch= {}, onePhaseCommit= {}",
							ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
							ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), false);
				}
			} catch (XAException xaex) {
				if (commitExists) {
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
						commitExists = true;
						// rollbackExists = true;
						archive.setCommitted(true);
						// archive.setRolledback(true); // TODO
						archive.setHeuristic(true);
						archive.setCompleted(true);
						break;
					case XAException.XA_HEURMIX:
						// Due to a heuristic decision, the work done on behalf of the specified
						// transaction branch was partially committed and partially rolled back.
						commitExists = true;
						rollbackExists = true;
						archive.setCommitted(true);
						archive.setRolledback(true);
						archive.setHeuristic(true);
						archive.setCompleted(true);
						break;
					case XAException.XA_HEURCOM:
						// Due to a heuristic decision, the work done on behalf of
						// the specified transaction branch was committed.
						commitExists = true;
						archive.setCommitted(true);
						archive.setHeuristic(true);
						archive.setCompleted(true);
						break;
					case XAException.XA_HEURRB:
						// Due to a heuristic decision, the work done on behalf of
						// the specified transaction branch was rolled back.
						rollbackExists = true;
						archive.setRolledback(true);
						archive.setHeuristic(true);
						archive.setCompleted(true);
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
						rollbackExists = true;
						archive.setRolledback(true);
						archive.setCompleted(true);
						break;
					case XAException.XAER_NOTA:
						// The specified XID is not known by the resource manager.
						if (archive.isCompleted() == false) {
							if (archive.isCommitted()) {
								commitExists = true;
							}
							if (archive.isRolledback()) {
								rollbackExists = true;
							}
						} else {
							int vote = archive.getVote();
							if (vote == XAResource.XA_RDONLY) {
								archive.setCompleted(true);
							} else if (vote == XAResource.XA_OK) {
								commitExists = true;
								archive.setCommitted(true);
								archive.setCompleted(true);
							} else {
								// should never happen
								rollbackExists = true;
								archive.setRolledback(true);
								archive.setCompleted(true);
							}
						}
						break;
					case XAException.XAER_RMFAIL:
						// An error occurred that makes the resource manager unavailable.
					case XAException.XAER_INVAL:
						// Invalid arguments were specified.
					case XAException.XAER_PROTO:
						// The routine was invoked in an improper context.
						unFinishExists = true;
						break;
					default:// XA_RB*
						rollbackExists = true;
						archive.setRolledback(true);
						archive.setCompleted(true);
						rollbackExists = true;
					}
				} else {
					try {
						this.rollback(xid);
						throw new XAException(XAException.XA_HEURRB);
					} catch (XAException xae) {
						switch (xae.errorCode) {
						case XAException.XA_HEURCOM:
							return;
						case XAException.XA_HEURMIX:
							throw xae;
						default:
							logger.warn("Unknown state in committing transaction phase.");
						}
					}
				}
			}
		} // end-for

		try {
			this.throwCommitExceptionIfNecessary(commitExists, rollbackExists);
		} catch (XAException xae) {
			if (unFinishExists) {
				throw new TransactionException(xae.errorCode);
			} else {
				throw xae;
			}
		}

	}

	public synchronized void rollback(Xid xid) throws TransactionException, XAException {
		boolean commitExists = false;
		boolean rollbackExists = false;
		boolean unFinishExists = false;
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);
			Xid branchXid = archive.getXid();
			try {
				if (archive.isCompleted()) {
					if (archive.isCommitted()) {
						commitExists = true;
					} else if (archive.isRolledback()) {
						rollbackExists = true;
					} else {
						// read-only, ignore.
					}
				} else {
					archive.rollback(branchXid);
					rollbackExists = true;
					archive.setRolledback(true);
					archive.setCompleted(true);
					logger.info("[{}] rollback: xares= {}, branch= {}",
							ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
							ByteUtils.byteArrayToString(branchXid.getBranchQualifier()));
				}
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

					// commitExists = true;
					rollbackExists = true;
					// archive.setCommitted(true); // TODO
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURMIX:
					// Due to a heuristic decision, the work done on behalf of the specified transaction
					// branch was partially committed and partially rolled back. A resource manager
					// may return this value only if it has successfully prepared xid.
					commitExists = true;
					rollbackExists = true;
					archive.setCommitted(true);
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURCOM:
					// Due to a heuristic decision, the work done on behalf of the specified transaction
					// branch was committed. A resource manager may return this value only if it has
					// successfully prepared xid.
					commitExists = true;
					archive.setCommitted(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURRB:
					// Due to a heuristic decision, the work done on behalf of the specified transaction
					// branch was rolled back. A resource manager may return this value only if it has
					// successfully prepared xid.
					rollbackExists = true;
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XAER_PROTO:
					// The routine was invoked in an improper context.
					int archiveVote = archive.getVote();
					if (archiveVote == XAResource.XA_OK) {
						commitExists = true;// TODO
						archive.setCommitted(true);
						archive.setCompleted(true);
					} else if (archiveVote == XAResource.XA_RDONLY) {
						// ignore
					} else {
						try {
							archive.end(branchXid, XAResource.TMFAIL);
							archive.rollback(branchXid);
							rollbackExists = true;
							archive.setRolledback(true);
							archive.setCompleted(true);
						} catch (Exception ignore) {
							unFinishExists = true;// TODO
						}
					}
					break;
				case XAException.XAER_NOTA:
					// The specified XID is not known by the resource manager.
				case XAException.XAER_RMERR:
					// An error occurred in rolling back the transaction branch. The resource manager is
					// free to forget about the branch when returning this error so long as all accessing
					// threads of control have been notified of the branch’s state.
					rollbackExists = true;
					archive.setRolledback(true);
					archive.setCompleted(true);
					break;
				case XAException.XAER_RMFAIL:
					// An error occurred that makes the resource manager unavailable.
				case XAException.XAER_INVAL:
					// Invalid arguments were specified.
					unFinishExists = true;
					break;
				default:// XA_RB*
					// The resource manager has rolled back the transaction branch’s work and has
					// released all held resources. These values are typically returned when the
					// branch was already marked rollback-only.
					rollbackExists = true;
					archive.setRolledback(true);
					archive.setCompleted(true);
				}
			}
		}

		try {
			this.throwRollbackExceptionIfNecessary(commitExists, rollbackExists);
		} catch (XAException xae) {
			if (unFinishExists) {
				throw new TransactionException(xae.errorCode);
			} else {
				throw xae;
			}
		}

	}

	private void throwRollbackExceptionIfNecessary(boolean commitExists, boolean rollbackExists) throws XAException {
		if (commitExists && rollbackExists) {
			throw new XAException(XAException.XA_HEURMIX);
		} else if (commitExists) {
			throw new XAException(XAException.XA_HEURCOM);
		}
	}

	public synchronized void recoveryRollback(Xid xid) throws TransactionException, XAException {
		this.fireRecoveryPrepare(xid);

		this.fireRecoveryRollback(xid);
	}

	private void fireRecoveryRollback(Xid xid) throws TransactionException, XAException {
		boolean commitExists = false;
		boolean rollbackExists = false;
		boolean unFinishExists = false;
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);
			Xid branchXid = archive.getXid();
			try {
				if (archive.isCompleted()) {
					if (archive.isCommitted()) {
						commitExists = true;
					} else if (archive.isRolledback()) {
						rollbackExists = true;
					} else {
						// read-only, ignore.
					}
				} else {
					archive.recoveryRollback(branchXid);
					rollbackExists = true;
					archive.setRolledback(true);
					archive.setCompleted(true);
					logger.info("[{}] rollback: xares= {}, branch= {}",
							ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
							ByteUtils.byteArrayToString(branchXid.getBranchQualifier()));
				}
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

					// commitExists = true;
					rollbackExists = true;
					// archive.setCommitted(true); // TODO
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURMIX:
					// Due to a heuristic decision, the work done on behalf of the specified transaction
					// branch was partially committed and partially rolled back. A resource manager
					// may return this value only if it has successfully prepared xid.
					commitExists = true;
					rollbackExists = true;
					archive.setCommitted(true);
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURCOM:
					// Due to a heuristic decision, the work done on behalf of the specified transaction
					// branch was committed. A resource manager may return this value only if it has
					// successfully prepared xid.
					commitExists = true;
					archive.setCommitted(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XA_HEURRB:
					// Due to a heuristic decision, the work done on behalf of the specified transaction
					// branch was rolled back. A resource manager may return this value only if it has
					// successfully prepared xid.
					rollbackExists = true;
					archive.setRolledback(true);
					archive.setHeuristic(true);
					archive.setCompleted(true);
					break;
				case XAException.XAER_PROTO:
					// The routine was invoked in an improper context.
					int archiveVote = archive.getVote();
					if (archiveVote == XAResource.XA_OK) {
						commitExists = true;// TODO
						archive.setCommitted(true);
						archive.setCompleted(true);
					} else if (archiveVote == XAResource.XA_RDONLY) {
						// ignore
					} else {
						try {
							archive.end(branchXid, XAResource.TMFAIL);
							archive.rollback(branchXid);
							rollbackExists = true;
							archive.setRolledback(true);
							archive.setCompleted(true);
						} catch (Exception ignore) {
							unFinishExists = true;// TODO
						}
					}
					break;
				case XAException.XAER_NOTA:
					// The specified XID is not known by the resource manager.
				case XAException.XAER_RMERR:
					// An error occurred in rolling back the transaction branch. The resource manager is
					// free to forget about the branch when returning this error so long as all accessing
					// threads of control have been notified of the branch’s state.
					rollbackExists = true;
					archive.setRolledback(true);
					archive.setCompleted(true);
					break;
				case XAException.XAER_RMFAIL:
					// An error occurred that makes the resource manager unavailable.
				case XAException.XAER_INVAL:
					// Invalid arguments were specified.
					unFinishExists = true;
					break;
				default:// XA_RB*
					// The resource manager has rolled back the transaction branch’s work and has
					// released all held resources. These values are typically returned when the
					// branch was already marked rollback-only.
					rollbackExists = true;
					archive.setRolledback(true);
					archive.setCompleted(true);
				}
			}
		}

		try {
			this.throwRollbackExceptionIfNecessary(commitExists, rollbackExists);
		} catch (XAException xae) {
			if (unFinishExists) {
				throw new TransactionException(xae.errorCode);
			} else {
				throw xae;
			}
		}

	}

	public int getTransactionTimeout() throws XAException {
		return this.transactionTimeout;
	}

	public boolean setTransactionTimeout(int seconds) throws XAException {
		this.transactionTimeout = seconds;
		return true;
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
		TransactionXid globalXid = this.transactionContext.getXid();
		int transactionStatus = transaction.getTransactionStatus();

		if (transactionStatus != Status.STATUS_PREPARING && transactionStatus != Status.STATUS_PREPARED
				&& transactionStatus != Status.STATUS_COMMITTING && transactionStatus != Status.STATUS_ROLLING_BACK) {
			return;
		}

		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);
			if (archive.isRecovered()) {
				continue;
			} else if (archive.isReadonly()) {
				continue;
			} else if (archive.isCommitted()) {
				continue;
			} else if (archive.isRolledback()) {
				continue;
			}

			boolean xidRecovered = false;
			if (archive.isIdentified()) {
				Xid thisXid = archive.getXid();
				byte[] thisGlobalTransactionId = thisXid.getGlobalTransactionId();
				byte[] thisBranchQualifier = thisXid.getBranchQualifier();
				try {
					Xid[] array = archive.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
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
					continue;
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
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive archive = this.resources.get(i);
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
		} // end-for
	}

	public boolean delistResource(XAResourceDescriptor descriptor, int flag) throws IllegalStateException, SystemException {

		XAResourceArchive archive = this.locateExisted(descriptor);
		if (archive == null) {
			throw new SystemException();
		}

		return this.delistResource(archive, flag);

	}

	private boolean delistResource(XAResourceArchive archive, int flag) throws SystemException, RollbackRequiredException {
		try {
			Xid branchXid = archive.getXid();
			archive.end(branchXid, flag);

			switch (flag) {
			case XAResource.TMSUCCESS:
			case XAResource.TMFAIL:
				archive.setDelisted(true);
				break;
			case XAResource.TMSUSPEND:
				break;
			default:
				throw new SystemException();
			}

			logger.info("[{}] delist: xares= {}, branch= {}, flags= {}",
					ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), flag);
		} catch (XAException xae) {
			logger.error("XATerminatorImpl.delistResource(XAResourceArchive, int)", xae);

			// Possible XAException values are XAER_RMERR, XAER_RMFAIL,
			// XAER_NOTA, XAER_INVAL, XAER_PROTO, or XA_RB*.
			switch (xae.errorCode) {
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
				return false;
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.
			case XAException.XAER_RMERR:
				// An error occurred in dissociating the transaction branch from the thread of control.
				SystemException sysex = new SystemException();
				sysex.initCause(xae);
				throw sysex;
			default:
				// XA_RB*
				RollbackRequiredException rrex = new RollbackRequiredException();
				rrex.initCause(xae);
				throw rrex;
			}
		} catch (RuntimeException ex) {
			SystemException sysex = new SystemException();
			sysex.initCause(ex);
			throw sysex;
		}

		return true;
	}

	public boolean enlistResource(XAResourceDescriptor descriptor)
			throws RollbackException, IllegalStateException, SystemException {

		XAResourceArchive archive = this.locateExisted(descriptor);
		int flags = XAResource.TMNOFLAGS;
		if (archive == null) {
			archive = new XAResourceArchive();
			archive.setDescriptor(descriptor);
			archive.setIdentified(UnidentifiedResourceDescriptor.class.isInstance(descriptor) == false);
			TransactionXid globalXid = this.transactionContext.getXid();
			XidFactory xidFactory = this.beanFactory.getXidFactory();
			archive.setXid(xidFactory.createBranchXid(globalXid));
		} else {
			flags = XAResource.TMJOIN;
		}

		return this.enlistResource(archive, flags);
	}

	private boolean enlistResource(XAResourceArchive archive, int flag) throws SystemException, RollbackException {
		try {
			Xid branchXid = archive.getXid();
			logger.info("[{}] enlist: xares= {}, branch= {}, flags: {}",
					ByteUtils.byteArrayToString(branchXid.getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(branchXid.getBranchQualifier()), flag);

			switch (flag) {
			case XAResource.TMNOFLAGS:
				long expired = this.transactionContext.getExpiredTime();
				long current = System.currentTimeMillis();
				long remains = expired - current;
				int timeout = (int) (remains / 1000L);
				archive.setTransactionTimeout(timeout);
				archive.start(branchXid, flag);
				this.resources.add(archive);
				break;
			case XAResource.TMJOIN:
				archive.start(branchXid, flag);
				archive.setDelisted(false);
				break;
			case XAResource.TMRESUME:
				archive.start(branchXid, flag);
				break;
			default:
				throw new SystemException();
			}
		} catch (XAException xae) {
			logger.error("XATerminatorImpl.enlistResource(XAResourceArchive, int)", xae);

			// Possible exceptions are XA_RB*, XAER_RMERR, XAER_RMFAIL,
			// XAER_DUPID, XAER_OUTSIDE, XAER_NOTA, XAER_INVAL, or XAER_PROTO.
			switch (xae.errorCode) {
			case XAException.XAER_DUPID:
				// * If neither TMJOIN nor TMRESUME is specified and the transaction
				// * specified by xid has previously been seen by the resource manager,
				// * the resource manager throws the XAException exception with XAER_DUPID error code.
				return false;
			case XAException.XAER_OUTSIDE:
				// The resource manager is doing work outside any global transaction
				// on behalf of the application.
			case XAException.XAER_NOTA:
				// Either TMRESUME or TMJOIN was set inflags, and the specified XID is not
				// known by the resource manager.
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
				return false;
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable
			case XAException.XAER_RMERR:
				// An error occurred in associating the transaction branch with the thread of control
				SystemException sysex = new SystemException();
				sysex.initCause(xae);
				throw sysex;
			default:
				// XA_RB*
				throw new RollbackException();
			}
		} catch (RuntimeException ex) {
			throw new RollbackException();
		}

		return true;
	}

	private XAResourceArchive locateExisted(XAResourceDescriptor descriptor) {
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive existed = this.resources.get(i);
			XAResourceDescriptor existedDescriptor = existed.getDescriptor();
			String identifier = descriptor.getIdentifier();
			String existedIdentifirer = existedDescriptor.getIdentifier();

			if (CommonUtils.equals(identifier, existedIdentifirer)) {
				try {
					if (existedDescriptor.isSameRM(descriptor)) {
						return existed;
					}
				} catch (XAException ex) {
					continue;
				} catch (RuntimeException ex) {
					continue;
				}
			} // end-if
		} // end-while
		return null;

	}

	public void resumeAllResource() throws RollbackException, SystemException {
		boolean rollbackRequired = false;
		boolean errorExists = false;
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive xares = this.resources.get(i);
			if (xares.isDelisted()) {
				try {
					this.enlistResource(xares, XAResource.TMRESUME);
				} catch (RollbackException rex) {
					rollbackRequired = true;
				} catch (SystemException rex) {
					errorExists = true;
				} catch (RuntimeException rex) {
					errorExists = true;
				}
			}
		}

		if (rollbackRequired) {
			throw new RollbackException();
		} else if (errorExists) {
			throw new SystemException(XAException.XAER_RMERR);
		}

	}

	public void suspendAllResource() throws RollbackException, SystemException {
		boolean rollbackRequired = false;
		boolean errorExists = false;
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive xares = this.resources.get(i);
			if (xares.isDelisted() == false) {
				try {
					this.delistResource(xares, XAResource.TMSUSPEND);
				} catch (RollbackRequiredException ex) {
					rollbackRequired = true;
				} catch (SystemException ex) {
					errorExists = true;
				} catch (RuntimeException ex) {
					errorExists = true;
				}
			}
		}

		if (rollbackRequired) {
			throw new RollbackException();
		} else if (errorExists) {
			throw new SystemException(XAException.XAER_RMERR);
		}
	}

	public void delistAllResource() throws RollbackException, SystemException {
		boolean rollbackRequired = false;
		boolean errorExists = false;
		for (int i = 0; i < this.resources.size(); i++) {
			XAResourceArchive xares = this.resources.get(i);
			if (xares.isDelisted() == false) {
				try {
					this.delistResource(xares, XAResource.TMSUCCESS);
				} catch (RollbackRequiredException ex) {
					rollbackRequired = true;
				} catch (SystemException ex) {
					errorExists = true;
				} catch (RuntimeException ex) {
					errorExists = true;
				}
			}
		} // end-for

		if (rollbackRequired) {
			throw new RollbackException();
		} else if (errorExists) {
			throw new SystemException(XAException.XAER_RMERR);
		}

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
