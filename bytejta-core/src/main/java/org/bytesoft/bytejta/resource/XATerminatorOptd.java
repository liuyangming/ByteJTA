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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

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

public class XATerminatorOptd implements XATerminator {
	static final Logger logger = LoggerFactory.getLogger(XATerminatorOptd.class);

	private TransactionBeanFactory beanFactory;
	private XAResourceArchive archive;

	public synchronized int prepare(Xid xid) throws XAException {
		if (this.archive == null) {
			return XAResource.XA_RDONLY;
		}
		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		boolean prepared = archive.getVote() != XAResourceArchive.DEFAULT_VOTE;

		int globalVote = XAResource.XA_RDONLY;
		if (prepared) {
			globalVote = archive.getVote();
		} else {
			globalVote = archive.prepare(archive.getXid());
			archive.setVote(globalVote);

			if (globalVote == XAResource.XA_RDONLY) {
				archive.setReadonly(true);
				archive.setCompleted(true);
			} else {
				globalVote = XAResource.XA_OK;
			}

			transactionLogger.updateResource(archive);
		}

		logger.info("[{}] prepare: xares= {}, branch= {}, vote= {}",
				ByteUtils.byteArrayToString(this.archive.getXid().getGlobalTransactionId()), archive,
				ByteUtils.byteArrayToString(this.archive.getXid().getBranchQualifier()), globalVote);

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
		if (archive.isCommitted() && archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURMIX);
		} else if (archive.isCommitted()) {
			return;
		} else if (archive.isReadonly()) {
			throw new XAException(XAException.XA_RDONLY); // XAException.XAER_NOTA
		} else if (archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURRB);
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		boolean updateRequired = true;
		try {
			archive.commit(archive.getXid(), true);
			archive.setCommitted(true);
			archive.setCompleted(true);

			logger.info("[{}] commit: xares= {}, branch= {}, opc= {}",
					ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), false);
		} catch (XAException xaex) {
			switch (xaex.errorCode) {
			case XAException.XA_HEURCOM:
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setCompleted(true);
				break;
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
			case XAException.XA_HEURHAZ:
				archive.setHeuristic(true);
				throw xaex;
			case XAException.XAER_RMFAIL:
				logger.warn("An error occurred in one phase commit: {}",
						ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()));
				updateRequired = false;
				throw new XAException(XAException.XA_HEURHAZ);
			case XAException.XAER_NOTA:
			case XAException.XAER_INVAL:
			case XAException.XAER_PROTO:
				logger.warn("An error occurred in one phase commit: {}",
						ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()));
				updateRequired = false;
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
				archive.setRolledback(true);
				archive.setCompleted(true);
				throw new XAException(XAException.XA_HEURRB);
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
		if (archive.isCommitted() && archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURMIX);
		} else if (archive.isCommitted()) {
			return;
		} else if (archive.isReadonly()) {
			throw new XAException(XAException.XA_RDONLY); // XAException.XAER_NOTA
		} else if (archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURRB);
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		boolean updateRequired = true;
		try {
			archive.commit(archive.getXid(), false);
			archive.setCommitted(true);
			archive.setCompleted(true);

			logger.info("[{}] commit: xares= {}, branch= {}, opc= {}",
					ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()), false);
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

				archive.setHeuristic(true);
				throw xaex;
			case XAException.XA_HEURMIX:
				// Due to a heuristic decision, the work done on behalf of the specified
				// transaction branch was partially committed and partially rolled back.
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setRolledback(true);
				archive.setCompleted(true);
				throw xaex;
			case XAException.XA_HEURCOM:
				// Due to a heuristic decision, the work done on behalf of
				// the specified transaction branch was committed.
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setCompleted(true);
				break;
			case XAException.XA_HEURRB:
				// Due to a heuristic decision, the work done on behalf of
				// the specified transaction branch was rolled back.
				archive.setHeuristic(true);
				archive.setRolledback(true);
				archive.setCompleted(true);
				throw xaex;
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.
				archive.setReadonly(true);
				throw new XAException(XAException.XA_RDONLY); // read-only
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.

				updateRequired = false;
				throw new XAException(XAException.XA_HEURHAZ);
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.

				updateRequired = false;
				throw new XAException(XAException.XAER_RMERR);
			case XAException.XAER_RMERR:
				// An error occurred in committing the work performed on behalf of the transaction
				// branch and the branch’s work has been rolled back. Note that returning this error
				// signals a catastrophic event to a transaction manager since other resource
				// managers may successfully commit their work on behalf of this branch. This error
				// should be returned only when a resource manager concludes that it can never
				// commit the branch and that it cannot hold the branch’s resources in a prepared
				// state. Otherwise, [XA_RETRY] should be returned.
			default: // XA_RB*
				archive.setRolledback(true);
				archive.setCompleted(true);
				throw new XAException(XAException.XA_HEURRB);
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

	/** error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR */
	public synchronized void rollback(Xid xid) throws XAException {
		if (archive.isCommitted() && archive.isRolledback()) {
			throw new XAException(XAException.XA_HEURMIX);
		} else if (archive.isRolledback()) {
			return;
		} else if (archive.isReadonly()) {
			throw new XAException(XAException.XA_RDONLY);
		} else if (archive.isCommitted()) {
			throw new XAException(XAException.XA_HEURCOM);
		}

		TransactionLogger transactionLogger = this.beanFactory.getTransactionLogger();

		boolean updateRequired = true;
		try {
			archive.rollback(archive.getXid());
			archive.setRolledback(true);
			archive.setCompleted(true);

			logger.info("[{}] rollback: xares= {}, branch= {}",
					ByteUtils.byteArrayToString(archive.getXid().getGlobalTransactionId()), archive,
					ByteUtils.byteArrayToString(archive.getXid().getBranchQualifier()));
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
				archive.setHeuristic(true);
				throw xaex;
			case XAException.XA_HEURMIX:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was partially committed and partially rolled back. A resource manager
				// may return this value only if it has successfully prepared xid.
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setRolledback(true);
				archive.setCompleted(true);
				throw xaex;
			case XAException.XA_HEURCOM:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was committed. A resource manager may return this value only if it has
				// successfully prepared xid.
				archive.setHeuristic(true);
				archive.setCommitted(true);
				archive.setCompleted(true);
				throw xaex;
			case XAException.XA_HEURRB:
				// Due to a heuristic decision, the work done on behalf of the specified transaction
				// branch was rolled back. A resource manager may return this value only if it has
				// successfully prepared xid.
				archive.setHeuristic(true);
				archive.setRolledback(true);
				archive.setCompleted(true);
				break;
			case XAException.XAER_RMFAIL:
				// An error occurred that makes the resource manager unavailable.

				updateRequired = false;
				throw new XAException(XAException.XA_HEURHAZ);
			case XAException.XAER_NOTA:
				// The specified XID is not known by the resource manager.
				if (archive.isReadonly()) {
					archive.setReadonly(true);
					archive.setCompleted(true);
					throw new XAException(XAException.XA_RDONLY);
				} else if (archive.getVote() == XAResourceArchive.DEFAULT_VOTE) {
					archive.setRolledback(true);
					archive.setCompleted(true);
					break; // rolled back
				} else if (archive.getVote() == XAResource.XA_RDONLY) {
					archive.setReadonly(true);
					archive.setCompleted(true);
					throw new XAException(XAException.XA_RDONLY);
				} else if (archive.getVote() == XAResource.XA_OK) {
					updateRequired = false;
					throw new XAException(XAException.XAER_RMERR);
				} else {
					updateRequired = false;
					throw new XAException(XAException.XAER_RMERR);
				}
			case XAException.XAER_PROTO:
				// The routine was invoked in an improper context.
			case XAException.XAER_INVAL:
				// Invalid arguments were specified.

				updateRequired = false;
				throw new XAException(XAException.XAER_RMERR);
			case XAException.XAER_RMERR:
				// An error occurred in rolling back the transaction branch. The resource manager is
				// free to forget about the branch when returning this error so long as all accessing
				// threads of control have been notified of the branch’s state.
			default: // XA_RB*
				// The resource manager has rolled back the transaction branch’s work and has
				// released all held resources. These values are typically returned when the
				// branch was already marked rollback-only.
				archive.setRolledback(true);
				archive.setCompleted(true);
			}
		} catch (RuntimeException rex) {
			logger.error("[{}] Error occurred while rolling back xa-resource: xares= {}, branch= {}",
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
