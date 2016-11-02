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
package org.bytesoft.transaction.archive;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAResourceArchive implements XAResource {
	static final Logger logger = LoggerFactory.getLogger(XAResourceArchive.class);
	public static final int DEFAULT_VOTE = -1;

	private boolean delisted;
	private boolean completed;
	private boolean readonly;
	private boolean committed;
	private boolean rolledback;
	private boolean heuristic;
	private boolean identified;

	private transient boolean recovered;

	private Xid xid;
	private int vote = DEFAULT_VOTE;
	private XAResourceDescriptor descriptor;

	public void commit(Xid ignore, boolean onePhase) throws XAException {
		if (this.readonly) {
			// ignore
		} else if (this.committed) {
			// ignore
		} else if (this.rolledback) {
			throw new XAException(XAException.XA_HEURRB);
		} else {
			descriptor.commit(xid, onePhase);
		}
	}

	public void recoveryCommit(Xid xid) throws XAException {
		if (this.readonly) {
			// ignore
		} else if (this.committed) {
			// ignore
		} else if (this.rolledback) {
			throw new XAException(XAException.XA_HEURRB);
		} else {
			descriptor.recoveryCommit(xid);
		}
	}

	public void end(Xid ignore, int flags) throws XAException {
		descriptor.end(xid, flags);
	}

	public void recoveryForget(Xid xid) throws XAException {
		descriptor.recoveryForget(xid);
	}

	public void forget(Xid ignore) throws XAException {
		descriptor.forget(xid);
	}

	public void forgetQuietly(Xid ignore) {
		try {
			descriptor.forget(xid);
		} catch (XAException ex) {
			logger.warn("Error occurred while forgeting xa-resource.", xid);
		}
	}

	public int getTransactionTimeout() throws XAException {
		return descriptor.getTransactionTimeout();
	}

	public boolean isSameRM(XAResource xares) throws XAException {
		if (XAResourceArchive.class.isInstance(xares)) {
			XAResourceArchive archive = (XAResourceArchive) xares;
			return descriptor.isSameRM(archive.getDescriptor());
		} else {
			return descriptor.isSameRM(xares);
		}
	}

	public int prepare(Xid ignore) throws XAException {
		if (this.vote == -1) {
			this.vote = this.descriptor.prepare(this.xid);
			this.readonly = this.vote == XAResource.XA_RDONLY;
		}
		return this.vote;

	}

	public Xid[] recover(int flag) throws XAException {
		return descriptor.recover(flag);
	}

	public void rollback(Xid ignore) throws XAException {

		if (this.readonly) {
			// ignore
		} else if (this.committed) {
			throw new XAException(XAException.XA_HEURCOM);
		} else if (this.rolledback) {
			// ignore
		} else {
			descriptor.rollback(xid);
		}

	}

	public void recoveryRollback(Xid xid) throws XAException {
		if (this.readonly) {
			// ignore
		} else if (this.committed) {
			throw new XAException(XAException.XA_HEURCOM);
		} else if (this.rolledback) {
			// ignore
		} else {
			descriptor.recoveryRollback(xid);
		}
	}

	public boolean setTransactionTimeout(int seconds) throws XAException {
		return descriptor.setTransactionTimeout(seconds);
	}

	public void start(Xid ignore, int flags) throws XAException {
		descriptor.start(xid, flags);
	}

	public String toString() {
		return String.format("xa-res-archive[descriptor: %s]", this.descriptor);
	}

	public XAResourceDescriptor getDescriptor() {
		return descriptor;
	}

	public void setDescriptor(XAResourceDescriptor descriptor) {
		this.descriptor = descriptor;
	}

	public boolean isDelisted() {
		return delisted;
	}

	public void setDelisted(boolean delisted) {
		this.delisted = delisted;
	}

	public Xid getXid() {
		return xid;
	}

	public void setXid(Xid xid) {
		this.xid = xid;
	}

	public int getVote() {
		return vote;
	}

	public void setVote(int vote) {
		this.vote = vote;
	}

	public boolean isCompleted() {
		return completed;
	}

	public void setCompleted(boolean completed) {
		this.completed = completed;
	}

	public boolean isCommitted() {
		return committed;
	}

	public void setCommitted(boolean committed) {
		this.committed = committed;
	}

	public boolean isRolledback() {
		return rolledback;
	}

	public void setRolledback(boolean rolledback) {
		this.rolledback = rolledback;
	}

	public boolean isReadonly() {
		return readonly;
	}

	public void setReadonly(boolean readonly) {
		this.readonly = readonly;
	}

	public boolean isHeuristic() {
		return heuristic;
	}

	public void setHeuristic(boolean heuristic) {
		this.heuristic = heuristic;
	}

	public boolean isRecovered() {
		return recovered;
	}

	public void setRecovered(boolean recovered) {
		this.recovered = recovered;
	}

	public boolean isIdentified() {
		return identified;
	}

	public void setIdentified(boolean identified) {
		this.identified = identified;
	}

}
