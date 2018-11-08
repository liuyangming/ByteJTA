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
package org.bytesoft.bytejta.supports.resource;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.remote.RemoteNode;
import org.bytesoft.transaction.remote.RemoteSvc;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;

public class RemoteResourceDescriptor implements XAResourceDescriptor {
	public static final int X_SAME_CLUSTER = 501;

	private RemoteCoordinator delegate;
	private String identifier;

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public String getIdentifier() {
		if (StringUtils.isNotBlank(this.identifier)) {
			return this.identifier;
		} else if (this.delegate == null) {
			return null;
		}

		this.identifier = this.delegate.getIdentifier();
		return this.identifier;
	}

	public RemoteAddr getRemoteAddr() {
		return this.delegate == null ? null : this.delegate.getRemoteAddr();
	}

	public RemoteNode getRemoteNode() {
		return this.delegate == null ? null : this.delegate.getRemoteNode();
	}

	public RemoteSvc getRemoteSvc() {
		return this.delegate == null ? null : CommonUtils.getRemoteSvc(this.delegate.getIdentifier());
	}

	public boolean isTransactionCommitted(Xid xid) throws IllegalStateException {
		throw new IllegalStateException();
	}

	public String toString() {
		return String.valueOf(this.delegate);
	}

	public void setTransactionTimeoutQuietly(int timeout) {
	}

	public void commit(Xid arg0, boolean arg1) throws XAException {
		delegate.commit(arg0, arg1);
	}

	public void end(Xid arg0, int arg1) throws XAException {
		// delegate.end(arg0, arg1);
	}

	public void forget(Xid arg0) throws XAException {
		delegate.forget(arg0);
	}

	public int getTransactionTimeout() throws XAException {
		throw new XAException(XAException.XAER_RMERR);
	}

	public boolean isSameRM(XAResource xares) throws XAException {
		if (xares == null) {
			return false;
		} else if (RemoteResourceDescriptor.class.isInstance(xares) == false) {
			return false;
		}
		RemoteResourceDescriptor that = (RemoteResourceDescriptor) xares;
		String thisKey = this.getIdentifier();
		String thatKey = that.getIdentifier();

		if (StringUtils.equalsIgnoreCase(thisKey, thatKey)) {
			return true;
		}

		if (CommonUtils.applicationEquals(thisKey, thatKey)) {
			throw new XAException(X_SAME_CLUSTER);
		}

		return false;
	}

	public int prepare(Xid arg0) throws XAException {
		return delegate.prepare(arg0);
	}

	public Xid[] recover(int arg0) throws XAException {
		return delegate.recover(arg0);
	}

	public void rollback(Xid arg0) throws XAException {
		delegate.rollback(arg0);
	}

	public boolean setTransactionTimeout(int arg0) throws XAException {
		return true;
	}

	public void start(Xid arg0, int arg1) throws XAException {
		delegate.start(arg0, arg1);
	}

	public RemoteCoordinator getDelegate() {
		return delegate;
	}

	public void setDelegate(RemoteCoordinator delegate) {
		this.delegate = delegate;
	}

}
