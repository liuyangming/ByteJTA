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

import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;

public class RemoteResourceDescriptor implements XAResourceDescriptor {

	private RemoteCoordinator delegate;
	private String identifier;

	public boolean isTransactionCommitted(Xid xid) throws IllegalStateException {
		throw new IllegalStateException();
	}

	public String toString() {
		return String.format("remote-resource[id= %s]", this.identifier);
	}

	public void setTransactionTimeoutQuietly(int timeout) {
	}

	public void commit(Xid arg0, boolean arg1) throws XAException {
		delegate.commit(arg0, arg1);
	}

	public void recoveryCommit(Xid arg0) throws XAException {
		delegate.recoveryCommit(arg0, false);
	}

	public void end(Xid arg0, int arg1) throws XAException {
		// delegate.end(arg0, arg1);
	}

	public void recoveryForget(Xid arg0) throws XAException {
		delegate.recoveryForget(arg0);
	}

	public void forget(Xid arg0) throws XAException {
		delegate.forget(arg0);
	}

	public int getTransactionTimeout() throws XAException {
		throw new XAException(XAException.XAER_RMERR);
	}

	public boolean isSameRM(XAResource xares) throws XAException {
		try {
			RemoteResourceDescriptor that = (RemoteResourceDescriptor) xares;
			return CommonUtils.equals(this.getIdentifier(), that.getIdentifier());
		} catch (RuntimeException rex) {
			return false;
		}
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

	public void recoveryRollback(Xid arg0) throws XAException {
		delegate.recoveryRollback(arg0);
	}

	public boolean setTransactionTimeout(int arg0) throws XAException {
		return true;
	}

	public void start(Xid arg0, int arg1) throws XAException {
		// delegate.start(arg0, arg1);
	}

	public RemoteCoordinator getDelegate() {
		return delegate;
	}

	public void setDelegate(RemoteCoordinator delegate) {
		this.delegate = delegate;
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

}
