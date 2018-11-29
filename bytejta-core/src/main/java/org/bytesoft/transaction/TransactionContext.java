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
package org.bytesoft.transaction;

import java.io.Serializable;

import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionContext implements Serializable, Cloneable {
	private static final long serialVersionUID = 1L;

	protected transient Object propagatedBy;
	protected transient boolean propagated;
	protected transient boolean coordinator;
	protected transient boolean recoveried;
	protected transient int recoveredTimes;

	protected TransactionXid xid;
	protected long createdTime;
	protected long expiredTime;
	protected boolean rollbackOnly;
	protected long configFlags;

	public TransactionContext clone() {
		TransactionContext that = new TransactionContext();
		that.xid = this.xid.clone();
		that.createdTime = System.currentTimeMillis();
		that.expiredTime = this.expiredTime;
		that.rollbackOnly = this.rollbackOnly;
		that.configFlags = this.configFlags;
		return that;
	}

	public int getRecoveredTimes() {
		return recoveredTimes;
	}

	public void setRecoveredTimes(int recoveredTimes) {
		this.recoveredTimes = recoveredTimes;
	}

	public boolean isCoordinator() {
		return coordinator;
	}

	public void setCoordinator(boolean coordinator) {
		this.coordinator = coordinator;
	}

	public boolean isRecoveried() {
		return recoveried;
	}

	public void setRecoveried(boolean recoveried) {
		this.recoveried = recoveried;
	}

	public TransactionXid getXid() {
		return xid;
	}

	public void setXid(TransactionXid xid) {
		this.xid = xid;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}

	public long getExpiredTime() {
		return expiredTime;
	}

	public void setExpiredTime(long expiredTime) {
		this.expiredTime = expiredTime;
	}

	public long getConfigFlags() {
		return configFlags;
	}

	public void setConfigFlags(long configFlags) {
		this.configFlags = configFlags;
	}

	public boolean isPropagated() {
		return propagated;
	}

	public void setPropagated(boolean propagated) {
		this.propagated = propagated;
	}

	public Object getPropagatedBy() {
		return propagatedBy;
	}

	public void setPropagatedBy(Object propagatedBy) {
		this.propagatedBy = propagatedBy;
	}

	public boolean isRollbackOnly() {
		return rollbackOnly;
	}

	public void setRollbackOnly(boolean rollbackOnly) {
		this.rollbackOnly = rollbackOnly;
	}

}
