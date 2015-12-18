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
package org.bytesoft.transaction.archive;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.xa.Xid;

public class TransactionArchive {
	private Xid xid;
	private int status;
	private int vote;
	private boolean coordinator;
	private boolean compensable;
	private boolean optimized;
	private final List<XAResourceArchive> nativeResources = new ArrayList<XAResourceArchive>();
	private final List<XAResourceArchive> remoteResources = new ArrayList<XAResourceArchive>();

	public Xid getXid() {
		return xid;
	}

	public void setXid(Xid xid) {
		this.xid = xid;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getVote() {
		return vote;
	}

	public void setVote(int vote) {
		this.vote = vote;
	}

	public List<XAResourceArchive> getNativeResources() {
		return nativeResources;
	}

	public List<XAResourceArchive> getRemoteResources() {
		return remoteResources;
	}

	public boolean isOptimized() {
		return optimized;
	}

	public void setOptimized(boolean optimized) {
		this.optimized = optimized;
	}

	public boolean isCoordinator() {
		return coordinator;
	}

	public void setCoordinator(boolean coordinator) {
		this.coordinator = coordinator;
	}

	public boolean isCompensable() {
		return compensable;
	}

	public void setCompensable(boolean compensable) {
		this.compensable = compensable;
	}

}
