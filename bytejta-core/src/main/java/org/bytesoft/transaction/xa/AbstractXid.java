/**
 * Copyright 2014 yangming.liu<liuyangming@gmail.com>.
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
package org.bytesoft.transaction.xa;

import java.io.Serializable;
import java.util.Arrays;

import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.utils.ByteUtils;

public abstract class AbstractXid implements Xid, Serializable {
	private static final long serialVersionUID = 1L;

	protected byte[] globalTransactionId;
	protected byte[] branchQualifier;

	public AbstractXid(byte[] global) {
		this(global, new byte[0]);
	}

	public AbstractXid(byte[] global, byte[] branch) {
		if (global == null) {
			throw new IllegalArgumentException("globalTransactionId cannot be null.");
		} else if (global.length > MAXGTRIDSIZE) {
			throw new IllegalArgumentException("length of globalTransactionId cannot exceed 64 bytes.");
		}

		if (branch == null) {
			throw new IllegalArgumentException("branchQualifier cannot be null.");
		} else if (branch.length > MAXBQUALSIZE) {
			throw new IllegalArgumentException("length of branchQualifier cannot exceed 64 bytes.");
		}
		this.globalTransactionId = global;
		this.branchQualifier = branch;
	}

	public abstract int getFormatId();

	public abstract AbstractXid getGlobalXid();

	public abstract AbstractXid createBranchXid();

	public byte[] getBranchQualifier() {
		return this.branchQualifier;
	}

	public byte[] getGlobalTransactionId() {
		return this.globalTransactionId;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getFormatId();
		result = prime * result + Arrays.hashCode(branchQualifier);
		result = prime * result + Arrays.hashCode(globalTransactionId);
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (getClass() != obj.getClass()) {
			return false;
		}
		AbstractXid other = (AbstractXid) obj;
		if (this.getFormatId() != other.getFormatId()) {
			return false;
		} else if (Arrays.equals(branchQualifier, other.branchQualifier) == false) {
			return false;
		} else if (Arrays.equals(globalTransactionId, other.globalTransactionId) == false) {
			return false;
		}
		return true;
	}

	public String toString() {
		String global = this.globalTransactionId == null ? null : ByteUtils.byteArrayToString(this.globalTransactionId);
		String branch = this.branchQualifier == null ? null : ByteUtils.byteArrayToString(this.branchQualifier);
		return String.format("%s-%s-%s", this.getFormatId(), global, branch);
	}

}
