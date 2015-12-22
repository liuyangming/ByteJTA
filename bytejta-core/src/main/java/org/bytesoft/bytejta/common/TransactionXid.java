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
package org.bytesoft.bytejta.common;

import java.io.Serializable;

import javax.transaction.xa.Xid;

import org.bytesoft.transaction.xa.AbstractXid;
import org.bytesoft.transaction.xa.XidFactory;

public class TransactionXid extends AbstractXid implements Xid, Serializable {
	private static final long serialVersionUID = 1L;

	public TransactionXid(byte[] global) {
		this(global, new byte[0]);
	}

	public TransactionXid(byte[] global, byte[] branch) {
		super(global, branch);
	}

	public int getFormatId() {
		return XidFactory.JTA_FORMAT_ID;
	}

	public AbstractXid getGlobalXid() {
		if (this.globalTransactionId == null || this.globalTransactionId.length == 0) {
			throw new IllegalStateException();
		} else if (this.branchQualifier != null && this.branchQualifier.length > 0) {
			TransactionBeanFactory beanFactory = TransactionBeanFactory.getInstance();
			XidFactory xidFactory = beanFactory.getXidFactory();
			return xidFactory.createGlobalXid(this.globalTransactionId);
		} else {
			return this;
		}
	}

	public AbstractXid createBranchXid() {
		if (this.globalTransactionId == null || this.globalTransactionId.length == 0) {
			throw new IllegalStateException();
		} else if (this.branchQualifier != null && this.branchQualifier.length > 0) {
			throw new IllegalStateException();
		} else {
			TransactionBeanFactory beanFactory = TransactionBeanFactory.getInstance();
			XidFactory xidFactory = beanFactory.getXidFactory();
			return xidFactory.createBranchXid(this);
		}
	}

}
