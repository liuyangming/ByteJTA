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
package org.bytesoft.bytejta.xa;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.common.TransactionConfigurator;
import org.bytesoft.bytejta.common.TransactionRepository;
import org.bytesoft.transaction.xa.TransactionXid;

public class RemoteResourceManager implements XAResource {

	public void commit(Xid xid, boolean onePhase) throws XAException {

		if (TransactionXid.class.isInstance(xid) == false) {
			throw new XAException(XAException.XAER_INVAL);
		}

		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
		TransactionXid currentXid = (TransactionXid) xid;
		TransactionXid globalXid = currentXid.getGlobalXid();
		TransactionImpl transaction = transactionRepository.getTransaction(globalXid);
		if (transaction == null) {
			transaction = transactionRepository.getErrorTransaction(globalXid);
		}

		if (transaction == null) {
			throw new XAException(XAException.XAER_NOTA);
		}

		if (onePhase) {
			// TODO
		} else {
			// TODO
		}

	}

	public void end(Xid xid, int flags) throws XAException {
	}

	public void forget(Xid xid) throws XAException {
	}

	public int getTransactionTimeout() throws XAException {
		return 0;
	}

	public boolean isSameRM(XAResource xares) throws XAException {
		return false;
	}

	public int prepare(Xid xid) throws XAException {
		if (TransactionXid.class.isInstance(xid) == false) {
			throw new XAException(XAException.XAER_INVAL);
		}

		TransactionConfigurator transactionConfigurator = TransactionConfigurator.getInstance();
		TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
		TransactionXid currentXid = (TransactionXid) xid;
		TransactionXid globalXid = currentXid.getGlobalXid();
		TransactionImpl transaction = transactionRepository.getTransaction(globalXid);
		if (transaction == null) {
			transaction = transactionRepository.getErrorTransaction(globalXid);
		}

		if (transaction == null) {
			throw new XAException(XAException.XAER_NOTA);
		}

		// TODO
		return XAResource.XA_OK;

	}

	public Xid[] recover(int flag) throws XAException {
		return null;
	}

	public void rollback(Xid xid) throws XAException {
	}

	public boolean setTransactionTimeout(int seconds) throws XAException {
		return false;
	}

	public void start(Xid xid, int flags) throws XAException {
	}

}
