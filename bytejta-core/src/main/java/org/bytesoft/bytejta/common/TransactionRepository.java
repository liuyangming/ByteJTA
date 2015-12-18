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
package org.bytesoft.bytejta.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionRepository {
	private final Map<TransactionXid, TransactionImpl> xidToTxMap = new ConcurrentHashMap<TransactionXid, TransactionImpl>();
	private final Map<TransactionXid, TransactionImpl> xidToErrTxMap = new ConcurrentHashMap<TransactionXid, TransactionImpl>();

	public void putTransaction(TransactionXid globalXid, TransactionImpl transaction) {
		this.xidToTxMap.put(globalXid, transaction);
	}

	public TransactionImpl getTransaction(TransactionXid globalXid) {
		return this.xidToTxMap.get(globalXid);
	}

	public TransactionImpl removeTransaction(TransactionXid globalXid) {
		return this.xidToTxMap.remove(globalXid);
	}

	public void putErrorTransaction(TransactionXid globalXid, TransactionImpl transaction) {
		this.xidToErrTxMap.put(globalXid, transaction);
	}

	public TransactionImpl getErrorTransaction(TransactionXid globalXid) {
		return this.xidToErrTxMap.get(globalXid);
	}

	public TransactionImpl removeErrorTransaction(TransactionXid globalXid) {
		return this.xidToErrTxMap.remove(globalXid);
	}

	public List<TransactionImpl> getErrorTransactionList() {
		return new ArrayList<TransactionImpl>(this.xidToErrTxMap.values());
	}

	public List<TransactionImpl> getActiveTransactionList() {
		return new ArrayList<TransactionImpl>(this.xidToTxMap.values());
	}

}
