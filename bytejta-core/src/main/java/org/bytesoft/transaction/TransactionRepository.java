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

import java.util.List;

import org.bytesoft.transaction.xa.TransactionXid;

public interface TransactionRepository {

	// active-transaction & error-transaction
	public void putTransaction(TransactionXid xid, Transaction transaction);

	public Transaction getTransaction(TransactionXid xid);

	public Transaction removeTransaction(TransactionXid xid);

	// error-transaction
	public void putErrorTransaction(TransactionXid xid, Transaction transaction);

	public Transaction getErrorTransaction(TransactionXid xid);

	public Transaction removeErrorTransaction(TransactionXid xid);

	public List<Transaction> getErrorTransactionList();

	public List<Transaction> getActiveTransactionList();

}
