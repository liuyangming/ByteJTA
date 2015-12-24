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
package org.bytesoft.transaction;

import java.util.List;

import org.bytesoft.transaction.xa.TransactionXid;

public interface TransactionRepository<T> {

	public void putTransaction(TransactionXid xid, T transaction);

	public T getTransaction(TransactionXid xid);

	public T removeTransaction(TransactionXid xid);

	public void putErrorTransaction(TransactionXid xid, T transaction);

	public T getErrorTransaction(TransactionXid xid);

	public T removeErrorTransaction(TransactionXid xid);

	public List<T> getErrorTransactionList();

	public List<T> getActiveTransactionList();

}
