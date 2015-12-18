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
package org.bytesoft.transaction.logger;

import java.util.List;

import javax.transaction.xa.Xid;

import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;

public class TransactionLoggerProxy implements TransactionLogger {
	private static final EmptyTransactionLogger empty = new EmptyTransactionLogger();
	private TransactionLogger delegate = empty;

	public void createTransaction(TransactionArchive archive) {
		delegate.createTransaction(archive);
	}

	public void updateTransaction(TransactionArchive archive) {
		delegate.updateTransaction(archive);
	}

	public void deleteTransaction(TransactionArchive archive) {
		delegate.deleteTransaction(archive);
	}

	public List<TransactionArchive> getTransactionArchiveList() {
		return delegate.getTransactionArchiveList();
	}

	public void updateResource(Xid transactionXid, XAResourceArchive archive) {
		delegate.updateResource(transactionXid, archive);
	}

	public void setDelegate(TransactionLogger delegate) {
		if (delegate == null) {
			this.delegate = empty;
		} else {
			this.delegate = delegate;
		}
	}

	public TransactionLogger getDelegate() {
		return delegate;
	}
}
