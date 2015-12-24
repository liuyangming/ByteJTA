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
package org.bytesoft.transaction.supports;

import org.bytesoft.bytejta.TransactionImpl;

public interface TransactionStatistic {
	public static final long FLAGS_ACTIVE = 0x1;
	public static final long FLAGS_PREPARING = 0x2;
	public static final long FLAGS_PREPARED = 0x4;
	public static final long FLAGS_COMMITTING = 0x8;
	public static final long FLAGS_COMMITTED = 0x10;
	public static final long FLAGS_ROLLINGBACK = 0x20;
	public static final long FLAGS_ROLEDBACK = 0x40;
	public static final long FLAGS_ERROR = 0x80;
	public static final long FLAGS_ERROR_TOTAL = 0x100;

	public void fireBeginTransaction(TransactionImpl transaction);

	public void firePreparingTransaction(TransactionImpl transaction);

	public void firePreparedTransaction(TransactionImpl transaction);

	public void fireCommittingTransaction(TransactionImpl transaction);

	public void fireCommittedTransaction(TransactionImpl transaction);

	public void fireRollingBackTransaction(TransactionImpl transaction);

	public void fireRolledbackTransaction(TransactionImpl transaction);

	public void fireCompleteFailure(TransactionImpl transaction);

	public void fireCleanupTransaction(TransactionImpl transaction);

	public void fireRecoverTransaction(TransactionImpl transaction);

}
