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
package org.bytesoft.transaction.supports;

import org.bytesoft.transaction.xa.TransactionXid;

public interface TransactionListener {

	public void onPrepareStart(TransactionXid xid);

	public void onPrepareSuccess(TransactionXid xid);

	public void onPrepareFailure(TransactionXid xid);

	public void onCommitStart(TransactionXid xid);

	public void onCommitSuccess(TransactionXid xid);

	public void onCommitFailure(TransactionXid xid);

	public void onCommitHeuristicMixed(TransactionXid xid);

	public void onCommitHeuristicRolledback(TransactionXid xid);

	public void onRollbackStart(TransactionXid xid);

	public void onRollbackSuccess(TransactionXid xid);

	public void onRollbackFailure(TransactionXid xid);

}
