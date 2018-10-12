/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta;

import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.Xid;

import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;

public interface TransactionStrategy /* extends TransactionBeanFactoryAware */ {

	public int TRANSACTION_STRATEGY_VACANT = 0;
	public int TRANSACTION_STRATEGY_SIMPLE = 1;
	public int TRANSACTION_STRATEGY_COMMON = 2;
	public int TRANSACTION_STRATEGY_LRO = 3;

	public int prepare(Xid xid) throws RollbackRequiredException, CommitRequiredException;

	public void commit(Xid xid, boolean onePhaseCommit)
			throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, SystemException;

	public void rollback(Xid xid)
			throws HeuristicMixedException, HeuristicCommitException, IllegalStateException, SystemException;

}
