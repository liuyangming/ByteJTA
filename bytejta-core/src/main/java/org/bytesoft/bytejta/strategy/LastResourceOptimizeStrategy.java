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
package org.bytesoft.bytejta.strategy;

import javax.transaction.HeuristicCommitException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.TransactionStrategy;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.internal.TransactionException;
import org.bytesoft.transaction.resource.XATerminator;

public class LastResourceOptimizeStrategy implements TransactionStrategy {
	private final XATerminator terminatorOne;
	private final XATerminator terminatorTwo;

	public LastResourceOptimizeStrategy(XATerminator terminatorOne, XATerminator terminatorTwo) {
		if (terminatorOne == null || terminatorOne.getResourceArchives().size() != 1) {
			throw new IllegalStateException();
		} else if (terminatorTwo == null || terminatorTwo.getResourceArchives().isEmpty()) {
			throw new IllegalStateException();
		}

		this.terminatorOne = terminatorOne;
		this.terminatorTwo = terminatorTwo;
	}

	public void prepare(Xid xid) throws RollbackRequiredException, CommitRequiredException {
		try {
			this.terminatorTwo.prepare(xid);
		} catch (Exception ex) {
			throw new RollbackRequiredException();
		}

		try {
			this.terminatorOne.commit(xid, true);
		} catch (XAException ex) {
			throw new RollbackRequiredException();
		}

	}

	public void commit(Xid xid)
			throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, SystemException {
		try {
			this.terminatorTwo.commit(xid, false);
		} catch (XAException ex) {
			switch (ex.errorCode) {
			case XAException.XA_HEURCOM:
				break;
			case XAException.XA_HEURRB:
				throw new HeuristicRollbackException();
			case XAException.XA_HEURMIX:
				throw new HeuristicMixedException();
			default:
				throw new SystemException();
			}
		} catch (RuntimeException ex) {
			throw new SystemException();
		}
	}

	public void rollback(Xid xid)
			throws HeuristicMixedException, HeuristicCommitException, IllegalStateException, SystemException {

		boolean mixedExists = false;
		boolean committedExists = false;
		boolean rolledbackExists = false;
		boolean unFinishExists = false;
		try {
			this.terminatorOne.rollback(xid);
			rolledbackExists = true;
			mixedExists = committedExists ? true : mixedExists;
		} catch (XAException ex) {
			switch (ex.errorCode) {
			case XAException.XA_HEURCOM:
				committedExists = true;
				mixedExists = rolledbackExists ? true : mixedExists;
				break;
			case XAException.XA_HEURRB:
				rolledbackExists = true;
				mixedExists = committedExists ? true : mixedExists;
				break;
			case XAException.XA_HEURMIX:
				mixedExists = true;
				break;
			}

			boolean errorFlag = TransactionException.class.isInstance(ex);
			unFinishExists = errorFlag ? true : unFinishExists;
		} catch (RuntimeException ex) {
			unFinishExists = true;
		}

		try {
			this.terminatorTwo.rollback(xid);
			rolledbackExists = true;
			mixedExists = committedExists ? true : mixedExists;
		} catch (XAException ex) {
			switch (ex.errorCode) {
			case XAException.XA_HEURCOM:
				committedExists = true;
				mixedExists = rolledbackExists ? true : mixedExists;
				break;
			case XAException.XA_HEURRB:
				rolledbackExists = true;
				mixedExists = committedExists ? true : mixedExists;
				break;
			case XAException.XA_HEURMIX:
				mixedExists = true;
				break;
			}

			boolean errorFlag = TransactionException.class.isInstance(ex);
			unFinishExists = errorFlag ? true : unFinishExists;
		} catch (RuntimeException ex) {
			unFinishExists = true;
		}

		if (mixedExists) {
			throw new HeuristicMixedException();
		} else if (committedExists && rolledbackExists) {
			throw new HeuristicMixedException();
		} else if (unFinishExists) {
			if (committedExists) {
				throw new HeuristicCommitException();
			} else {
				// ignore
			}
		} else {
			throw new SystemException();
		}

	}

}
