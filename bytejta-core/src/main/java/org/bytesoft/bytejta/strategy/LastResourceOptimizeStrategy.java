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
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.TransactionStrategy;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
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

	public int prepare(Xid xid) throws RollbackRequiredException, CommitRequiredException {

		int vote = XAResource.XA_RDONLY;
		try {
			vote = this.terminatorTwo.prepare(xid);
		} catch (Exception ex) {
			throw new RollbackRequiredException();
		}

		try {
			this.terminatorOne.commit(xid, true);
		} catch (XAException ex) {
			// error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR
			switch (ex.errorCode) {
			case XAException.XA_HEURCOM:
				throw new CommitRequiredException();
			case XAException.XA_HEURRB:
				throw new RollbackRequiredException();
			case XAException.XA_HEURMIX:
				throw new CommitRequiredException();
			case XAException.XA_HEURHAZ:
				throw new CommitRequiredException(); // TODO
			case XAException.XA_RDONLY:
				return vote;
			case XAException.XAER_RMERR:
			default:
				throw new RollbackRequiredException();
			}
		} catch (RuntimeException rex) {
			throw new RollbackRequiredException();
		}

		throw new CommitRequiredException();
	}

	public void commit(Xid xid)
			throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException, SystemException {
		try {
			this.terminatorTwo.commit(xid, false);
		} catch (XAException ex) {
			// error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR
			switch (ex.errorCode) {
			case XAException.XA_HEURCOM:
				break;
			case XAException.XA_HEURRB:
				throw new HeuristicRollbackException();
			case XAException.XA_HEURMIX:
				throw new HeuristicMixedException();
			case XAException.XA_HEURHAZ:
				throw new SystemException();
			case XAException.XA_RDONLY:
				break;
			case XAException.XAER_RMERR:
				throw new SystemException();
			default:
				// should never happen
				throw new SystemException();
			}
		} catch (RuntimeException ex) {
			throw new SystemException();
		}
	}

	public void rollback(Xid xid)
			throws HeuristicMixedException, HeuristicCommitException, IllegalStateException, SystemException {

		boolean committedExists = false;
		boolean rolledbackExists = false;
		boolean unFinishExists = false;
		boolean errorExists = false;
		try {
			this.terminatorOne.rollback(xid);
			rolledbackExists = true;
		} catch (XAException ex) {
			// error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR
			switch (ex.errorCode) {
			case XAException.XA_HEURCOM:
				committedExists = true;
				break;
			case XAException.XA_HEURRB:
				rolledbackExists = true;
				break;
			case XAException.XA_HEURMIX:
				committedExists = true;
				rolledbackExists = true;
				break;
			case XAException.XA_HEURHAZ:
				unFinishExists = true;
				break;
			case XAException.XA_RDONLY:
				break;
			case XAException.XAER_RMERR:
				errorExists = true;
				break;
			default:
				// should never happen
				errorExists = true;
			}

		} catch (RuntimeException ex) {
			unFinishExists = true;
		}

		try {
			this.terminatorTwo.rollback(xid);
			rolledbackExists = true;
		} catch (XAException ex) {
			// error: XA_HEURHAZ, XA_HEURMIX, XA_HEURCOM, XA_HEURRB, XA_RDONLY, XAER_RMERR
			switch (ex.errorCode) {
			case XAException.XA_HEURCOM:
				committedExists = true;
				break;
			case XAException.XA_HEURRB:
				rolledbackExists = true;
				break;
			case XAException.XA_HEURMIX:
				committedExists = true;
				rolledbackExists = true;
				break;
			case XAException.XA_HEURHAZ:
				unFinishExists = true;
				break;
			case XAException.XA_RDONLY:
				break;
			case XAException.XAER_RMERR:
				errorExists = true;
				break;
			default:
				// should never happen
				errorExists = true;
			}
		} catch (RuntimeException ex) {
			unFinishExists = true;
		}

		if (committedExists && rolledbackExists) {
			throw new HeuristicMixedException();
		} else if (unFinishExists) {
			throw new SystemException(); // hazard
		} else if (errorExists) {
			throw new SystemException();
		} else if (committedExists) {
			throw new HeuristicCommitException();
		}

	}

}
