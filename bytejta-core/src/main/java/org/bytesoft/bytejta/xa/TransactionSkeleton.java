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

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.common.TransactionConfigurator;
import org.bytesoft.bytejta.common.TransactionRepository;
import org.bytesoft.transaction.CommitRequiredException;
import org.bytesoft.transaction.RollbackRequiredException;
import org.bytesoft.transaction.xa.TransactionXid;

public class TransactionSkeleton implements XAResource {

	public void commit(Xid xid, boolean onePhase) throws XAException {
		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = branchXid.getGlobalXid();
		TransactionConfigurator configurator = TransactionConfigurator.getInstance();
		TransactionRepository repository = configurator.getTransactionRepository();
		TransactionImpl transaction = repository.getTransaction(globalXid);
		boolean transactionDone = true;
		try {
			if (onePhase) {
				transaction.commit();
			} else {
				transaction.participantCommit();
			}
		} catch (SecurityException ignore) {
			transactionDone = false;
			repository.putErrorTransaction(globalXid, transaction);
			throw new XAException(XAException.XAER_RMERR);
		} catch (IllegalStateException ignore) {
			transactionDone = false;
			repository.putErrorTransaction(globalXid, transaction);
			throw new XAException(XAException.XAER_RMERR);
		} catch (CommitRequiredException ignore) {
			transactionDone = false;
			repository.putErrorTransaction(globalXid, transaction);
			throw new XAException(XAException.XAER_RMERR);
		} catch (RollbackException ignore) {
			throw new XAException(XAException.XA_HEURRB);
		} catch (HeuristicMixedException ignore) {
			transactionDone = false;// TODO
			repository.putErrorTransaction(globalXid, transaction);
			throw new XAException(XAException.XA_HEURMIX);
		} catch (HeuristicRollbackException ignore) {
			throw new XAException(XAException.XA_HEURRB);
		} catch (SystemException ignore) {
			transactionDone = false;
			repository.putErrorTransaction(globalXid, transaction);
			throw new XAException(XAException.XAER_RMERR);
		} finally {
			if (transactionDone) {
				repository.removeErrorTransaction(globalXid);
				repository.removeTransaction(globalXid);
			}
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
		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = branchXid.getGlobalXid();
		TransactionConfigurator configurator = TransactionConfigurator.getInstance();
		TransactionRepository repository = configurator.getTransactionRepository();
		TransactionImpl transaction = repository.getTransaction(globalXid);
		try {
			transaction.participantPrepare();
		} catch (CommitRequiredException crex) {
			return XAResource.XA_OK;
		} catch (RollbackRequiredException rrex) {
			throw new XAException(XAException.XAER_RMERR);
		}
		return XAResource.XA_RDONLY;
	}

	public Xid[] recover(int flag) throws XAException {
		return null;
	}

	public void rollback(Xid xid) throws XAException {
		TransactionXid branchXid = (TransactionXid) xid;
		TransactionXid globalXid = branchXid.getGlobalXid();
		TransactionConfigurator configurator = TransactionConfigurator.getInstance();
		TransactionRepository repository = configurator.getTransactionRepository();
		TransactionImpl transaction = repository.getTransaction(globalXid);

		boolean transactionDone = true;
		try {
			transaction.rollback();
		} catch (RollbackRequiredException rrex) {
			transactionDone = false;
			repository.putErrorTransaction(globalXid, transaction);
			throw new XAException(XAException.XAER_RMERR);
		} catch (SystemException ex) {
			transactionDone = false;
			repository.putErrorTransaction(globalXid, transaction);
			throw new XAException(XAException.XAER_RMERR);
		} catch (RuntimeException rrex) {
			transactionDone = false;
			repository.putErrorTransaction(globalXid, transaction);
			SystemException ex = new SystemException();
			ex.initCause(rrex);
			throw new XAException(XAException.XAER_RMERR);
		} finally {
			if (transactionDone) {
				repository.removeErrorTransaction(globalXid);
				repository.removeTransaction(globalXid);
			}
		}
	}

	public boolean setTransactionTimeout(int seconds) throws XAException {
		return false;
	}

	public void start(Xid xid, int flags) throws XAException {
	}

}
