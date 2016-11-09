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
package org.bytesoft.transaction.internal;

import java.util.ArrayList;
import java.util.List;

import org.bytesoft.transaction.supports.TransactionListener;
import org.bytesoft.transaction.supports.TransactionListenerAdapter;
import org.bytesoft.transaction.xa.TransactionXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionListenerList extends TransactionListenerAdapter {
	static final Logger logger = LoggerFactory.getLogger(TransactionListenerList.class.getSimpleName());

	private final List<TransactionListener> listeners = new ArrayList<TransactionListener>();

	public void registerTransactionListener(TransactionListener listener) {
		this.listeners.add(listener);
	}

	public void onPrepareStart(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onPrepareStart(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onPrepareSuccess(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onPrepareSuccess(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onPrepareFailure(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onPrepareFailure(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitStart(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitStart(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitSuccess(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitSuccess(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitFailure(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitFailure(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitHeuristicMixed(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitHeuristicMixed(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onCommitHeuristicRolledback(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onCommitHeuristicRolledback(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onRollbackStart(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onRollbackStart(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onRollbackSuccess(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onRollbackSuccess(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onRollbackFailure(TransactionXid xid) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.onRollbackFailure(xid);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

}
