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

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.bytesoft.transaction.supports.TransactionResourceListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionResourceListenerList implements TransactionResourceListener {
	static final Logger logger = LoggerFactory.getLogger(TransactionResourceListenerList.class.getSimpleName());

	private final List<TransactionResourceListener> listeners = new ArrayList<TransactionResourceListener>();

	public void registerTransactionResourceListener(TransactionResourceListener listener) {
		this.listeners.add(listener);
	}

	public void onEnlistResource(Xid xid, XAResource xares) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionResourceListener listener = this.listeners.get(i);
				listener.onEnlistResource(xid, xares);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

	public void onDelistResource(Xid xid, XAResource xares) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionResourceListener listener = this.listeners.get(i);
				listener.onDelistResource(xid, xares);
			} catch (RuntimeException rex) {
				logger.error(rex.getMessage(), rex);
			}
		}
	}

}
