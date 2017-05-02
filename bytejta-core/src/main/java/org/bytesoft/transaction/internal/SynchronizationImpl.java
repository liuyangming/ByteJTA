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

import javax.transaction.Synchronization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SynchronizationImpl implements Synchronization {
	static final Logger logger = LoggerFactory.getLogger(SynchronizationImpl.class);

	private Synchronization delegate;
	private boolean beforeRequired;
	private boolean finishRequired;

	public SynchronizationImpl(Synchronization sync) {
		if (sync == null) {
			throw new IllegalArgumentException();
		} else {
			this.delegate = sync;
			this.beforeRequired = true;
			this.finishRequired = true;
		}
	}

	public void beforeCompletion() {
		if (this.beforeRequired) {
			try {
				this.delegate.beforeCompletion();
			} catch (RuntimeException rex) {
				// ignore
			} finally {
				this.beforeRequired = false;
			}
		}
	}

	public void afterCompletion(int status) {
		if (this.finishRequired) {
			try {
				this.delegate.afterCompletion(status);
			} catch (RuntimeException rex) {
				// ignore
			} finally {
				this.finishRequired = false;
			}
		}
	}

	public String toString() {
		return String.format("[%s] delegate: %s", this.getClass().getSimpleName(), this.delegate);
	}
}
