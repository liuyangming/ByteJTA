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
package org.bytesoft.transaction;

import javax.transaction.Synchronization;

public class SynchronizationImpl implements Synchronization {
	private Synchronization delegate;
	private boolean beforeCompletionRequired;
	private boolean afterCompletionRequired;

	public SynchronizationImpl(Synchronization sync) {
		if (sync == null) {
			throw new IllegalArgumentException();
		} else {
			this.delegate = sync;
			this.beforeCompletionRequired = true;
			this.afterCompletionRequired = true;
		}
	}

	public void beforeCompletion() {
		if (this.beforeCompletionRequired) {
			try {
				this.delegate.beforeCompletion();
			} finally {
				this.beforeCompletionRequired = false;
			}
		}
	}

	public void afterCompletion(int status) {
		if (this.afterCompletionRequired) {
			try {
				this.delegate.afterCompletion(status);
			} finally {
				this.afterCompletionRequired = false;
			}
		}
	}

	public String toString() {
		return String.format("[%s] delegate: %s", this.getClass().getSimpleName(), this.delegate);
	}
}
