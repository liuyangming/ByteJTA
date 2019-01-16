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

import javax.transaction.Synchronization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynchronizationList implements Synchronization {
	private static final Logger logger = LoggerFactory.getLogger(SynchronizationList.class);
	private final List<Synchronization> synchronizations = new ArrayList<Synchronization>();

	private boolean beforeCompletionInvoked;
	private boolean finishCompletionInvoked;

	public void registerSynchronizationQuietly(Synchronization sync) {
		SynchronizationImpl synchronization = new SynchronizationImpl(sync);
		this.synchronizations.add(synchronization);
	}

	public synchronized void beforeCompletion() {
		if (this.beforeCompletionInvoked == false) {
			int length = this.synchronizations.size();
			for (int i = 0; i < length; i++) {
				Synchronization synchronization = this.synchronizations.get(i);
				try {
					synchronization.beforeCompletion();
				} catch (RuntimeException error) {
					logger.error(error.getMessage(), error);
				}
			} // end-for

			this.beforeCompletionInvoked = true;
		} // end-if (this.beforeCompletionInvoked == false)
	}

	public synchronized void afterCompletion(int status) {
		if (this.finishCompletionInvoked == false) {
			int length = this.synchronizations.size();
			for (int i = 0; i < length; i++) {
				Synchronization synchronization = this.synchronizations.get(i);
				try {
					synchronization.afterCompletion(status);
				} catch (RuntimeException error) {
					logger.error(error.getMessage(), error);
				}
			} // end-for

			this.finishCompletionInvoked = true;
		} // end-if (this.finishCompletionInvoked == false)
	}

}
