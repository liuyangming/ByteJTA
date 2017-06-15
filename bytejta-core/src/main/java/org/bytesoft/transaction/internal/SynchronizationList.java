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

public class SynchronizationList implements Synchronization {
	private final List<Synchronization> synchronizations = new ArrayList<Synchronization>();

	public void registerSynchronizationQuietly(Synchronization sync) {
		SynchronizationImpl synchronization = new SynchronizationImpl(sync);
		this.synchronizations.add(synchronization);
	}

	public void beforeCompletion() {
		int length = this.synchronizations.size();
		for (int i = 0; i < length; i++) {
			Synchronization synchronization = this.synchronizations.get(i);
			synchronization.beforeCompletion();
		} // end-for
	}

	public void afterCompletion(int status) {
		int length = this.synchronizations.size();
		for (int i = 0; i < length; i++) {
			Synchronization synchronization = this.synchronizations.get(i);
			synchronization.afterCompletion(status);
		} // end-for
	}

}
