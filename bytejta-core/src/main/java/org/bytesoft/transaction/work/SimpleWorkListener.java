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
package org.bytesoft.transaction.work;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkListener;

import org.apache.log4j.Logger;

public class SimpleWorkListener implements WorkListener {
	static final Logger logger = Logger.getLogger(SimpleWorkListener.class.getSimpleName());

	private long acceptedTime = -1;
	private long startedTime = -1;

	private final WorkListener delegate;
	private final Lock lock = new ReentrantLock();
	private final Condition condition = this.lock.newCondition();

	public SimpleWorkListener(WorkListener workListener) {
		this.delegate = workListener;
	}

	public long waitForStart() {
		try {
			this.lock.lock();
			while (this.acceptedTime < 0 || this.startedTime < 0) {
				try {
					this.condition.await();
				} catch (InterruptedException ex) {
					logger.debug(ex.getMessage());
				}
			}
			return this.startedTime - this.acceptedTime;
		} finally {
			this.lock.unlock();
		}
	}

	public void signalStarted() {
		try {
			this.lock.lock();
			this.condition.signalAll();
		} finally {
			this.lock.unlock();
		}
	}

	public void workAccepted(WorkEvent event) {
		if (this.delegate != null) {
			delegate.workAccepted(event);
		}
		this.acceptedTime = System.currentTimeMillis();
	}

	public void workCompleted(WorkEvent event) {
		if (this.delegate != null) {
			delegate.workCompleted(event);
		}
	}

	public void workRejected(WorkEvent event) {
		if (this.delegate != null) {
			delegate.workRejected(event);
		}
	}

	public void workStarted(WorkEvent event) {
		if (this.delegate != null) {
			delegate.workStarted(event);
		}
		this.startedTime = System.currentTimeMillis();
		this.signalStarted();
	}

}
