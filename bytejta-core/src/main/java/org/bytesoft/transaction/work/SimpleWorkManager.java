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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.resource.spi.work.ExecutionContext;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkListener;
import javax.resource.spi.work.WorkManager;

import org.apache.log4j.Logger;

public class SimpleWorkManager implements WorkManager {
	static final Logger logger = Logger.getLogger(SimpleWorkManager.class.getSimpleName());

	private final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());
	// private final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);

	public void doWork(Work work) throws WorkException {
		this.doWork(work, 1800 * 1000L, null, null);
	}

	public void doWork(Work work, long startTimeout, ExecutionContext execContext, WorkListener workListener)
			throws WorkException {
		SimpleWorkListener wrappedListener = new SimpleWorkListener(workListener);
		wrappedListener.workAccepted(new WorkEvent(this, WorkEvent.WORK_ACCEPTED, work, null));
		SimpleWork task = new SimpleWork();
		task.setWork(work);
		task.setWorkListener(wrappedListener);
		Future<?> future = this.executor.submit(task);
		try {
			future.get();
		} catch (CancellationException ex) {
			wrappedListener.workCompleted(new WorkEvent(this, WorkEvent.WORK_REJECTED, work, new WorkException(ex)));
		} catch (InterruptedException ex) {
			wrappedListener.workCompleted(new WorkEvent(this, WorkEvent.WORK_COMPLETED, work, new WorkException(ex)));
		} catch (ExecutionException ex) {
			wrappedListener.workCompleted(new WorkEvent(this, WorkEvent.WORK_COMPLETED, work, new WorkException(ex)));
		}
	}

	public long startWork(Work work) throws WorkException {
		return this.startWork(work, 1800 * 1000L, null, null);
	}

	public long startWork(Work work, long startTimeout, ExecutionContext execContext, WorkListener workListener)
			throws WorkException {
		SimpleWorkListener wrappedListener = new SimpleWorkListener(workListener);
		wrappedListener.workAccepted(new WorkEvent(this, WorkEvent.WORK_ACCEPTED, work, null));
		SimpleWork task = new SimpleWork();
		task.setSource(this);
		task.setWork(work);
		task.setWorkListener(wrappedListener);
		this.executor.submit(task);
		return wrappedListener.waitForStart();
	}

	public void scheduleWork(Work work) throws WorkException {
		throw new WorkException("not supported yet!");
	}

	public void scheduleWork(Work work, long startTimeout, ExecutionContext execContext, WorkListener workListener)
			throws WorkException {
		// SimpleWorkListener wrappedListener = new SimpleWorkListener(workListener);
		// wrappedListener.workAccepted(new WorkEvent(this, WorkEvent.WORK_ACCEPTED, work, null));
		// SimpleWork task = new SimpleWork();
		// task.setSource(this);
		// task.setWork(work);
		// task.setWorkListener(wrappedListener);
		// // ScheduledFuture<?> future =
		// this.scheduled.scheduleAtFixedRate(task, 0, 1000, TimeUnit.MILLISECONDS);
		throw new WorkException("not supported yet!");
	}

}
