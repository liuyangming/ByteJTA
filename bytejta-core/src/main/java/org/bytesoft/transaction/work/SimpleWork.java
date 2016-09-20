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

import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkEvent;
import javax.resource.spi.work.WorkListener;

public class SimpleWork implements Runnable {

	private Object source;
	private Work work;
	private WorkListener workListener;

	public void run() {
		this.workListener.workStarted(new WorkEvent(this.source, WorkEvent.WORK_STARTED, this.work, null));
		this.work.run();
		this.workListener.workCompleted(new WorkEvent(this.source, WorkEvent.WORK_COMPLETED, this.work, null));
	}

	public void setSource(Object source) {
		this.source = source;
	}

	public void setWork(Work work) {
		this.work = work;
	}

	public void setWorkListener(WorkListener workListener) {
		this.workListener = workListener;
	}

}
