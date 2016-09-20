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
package org.bytesoft.transaction.adapter;

import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;

public class ResourceAdapterImpl implements ResourceAdapter {

	private Work transactionWork;
	private WorkManager workManager;

	public void start(BootstrapContext ctx) throws ResourceAdapterInternalException {

		this.workManager = ctx.getWorkManager();
		try {
			this.workManager.startWork(this.transactionWork);
		} catch (WorkException ex) {
			this.stop();
			throw new ResourceAdapterInternalException(ex);
		} catch (RuntimeException ex) {
			this.stop();
			throw new ResourceAdapterInternalException(ex);
		}
	}

	public void stop() {
		this.transactionWork.release();
	}

	public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) throws ResourceException {
	}

	public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) {
	}

	public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException {
		return new XAResource[0];
	}

	public Work getTransactionWork() {
		return transactionWork;
	}

	public void setTransactionWork(Work transactionWork) {
		this.transactionWork = transactionWork;
	}

}
