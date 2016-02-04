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
package org.bytesoft.transaction.supports.rpc;

import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.aware.TransactionBeanFactoryAware;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.byterpc.supports.ServiceFactoryImpl;
import org.bytesoft.byterpc.svc.ServiceFactory;
import org.bytesoft.transaction.TransactionBeanFactory;

public class TransactionServiceFactory extends ServiceFactoryImpl implements ServiceFactory,
		TransactionBeanFactoryAware {

	private TransactionBeanFactory transactionBeanFactory;

	public void initialize() {
		RemoteCoordinator nativeCoordinator = this.transactionBeanFactory.getNativeCoordinator();
		this.putServiceObject(XAResource.class.getName(), XAResource.class, nativeCoordinator);

	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.transactionBeanFactory = tbf;
	}

}
