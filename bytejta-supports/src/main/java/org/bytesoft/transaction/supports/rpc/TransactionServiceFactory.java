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

import org.bytesoft.bytejta.common.TransactionConfigurator;
import org.bytesoft.byterpc.supports.ServiceFactoryImpl;
import org.bytesoft.byterpc.svc.ServiceFactory;

public class TransactionServiceFactory extends ServiceFactoryImpl implements ServiceFactory {

	public void initialize() {

		TransactionConfigurator configurator = TransactionConfigurator.getInstance();
		XAResource transactionSkeleton = configurator.getTransactionSkeleton();
		this.putServiceObject(XAResource.class.getName(), XAResource.class, transactionSkeleton);

	}

}
