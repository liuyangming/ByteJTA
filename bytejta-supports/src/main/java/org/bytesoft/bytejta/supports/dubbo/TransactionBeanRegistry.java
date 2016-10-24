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
package org.bytesoft.bytejta.supports.dubbo;

import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;

public class TransactionBeanRegistry implements TransactionBeanFactoryAware {
	private static final TransactionBeanRegistry instance = new TransactionBeanRegistry();

	private TransactionBeanFactory beanFactory;
	private RemoteCoordinator consumeCoordinator;

	private TransactionBeanRegistry() {
		if (instance != null) {
			throw new IllegalStateException();
		}
	}

	public static TransactionBeanRegistry getInstance() {
		return instance;
	}

	public void setBeanFactory(TransactionBeanFactory tbf) {
		this.beanFactory = tbf;
	}

	public TransactionBeanFactory getBeanFactory() {
		return beanFactory;
	}

	public RemoteCoordinator getConsumeCoordinator() {
		return consumeCoordinator;
	}

	public void setConsumeCoordinator(RemoteCoordinator consumeCoordinator) {
		this.consumeCoordinator = consumeCoordinator;
	}

}
