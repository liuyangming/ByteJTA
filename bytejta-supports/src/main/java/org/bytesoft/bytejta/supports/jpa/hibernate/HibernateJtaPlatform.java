/**
 * Copyright 2014-2018 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.supports.jpa.hibernate;

import org.bytesoft.bytejta.TransactionBeanFactoryImpl;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.hibernate.engine.transaction.jta.platform.internal.AbstractJtaPlatform;

public class HibernateJtaPlatform extends AbstractJtaPlatform {
	private static final long serialVersionUID = 1L;

	protected javax.transaction.TransactionManager locateTransactionManager() {
		TransactionBeanFactory beanFactory = TransactionBeanFactoryImpl.getInstance();
		return beanFactory == null ? null : beanFactory.getTransactionManager();
	}

	protected javax.transaction.UserTransaction locateUserTransaction() {
		return null;
	}
}
