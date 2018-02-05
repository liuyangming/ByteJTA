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
package org.bytesoft.bytejta.supports.springcloud.hystrix;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import org.bytesoft.bytejta.TransactionImpl;
import org.bytesoft.bytejta.supports.springcloud.SpringCloudBeanRegistry;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionHystrixFeignHandler implements InvocationHandler {
	static final Logger logger = LoggerFactory.getLogger(TransactionHystrixFeignHandler.class);

	private InvocationHandler delegate;

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if (Object.class.equals(method.getDeclaringClass())) {
			return this.delegate.invoke(proxy, method, args);
		} else {
			final SpringCloudBeanRegistry beanRegistry = SpringCloudBeanRegistry.getInstance();
			TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
			TransactionManager transactionManager = beanFactory.getTransactionManager();

			TransactionImpl transaction = //
					(TransactionImpl) transactionManager.getTransactionQuietly();
			if (transaction == null) {
				return this.delegate.invoke(proxy, method, args);
			} else {
				Method targetMethod = TransactionHystrixInvocationHandler.class.getDeclaredMethod(
						TransactionHystrixBeanPostProcessor.HYSTRIX_INVOKER_NAME,
						new Class<?>[] { TransactionHystrixInvocation.class });
				TransactionHystrixInvocation invocation = new TransactionHystrixInvocation();
				invocation.setThread(Thread.currentThread());
				invocation.setMethod(method);
				invocation.setArgs(args);
				Object[] targetArgs = new Object[] { invocation };
				return this.delegate.invoke(proxy, targetMethod, targetArgs);
			}
		}
	}

	public InvocationHandler getDelegate() {
		return delegate;
	}

	public void setDelegate(InvocationHandler delegate) {
		this.delegate = delegate;
	}

}
