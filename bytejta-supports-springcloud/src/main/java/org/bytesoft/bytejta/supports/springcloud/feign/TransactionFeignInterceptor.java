/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.supports.springcloud.feign;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.bytesoft.bytejta.supports.springcloud.SpringCloudBeanRegistry;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class TransactionFeignInterceptor
		implements feign.RequestInterceptor, TransactionEndpointAware, ApplicationContextAware {
	static final String HEADER_TRANCACTION_KEY = "org.bytesoft.bytejta.transaction";
	static final String HEADER_PROPAGATION_KEY = "org.bytesoft.bytejta.propagation";

	private String identifier;
	private ApplicationContext applicationContext;

	public void apply(feign.RequestTemplate template) {
		final SpringCloudBeanRegistry beanRegistry = SpringCloudBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionManager transactionManager = beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		if (transaction == null) {
			return;
		}

		try {
			TransactionContext transactionContext = transaction.getTransactionContext();
			byte[] byteArray = CommonUtils.serializeObject(transactionContext);

			String transactionText = ByteUtils.byteArrayToString(byteArray);

			Map<String, Collection<String>> headers = template.headers();
			if (headers.containsKey(HEADER_TRANCACTION_KEY) == false) {
				template.header(HEADER_TRANCACTION_KEY, transactionText);
			}

			if (headers.containsKey(HEADER_PROPAGATION_KEY) == false) {
				template.header(HEADER_PROPAGATION_KEY, identifier);
			}

		} catch (IOException ex) {
			throw new RuntimeException("Error occurred while preparing the transaction context!", ex);
		}
	}

	public void setEndpoint(String identifier) {
		this.identifier = identifier;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

}