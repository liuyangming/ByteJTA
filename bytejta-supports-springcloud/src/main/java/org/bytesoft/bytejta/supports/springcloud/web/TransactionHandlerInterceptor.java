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
package org.bytesoft.bytejta.supports.springcloud.web;

import java.util.Base64;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.bytesoft.bytejta.supports.rpc.TransactionRequestImpl;
import org.bytesoft.bytejta.supports.rpc.TransactionResponseImpl;
import org.bytesoft.bytejta.supports.springcloud.SpringCloudBeanRegistry;
import org.bytesoft.bytejta.supports.springcloud.controller.TransactionCoordinatorController;
import org.bytesoft.common.utils.SerializeUtils;
import org.bytesoft.transaction.Transaction;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.TransactionContext;
import org.bytesoft.transaction.TransactionManager;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.supports.rpc.TransactionInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

public class TransactionHandlerInterceptor implements HandlerInterceptor, TransactionEndpointAware, ApplicationContextAware {
	private static final Logger logger = LoggerFactory.getLogger(TransactionHandlerInterceptor.class);

	static final String HEADER_TRANCACTION_KEY = "X-BYTEJTA-TRANSACTION"; // org.bytesoft.bytejta.transaction
	static final String HEADER_PROPAGATION_KEY = "X-BYTEJTA-PROPAGATION"; // org.bytesoft.bytejta.propagation

	private String identifier;
	private ApplicationContext applicationContext;

	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
		if (HandlerMethod.class.isInstance(handler) == false) {
			logger.warn("TransactionHandlerInterceptor cannot handle current request(uri= {}, handler= {}) correctly.",
					request.getRequestURI(), handler);
			return true;
		}

		HandlerMethod hm = (HandlerMethod) handler;
		Class<?> clazz = hm.getBeanType();
		if (TransactionCoordinatorController.class.equals(clazz)) {
			return true;
		} else if (ErrorController.class.isInstance(hm.getBean())) {
			return true;
		}

		String transactionStr = request.getHeader(HEADER_TRANCACTION_KEY);
		if (StringUtils.isBlank(transactionStr)) {
			return true;
		}

		String propagationStr = request.getHeader(HEADER_PROPAGATION_KEY);

		String transactionText = StringUtils.trimToNull(transactionStr);
		String propagationText = StringUtils.trimToNull(propagationStr);

		SpringCloudBeanRegistry beanRegistry = SpringCloudBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionInterceptor transactionInterceptor = beanFactory.getTransactionInterceptor();

		byte[] byteArray = transactionText == null ? new byte[0] : Base64.getDecoder().decode(transactionText);

		TransactionContext transactionContext = null;
		if (byteArray != null && byteArray.length > 0) {
			transactionContext = (TransactionContext) SerializeUtils.deserializeObject(byteArray);
			transactionContext.setPropagated(true);
			transactionContext.setPropagatedBy(propagationText);
		}

		TransactionRequestImpl req = new TransactionRequestImpl();
		req.setTransactionContext(transactionContext);
		req.setTargetTransactionCoordinator(beanRegistry.getConsumeCoordinator(propagationText));

		transactionInterceptor.afterReceiveRequest(req);

		TransactionManager transactionManager = beanFactory.getTransactionManager();
		Transaction transaction = transactionManager.getTransactionQuietly();
		byte[] responseByteArray = SerializeUtils.serializeObject(transaction.getTransactionContext());
		String responseTransactionStr = Base64.getEncoder().encodeToString(responseByteArray);
		response.setHeader(HEADER_TRANCACTION_KEY, responseTransactionStr);
		response.setHeader(HEADER_PROPAGATION_KEY, this.identifier);

		return true;
	}

	public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView)
			throws Exception {
	}

	public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
			throws Exception {
		if (HandlerMethod.class.isInstance(handler) == false) {
			return;
		}

		HandlerMethod hm = (HandlerMethod) handler;
		Class<?> clazz = hm.getBeanType();
		if (TransactionCoordinatorController.class.equals(clazz)) {
			return;
		} else if (ErrorController.class.isInstance(hm.getBean())) {
			return;
		}

		String transactionText = request.getHeader(HEADER_TRANCACTION_KEY);
		if (StringUtils.isBlank(transactionText)) {
			return;
		}

		SpringCloudBeanRegistry beanRegistry = SpringCloudBeanRegistry.getInstance();
		TransactionBeanFactory beanFactory = beanRegistry.getBeanFactory();
		TransactionManager transactionManager = beanFactory.getTransactionManager();
		TransactionInterceptor transactionInterceptor = beanFactory.getTransactionInterceptor();

		Transaction transaction = transactionManager.getTransactionQuietly();
		TransactionContext transactionContext = transaction.getTransactionContext();

		// byte[] byteArray = SerializeUtils.serializeObject(transactionContext);
		// String transactionStr = ByteUtils.byteArrayToString(byteArray);
		// response.setHeader(HEADER_TRANCACTION_KEY, transactionStr);
		// response.setHeader(HEADER_PROPAGATION_KEY, this.identifier);

		TransactionResponseImpl resp = new TransactionResponseImpl();
		resp.setTransactionContext(transactionContext);
		resp.setSourceTransactionCoordinator(beanRegistry.getConsumeCoordinator(null));

		transactionInterceptor.beforeSendResponse(resp);

	}

	public String getEndpoint() {
		return this.identifier;
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