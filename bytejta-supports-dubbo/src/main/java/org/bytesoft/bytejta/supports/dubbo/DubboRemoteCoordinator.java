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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.supports.internal.RemoteCoordinatorRegistry;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.TransactionParticipant;
import org.bytesoft.transaction.remote.RemoteAddr;
import org.bytesoft.transaction.remote.RemoteCoordinator;
import org.bytesoft.transaction.remote.RemoteNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DubboRemoteCoordinator implements InvocationHandler {
	static final Logger logger = LoggerFactory.getLogger(DubboRemoteCoordinator.class);

	private InvocationContext invocationContext;
	private RemoteCoordinator proxyCoordinator;
	private RemoteCoordinator remoteCoordinator;

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		RemoteCoordinatorRegistry participantRegistry = RemoteCoordinatorRegistry.getInstance();
		InvocationContextRegistry invocationRegistry = InvocationContextRegistry.getInstance();
		try {
			invocationRegistry.associateInvocationContext(this.invocationContext);
			Class<?> clazz = method.getDeclaringClass();
			String methodName = method.getName();
			Class<?> returnType = method.getReturnType();
			if (Object.class.equals(clazz)) {
				return method.invoke(this, args);
			} else if (TransactionParticipant.class.equals(clazz)) {
				if ("getIdentifier".equals(methodName)) {
					return this.getParticipantsIdentifier(proxy, method, args);
				} else {
					throw new XAException(XAException.XAER_RMFAIL);
				}
			} else if (RemoteCoordinator.class.equals(clazz)) {
				if ("getApplication".equals(methodName)) {
					return this.getParticipantsApplication(proxy, method, args);
				} else if ("getRemoteAddr".equals(methodName) && RemoteAddr.class.equals(returnType)) {
					String identifier = this.getParticipantsIdentifier(proxy, method, args);
					return identifier == null ? null : CommonUtils.getRemoteAddr(identifier);
				} else if ("getRemoteNode".equals(methodName) && RemoteNode.class.equals(returnType)) {
					String identifier = this.getParticipantsIdentifier(proxy, method, args);
					return identifier == null ? null : CommonUtils.getRemoteNode(identifier);
				} else {
					throw new XAException(XAException.XAER_RMFAIL);
				}
			} else if (XAResource.class.equals(clazz)) {
				String serverHost = this.invocationContext == null ? null : this.invocationContext.getServerHost();
				int serverPort = this.invocationContext == null ? 0 : this.invocationContext.getServerPort();
				String remoteText = String.format("%s:%s:%s", serverHost, null, serverPort);
				RemoteAddr remoteAddr = CommonUtils.getRemoteAddr(remoteText);
				if ("start".equals(methodName)) {
					if (this.invocationContext == null) {
						throw new IllegalAccessException();
					} else if (participantRegistry.getRemoteNode(remoteAddr) != null) {
						return null;
					} else {
						return this.invokeForGeneric(proxy, method, args);
					}
				} else if ("prepare".equals(methodName)) {
					return this.invokeForGeneric(proxy, method, args);
				} else if ("commit".equals(methodName)) {
					return this.invokeForGeneric(proxy, method, args);
				} else if ("rollback".equals(methodName)) {
					return this.invokeForGeneric(proxy, method, args);
				} else if ("recover".equals(methodName)) {
					return this.invokeForGeneric(proxy, method, args);
				} else if ("forget".equals(methodName)) {
					return this.invokeForGeneric(proxy, method, args);
				} else {
					throw new XAException(XAException.XAER_RMFAIL);
				}
			} else {
				throw new IllegalAccessException();
			}
		} finally {
			invocationRegistry.desociateInvocationContext();
		}
	}

	private String getParticipantsIdentifier(Object proxy, Method method, Object[] args) throws Throwable {
		if (this.invocationContext == null) {
			return null;
		}

		String serverHost = this.invocationContext == null ? null : this.invocationContext.getServerHost();
		String serviceKey = this.invocationContext == null ? null : this.invocationContext.getServiceKey();
		int serverPort = this.invocationContext == null ? 0 : this.invocationContext.getServerPort();
		if (StringUtils.isNotBlank(serviceKey)) {
			return String.format("%s:%s:%s", serverHost, serviceKey, serverPort);
		} else {
			Object application = this.getParticipantsApplication(proxy, method, args);
			return String.format("%s:%s:%s", serverHost, application, serverPort);
		}
	}

	private Object getParticipantsApplication(Object proxy, Method method, Object[] args) throws Throwable {
		if (this.invocationContext == null) {
			return null;
		} else if (StringUtils.isNotBlank(this.invocationContext.getServiceKey())) {
			return this.invocationContext.getServiceKey();
		}
		RemoteCoordinatorRegistry participantRegistry = RemoteCoordinatorRegistry.getInstance();

		String serverHost = this.invocationContext.getServerHost();
		int serverPort = this.invocationContext.getServerPort();
		RemoteAddr remoteAddr = CommonUtils.getRemoteAddr(String.format("%s:%s:%s", serverHost, null, serverPort));
		RemoteNode remoteNode = participantRegistry.getRemoteNode(remoteAddr);
		if (remoteNode != null) {
			this.invocationContext.setServiceKey(remoteNode.getServiceKey());
			return this.invocationContext.getServiceKey();
		}

		Object result = this.invokeForSpecifiedDestination(proxy, method, args);
		String application = CommonUtils.getApplication(String.valueOf(result));
		this.invocationContext.setServiceKey(application);

		String instanceId = String.format("%s:%s:%s", serverHost, application, serverPort);
		remoteNode = CommonUtils.getRemoteNode(instanceId);

		if (StringUtils.isNotBlank(instanceId) && remoteAddr != null && remoteNode != null
				&& participantRegistry.containsRemoteNode(remoteAddr) == false) {
			participantRegistry.putParticipant(remoteNode.getServiceKey(), this.proxyCoordinator);
			participantRegistry.putRemoteNode(remoteAddr, remoteNode);
		}

		return application;
	}

	public Object invokeForGeneric(Object proxy, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(this.remoteCoordinator, args);
		} catch (IllegalArgumentException error) {
			logger.warn("Error occurred!", error);
			throw new XAException(XAException.XAER_RMERR);
		} catch (InvocationTargetException error) {
			throw error.getTargetException();
		} catch (IllegalAccessException error) {
			logger.warn("Error occurred!", error);
			throw new XAException(XAException.XAER_RMERR);
		}
	}

	public Object invokeForSpecifiedDestination(Object proxy, Method method, Object[] args) throws Throwable {
		RemoteCoordinatorRegistry participantRegistry = RemoteCoordinatorRegistry.getInstance();
		String serverHost = this.invocationContext.getServerHost();
		int serverPort = this.invocationContext.getServerPort();
		String address = String.format("%s:%s:%s", serverHost, null, serverPort);
		RemoteAddr remoteAddr = CommonUtils.getRemoteAddr(address);

		RemoteCoordinator participant = participantRegistry.getPhysicalInstance(remoteAddr);
		if (participant == null) {
			throw new XAException(XAException.XAER_RMERR);
		}

		try {
			return method.invoke(participant, args);
		} catch (IllegalArgumentException error) {
			logger.warn("Error occurred!", error);
			throw new XAException(XAException.XAER_RMERR);
		} catch (InvocationTargetException error) {
			throw error.getTargetException();
		} catch (IllegalAccessException error) {
			logger.warn("Error occurred!", error);
			throw new XAException(XAException.XAER_RMERR);
		}
	}

	public InvocationContext getInvocationContext() {
		return invocationContext;
	}

	public void setInvocationContext(InvocationContext invocationContext) {
		this.invocationContext = invocationContext;
	}

	public RemoteCoordinator getProxyCoordinator() {
		return proxyCoordinator;
	}

	public void setProxyCoordinator(RemoteCoordinator proxyCoordinator) {
		this.proxyCoordinator = proxyCoordinator;
	}

	public RemoteCoordinator getRemoteCoordinator() {
		return remoteCoordinator;
	}

	public void setRemoteCoordinator(RemoteCoordinator remoteCoordinator) {
		this.remoteCoordinator = remoteCoordinator;
	}

}
