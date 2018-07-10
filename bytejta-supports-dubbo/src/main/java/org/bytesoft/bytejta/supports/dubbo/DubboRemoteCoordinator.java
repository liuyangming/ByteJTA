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
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinatorRegistry;
import org.bytesoft.common.utils.CommonUtils;

public class DubboRemoteCoordinator implements InvocationHandler {

	private InvocationContext invocationContext;
	private RemoteCoordinator proxyCoordinator;
	private RemoteCoordinator remoteCoordinator;

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		InvocationContextRegistry registry = InvocationContextRegistry.getInstance();
		try {
			registry.associateInvocationContext(this.invocationContext);
			Class<?> clazz = method.getDeclaringClass();
			String methodName = method.getName();
			if (Object.class.equals(clazz)) {
				return method.invoke(this, args);
			} else if (RemoteCoordinator.class.equals(clazz)) {
				if ("getIdentifier".equals(methodName)) {
					String serverHost = this.invocationContext == null ? null : this.invocationContext.getServerHost();
					String serviceKey = this.invocationContext == null ? null : this.invocationContext.getServiceKey();
					int serverPort = this.invocationContext == null ? 0 : this.invocationContext.getServerPort();

					if (this.invocationContext == null) {
						return null;
					} else if (StringUtils.isNotBlank(serviceKey)) {
						return String.format("%s:%s:%s", serverHost, serviceKey, serverPort);
					} else {
						Object application = this.getParticipantsApplication(proxy, method, args);
						return String.format("%s:%s:%s", serverHost, application, serverPort);
					}
				} else if ("getApplication".equals(methodName)) {
					if (this.invocationContext == null) {
						return null;
					} else if (StringUtils.isNotBlank(this.invocationContext.getServiceKey())) {
						return this.invocationContext.getServiceKey();
					} else {
						return this.getParticipantsApplication(proxy, method, args);
					}
				} else {
					throw new XAException(XAException.XAER_RMFAIL);
				}
			} else if (XAResource.class.equals(clazz)) {
				String serverHost = this.invocationContext == null ? null : this.invocationContext.getServerHost();
				int serverPort = this.invocationContext == null ? 0 : this.invocationContext.getServerPort();
				String remoteAddr = String.format("%s:%s", serverHost, serverPort);
				if ("start".equals(methodName)) {
					RemoteCoordinatorRegistry coordinatorRegistry = RemoteCoordinatorRegistry.getInstance();
					if (this.invocationContext == null) {
						throw new IllegalAccessException();
					} else if (coordinatorRegistry.containsApplication(remoteAddr)) {
						return null;
					} else {
						return this.invokeCoordinator(proxy, method, args);
					}
				} else if ("prepare".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("commit".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("rollback".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("recover".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("forget".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else {
					throw new XAException(XAException.XAER_RMFAIL);
				}
			} else {
				throw new IllegalAccessException();
			}
		} finally {
			registry.desociateInvocationContext();
		}
	}

	public Object invokeCoordinator(Object proxy, Method method, Object[] args) throws Throwable {
		try {
			return method.invoke(this.remoteCoordinator, args);
		} catch (InvocationTargetException ex) {
			throw ex.getTargetException();
		} catch (IllegalAccessException ex) {
			throw new RuntimeException(ex);
		}
	}

	private Object getParticipantsApplication(Object proxy, Method method, Object[] args) throws Throwable {
		RemoteCoordinatorRegistry coordinatorRegistry = RemoteCoordinatorRegistry.getInstance();

		Object result = this.invokeCoordinator(proxy, method, args);
		String instanceId = CommonUtils.getApplication(String.valueOf(result));
		this.invocationContext.setServiceKey(instanceId);

		if (StringUtils.isNotBlank(instanceId) && coordinatorRegistry.getRemoteCoordinator(instanceId) == null) {
			String[] values = instanceId == null ? new String[0] : instanceId.split("\\s*:\\s*");

			String targetAddr = values.length == 3 ? values[0] : StringUtils.EMPTY;
			String targetName = values.length == 3 ? values[1] : StringUtils.EMPTY;
			String targetPort = values.length == 3 ? values[2] : String.valueOf(0);

			String remoteAddr = StringUtils.isBlank(targetAddr) && StringUtils.isBlank(targetPort) //
					? StringUtils.EMPTY : String.format("%s:%s", targetAddr, targetPort);

			coordinatorRegistry.putApplication(remoteAddr, targetName);
			coordinatorRegistry.putRemoteAddr(instanceId, remoteAddr);

			RemoteCoordinator remoteAddrCoordinator = //
					coordinatorRegistry.getRemoteCoordinatorByAddr(remoteAddr);
			if (remoteAddrCoordinator == null) {
				coordinatorRegistry.putRemoteCoordinatorByAddr(remoteAddr, this.proxyCoordinator);
			}

			RemoteCoordinator instanceCoordinator = coordinatorRegistry.getRemoteCoordinator(instanceId);
			if (instanceCoordinator == null) {
				coordinatorRegistry.putRemoteCoordinator(instanceId, this.proxyCoordinator);
			}
		}

		return instanceId;
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
