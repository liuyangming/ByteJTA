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

import org.bytesoft.bytejta.supports.invoke.InvocationContext;
import org.bytesoft.bytejta.supports.invoke.InvocationContextRegistry;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;

public class DubboRemoteCoordinator implements InvocationHandler {

	private InvocationContext invocationContext;
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
					int serverPort = this.invocationContext == null ? 0 : this.invocationContext.getServerPort();
					return this.invocationContext == null ? null : String.format("%s:%s", serverHost, serverPort);
				} else if ("recoveryCommit".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else if ("recoveryRollback".equals(methodName)) {
					return this.invokeCoordinator(proxy, method, args);
				} else {
					throw new XAException(XAException.XAER_RMFAIL);
				}
			} else if (XAResource.class.equals(clazz)) {
				if ("prepare".equals(methodName)) {
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

	public InvocationContext getInvocationContext() {
		return invocationContext;
	}

	public void setInvocationContext(InvocationContext invocationContext) {
		this.invocationContext = invocationContext;
	}

	public RemoteCoordinator getRemoteCoordinator() {
		return remoteCoordinator;
	}

	public void setRemoteCoordinator(RemoteCoordinator remoteCoordinator) {
		this.remoteCoordinator = remoteCoordinator;
	}

}
