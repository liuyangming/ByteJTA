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
package org.bytesoft.bytejta.supports.resource;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import javax.jms.XAConnectionFactory;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.sql.XADataSource;

import org.bytesoft.bytejta.supports.jdbc.LocalXADataSource;

public class ManagedConnectionFactoryHandler implements InvocationHandler {

	private final Object delegate;
	private String identifier;

	public ManagedConnectionFactoryHandler(Object xads) {
		this.delegate = xads;
	}

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> declaringClass = method.getDeclaringClass();
		Class<?> returningClass = method.getReturnType();

		Object resultObject = method.invoke(this.delegate, args);
		Class<?> clazz = resultObject.getClass();
		ClassLoader cl = clazz.getClassLoader();

		boolean containsReturningClass = false;
		Class<?>[] interfaces = clazz.getInterfaces();
		for (int i = 0; i < interfaces.length; i++) {
			Class<?> interfaceClass = interfaces[i];
			if (interfaceClass.equals(returningClass)) {
				containsReturningClass = true;
				break;
			}
		}

		if (LocalXADataSource.class.isInstance(this.delegate) && XADataSource.class.equals(declaringClass)
				&& javax.sql.XAConnection.class.equals(returningClass)) {
			ManagedConnectionHandler interceptor = new ManagedConnectionHandler(resultObject);
			interceptor.setIdentifier(this.identifier);

			Object finalObject = null;
			if (containsReturningClass) {
				finalObject = Proxy.newProxyInstance(cl, interfaces, interceptor);
			} else {
				Class<?>[] interfaceArray = new Class<?>[interfaces.length + 2];
				System.arraycopy(interfaces, 0, interfaceArray, 0, interfaces.length);
				interfaceArray[interfaces.length] = javax.sql.XAConnection.class;
				interfaceArray[interfaces.length + 1] = javax.sql.DataSource.class;
				finalObject = Proxy.newProxyInstance(cl, interfaceArray, interceptor);
			}
			return (javax.sql.XAConnection) finalObject;
		} else if (XADataSource.class.equals(declaringClass) && javax.sql.XAConnection.class.equals(returningClass)) {
			ManagedConnectionHandler interceptor = new ManagedConnectionHandler(resultObject);
			interceptor.setIdentifier(this.identifier);

			Object finalObject = null;
			if (containsReturningClass) {
				finalObject = Proxy.newProxyInstance(cl, interfaces, interceptor);
			} else {
				Class<?>[] interfaceArray = new Class<?>[interfaces.length + 1];
				System.arraycopy(interfaces, 0, interfaceArray, 0, interfaces.length);
				interfaceArray[interfaces.length] = javax.sql.XAConnection.class;
				finalObject = Proxy.newProxyInstance(cl, interfaceArray, interceptor);
			}
			return (javax.sql.XAConnection) finalObject;
		} else if (XAConnectionFactory.class.equals(declaringClass)
				&& javax.jms.XAConnection.class.equals(returningClass)) {
			ManagedConnectionHandler interceptor = new ManagedConnectionHandler(resultObject);
			interceptor.setIdentifier(this.identifier);

			Object finalObject = null;
			if (containsReturningClass) {
				finalObject = Proxy.newProxyInstance(cl, interfaces, interceptor);
			} else {
				Class<?>[] interfaceArray = new Class<?>[interfaces.length + 1];
				System.arraycopy(interfaces, 0, interfaceArray, 0, interfaces.length);
				interfaceArray[interfaces.length] = javax.jms.XAConnection.class;
				finalObject = Proxy.newProxyInstance(cl, interfaceArray, interceptor);
			}
			return (javax.jms.XAConnection) finalObject;
		} else if (ManagedConnectionFactory.class.equals(declaringClass)
				&& ManagedConnection.class.equals(returningClass)) {
			ManagedConnectionHandler interceptor = new ManagedConnectionHandler(resultObject);
			interceptor.setIdentifier(this.identifier);

			Object finalObject = null;
			if (containsReturningClass) {
				finalObject = Proxy.newProxyInstance(cl, interfaces, interceptor);
			} else {
				Class<?>[] interfaceArray = new Class<?>[interfaces.length + 1];
				System.arraycopy(interfaces, 0, interfaceArray, 0, interfaces.length);
				interfaceArray[interfaces.length] = ManagedConnection.class;
				finalObject = Proxy.newProxyInstance(cl, interfaceArray, interceptor);
			}

			return (ManagedConnection) finalObject;
		} else {
			return resultObject;
		}
	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

}
