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

import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;

import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;

public class ManagedConnectionHandler implements InvocationHandler {

	private final Object delegate;
	private String identifier;

	public ManagedConnectionHandler(Object managed) {
		this.delegate = managed;
	}

	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Class<?> declaringClass = method.getDeclaringClass();
		Class<?> returningClass = method.getReturnType();

		Object resultObject = method.invoke(this.delegate, args);
		if (resultObject != null && XAResourceDescriptor.class.isInstance(resultObject)) {
			return resultObject;
		}

		CommonResourceDescriptor descriptor = new CommonResourceDescriptor();
		descriptor.setIdentifier(this.identifier);
		if (javax.sql.XAConnection.class.equals(declaringClass) && XAResource.class.equals(returningClass)) {
			descriptor.setDelegate((XAResource) resultObject);
		} else if (javax.jms.XAConnection.class.equals(declaringClass) && XAResource.class.equals(returningClass)) {
			descriptor.setDelegate((XAResource) resultObject);
		} else if (ManagedConnection.class.equals(declaringClass) && XAResource.class.equals(returningClass)) {
			descriptor.setDelegate((XAResource) resultObject);
		}

		return descriptor.getDelegate() != null ? descriptor : resultObject;

	}

	public String getIdentifier() {
		return identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

}
