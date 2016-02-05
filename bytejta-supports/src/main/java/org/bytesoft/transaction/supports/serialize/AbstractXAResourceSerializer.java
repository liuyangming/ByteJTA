/**
 * Copyright 2014-2015 yangming.liu<liuyangming@gmail.com>.
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
package org.bytesoft.transaction.supports.serialize;

import java.io.IOException;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public abstract class AbstractXAResourceSerializer implements XAResourceSerializer, ApplicationContextAware {
	private static Pattern pattern = Pattern.compile("^[^:]+\\s*:\\s*\\d+$");
	private ApplicationContext applicationContext;

	public abstract XAResourceDescriptor deserializeTransactionResource(String identifier) throws IOException;

	public XAResourceDescriptor deserialize(String identifier) throws IOException {
		Object bean = null;
		try {
			bean = this.applicationContext.getBean(identifier);
		} catch (BeansException bex) {
			Matcher matcher = pattern.matcher(identifier);
			if (matcher.find()) {
				bean = this.deserializeTransactionResource(identifier);
			} else {
				throw new IllegalStateException(bex);
			}
		}
		if (bean != null && XADataSource.class.isInstance(bean)) {
			try {
				XADataSource xaDataSource = (XADataSource) bean;
				XAConnection xaConnection = xaDataSource.getXAConnection();
				XAResource xaResource = xaConnection.getXAResource();
				CommonResourceDescriptor descriptor = new CommonResourceDescriptor();
				descriptor.setIdentifier(identifier);
				descriptor.setDelegate(xaResource);
				return descriptor;
			} catch (SQLException ex) {
				throw new IOException(ex);
			}
		}
		// else if (bean != null && XAResourceDescriptor.class.isInstance(bean)) {
		// XAResourceDescriptor resource = (XAResourceDescriptor) bean;
		// RemoteResourceDescriptor descriptor = new RemoteResourceDescriptor();
		// descriptor.setDelegate(resource);
		// descriptor.setIdentifier(resource.getIdentifier());
		// return descriptor;
		// }
		throw new IllegalStateException();
	}

	public String serialize(XAResourceDescriptor descriptor) throws IOException {
		return descriptor.getIdentifier();
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

}
