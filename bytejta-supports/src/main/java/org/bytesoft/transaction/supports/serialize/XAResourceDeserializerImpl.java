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
package org.bytesoft.transaction.supports.serialize;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;

import org.bytesoft.bytejta.supports.wire.RemoteCoordinatorRegistry;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class XAResourceDeserializerImpl implements XAResourceDeserializer, ApplicationContextAware {
	private static Pattern pattern = Pattern.compile("^[^:]+\\s*:\\s*\\d+$");
	private ApplicationContext applicationContext;

	private Map<String, XAResource> cachedResourceMap = new ConcurrentHashMap<String, XAResource>();

	public XAResource deserialize(String identifier) throws IOException {
		try {
			Object bean = this.applicationContext.getBean(identifier);
			XAResource cachedResource = this.cachedResourceMap.get(identifier);
			if (cachedResource == null) {
				cachedResource = this.deserializeResource(bean);
				if (cachedResource != null) {
					this.cachedResourceMap.put(identifier, cachedResource);
				}
			}
			return cachedResource;
		} catch (BeansException bex) {
			Matcher matcher = pattern.matcher(identifier);
			if (matcher.find()) {
				return RemoteCoordinatorRegistry.getInstance().getTransactionManagerStub(identifier);
			} else {
				throw new IOException(bex);
			}
		} catch (IOException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new IOException(ex);
		}

	}

	private XAResource deserializeResource(Object bean) throws Exception {
		if (XADataSource.class.isInstance(bean)) {
			XADataSource xaDataSource = (XADataSource) bean;
			XAConnection xaConnection = null;
			try {
				xaConnection = xaDataSource.getXAConnection();
				return xaConnection.getXAResource();
			} finally {
				// this.closeIfNecessary(xaConnection);
			}
		} else if (XAConnectionFactory.class.isInstance(bean)) {
			XAConnectionFactory connectionFactory = (XAConnectionFactory) bean;
			javax.jms.XAConnection xaConnection = null;
			XASession xaSession = null;
			try {
				xaConnection = connectionFactory.createXAConnection();
				xaSession = xaConnection.createXASession();
				return xaSession.getXAResource();
			} finally {
				// this.closeIfNecessary(xaSession);
				// this.closeIfNecessary(xaConnection);
			}
		} else if (ManagedConnectionFactory.class.isInstance(bean)) {
			ManagedConnectionFactory connectionFactory = (ManagedConnectionFactory) bean;
			ManagedConnection managedConnection = null;
			try {
				managedConnection = connectionFactory.createManagedConnection(null, null);
				return managedConnection.getXAResource();
			} finally {
				// this.closeIfNecessary(managedConnection);
			}
		} else {
			return null;
		}

	}

	protected void closeIfNecessary(XAConnection closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				return;
			}
		}
	}

	protected void closeIfNecessary(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				return;
			}
		}
	}

	protected void closeIfNecessary(ManagedConnection closeable) {
		if (closeable != null) {
			try {
				closeable.destroy();
			} catch (Exception ex) {
				return;
			}
		}
	}

	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}

}
