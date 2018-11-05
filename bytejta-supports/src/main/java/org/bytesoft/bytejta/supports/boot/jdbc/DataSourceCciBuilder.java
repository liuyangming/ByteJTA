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
package org.bytesoft.bytejta.supports.boot.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;
import javax.sql.XADataSource;

import org.apache.commons.dbcp2.managed.BasicManagedDataSource;
import org.apache.commons.lang3.StringUtils;
import org.bytesoft.bytejta.supports.resource.ManagedConnectionFactoryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertyNameAliases;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

public class DataSourceCciBuilder<T extends DataSource> {
	static final Logger logger = LoggerFactory.getLogger(DataSourceCciBuilder.class);

	private XADataSource xaDataSourceInstance;
	private final Map<String, String> properties = new HashMap<String, String>();

	private DataSourceCciBuilder(XADataSource xaDataSourceInstance) {
		if (xaDataSourceInstance == null) {
			throw new IllegalArgumentException("the xaDataSourceInstance cannot be null!");
		}
		this.xaDataSourceInstance = xaDataSourceInstance;
	}

	@SuppressWarnings("rawtypes")
	public static DataSourceCciBuilder<?> create(XADataSource xaDataSourceInstance) {
		return new DataSourceCciBuilder(xaDataSourceInstance);
	}

	@SuppressWarnings("unchecked")
	public DataSource build() {
		this.validateXADataSourceInstance();

		BasicManagedDataSource dataSource = BeanUtils.instantiateClass(BasicManagedDataSource.class);
		this.bind(dataSource);

		dataSource.setXaDataSourceInstance(this.xaDataSourceInstance);

		return (T) dataSource;
	}

	private void validateXADataSourceInstance() {
		InvocationHandler handler = Proxy.isProxyClass(this.xaDataSourceInstance.getClass())
				? Proxy.getInvocationHandler(this.xaDataSourceInstance) : null;
		ManagedConnectionFactoryHandler mcfh = //
				(handler == null || ManagedConnectionFactoryHandler.class.isInstance(handler) == false) ? null
						: (ManagedConnectionFactoryHandler) handler;
		if (BeanNameAware.class.isInstance(this.xaDataSourceInstance) == false
				&& (mcfh == null || StringUtils.isBlank(mcfh.getIdentifier()))) {
			throw new IllegalStateException("XADataSource is not properly configured!");
		}
	}

	private void bind(DataSource dataSource) {
		ConfigurationPropertySource source = new MapConfigurationPropertySource(this.properties);
		ConfigurationPropertyNameAliases aliases = new ConfigurationPropertyNameAliases();
		Binder binder = new Binder(source.withAliases(aliases));
		binder.bind(ConfigurationPropertyName.EMPTY, Bindable.ofInstance(dataSource));
	}

}
