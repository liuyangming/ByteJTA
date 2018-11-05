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

import java.util.HashMap;
import java.util.Map;

import javax.sql.XADataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertyNameAliases;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.util.ClassUtils;

public class DataSourceSpiBuilder<T extends XADataSource> {
	private static final String[] XA_DATA_SOURCE_TYPE_NAMES = new String[] { "com.mysql.jdbc.jdbc2.optional.MysqlXADataSource",
			"org.postgresql.xa.PGXADataSource", "oracle.jdbc.xa.client.OracleXADataSource" };
	static final Logger logger = LoggerFactory.getLogger(DataSourceSpiBuilder.class);

	private Class<? extends XADataSource> type;
	private final Map<String, String> properties = new HashMap<String, String>();
	private ClassLoader classLoader;

	private DataSourceSpiBuilder(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	@SuppressWarnings("rawtypes")
	public static DataSourceSpiBuilder<?> create() {
		return new DataSourceSpiBuilder(null);
	}

	@SuppressWarnings("rawtypes")
	public static DataSourceSpiBuilder<?> create(ClassLoader classLoader) {
		return new DataSourceSpiBuilder(classLoader);
	}

	@SuppressWarnings("deprecation")
	public XADataSource build() {
		Class<? extends XADataSource> type = this.getType();
		XADataSource xaDataSourceInstance = BeanUtils.instantiate(type);
		this.bind(xaDataSourceInstance);
		return xaDataSourceInstance;
	}

	private void bind(XADataSource result) {
		ConfigurationPropertySource source = new MapConfigurationPropertySource(this.properties);
		ConfigurationPropertyNameAliases aliases = new ConfigurationPropertyNameAliases();
		aliases.addAliases("url", "jdbc-url");
		aliases.addAliases("username", "user");
		Binder binder = new Binder(source.withAliases(aliases));
		binder.bind(ConfigurationPropertyName.EMPTY, Bindable.ofInstance(result));
	}

	public DataSourceSpiBuilder<?> type(Class<? extends XADataSource> type) {
		this.type = type;
		return this;
	}

	public DataSourceSpiBuilder<?> url(String url) {
		this.properties.put("url", url);
		return this;
	}

	public DataSourceSpiBuilder<?> username(String username) {
		this.properties.put("username", username);
		return this;
	}

	public DataSourceSpiBuilder<?> password(String password) {
		this.properties.put("password", password);
		return this;
	}

	private Class<? extends XADataSource> getType() {
		Class<? extends XADataSource> type = (this.type != null) ? this.type : findType(this.classLoader);
		if (type != null) {
			return type;
		} else {
			throw new IllegalStateException("No supported XADataSource type found");
		}
	}

	@SuppressWarnings("unchecked")
	public static Class<? extends XADataSource> findType(ClassLoader classLoader) {
		for (String name : XA_DATA_SOURCE_TYPE_NAMES) {
			try {
				return (Class<? extends XADataSource>) ClassUtils.forName(name, classLoader);
			} catch (Exception error) {
				logger.debug("Error occurred while loading class: {}!", name, error);
			}
		}
		return null;
	}

}
