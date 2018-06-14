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
package org.bytesoft.bytejta.supports.springcloud.auto.jdbc;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.managed.BasicManagedDataSource;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.boot.bind.RelaxedDataBinder;

public class XADataSourceBuilder {
	private Class<? extends DataSource> type = BasicManagedDataSource.class;
	private Map<String, String> properties = new HashMap<String, String>();

	public DataSource build() {
		Class<? extends DataSource> type = getType();
		DataSource result = BeanUtils.instantiate(type);
		this.bind(result);
		return result;
	}

	private void bind(DataSource result) {
		MutablePropertyValues properties = new MutablePropertyValues(this.properties);
		new RelaxedDataBinder(result).withAlias("url", "jdbcUrl").withAlias("username", "user").bind(properties);
	}

	public XADataSourceBuilder type(Class<? extends DataSource> type) {
		this.type = type;
		return this;
	}

	public XADataSourceBuilder url(String url) {
		this.properties.put("url", url);
		return this;
	}

	public XADataSourceBuilder username(String username) {
		this.properties.put("username", username);
		return this;
	}

	public XADataSourceBuilder password(String password) {
		this.properties.put("password", password);
		return this;
	}

	public XADataSourceBuilder dataSourceClassName(String dataSourceClassName) {
		this.properties.put("dataSourceClassName", dataSourceClassName);
		return this;
	}

	private Class<? extends DataSource> getType() {
		if (this.type != null) {
			return this.type;
		} else {
			throw new IllegalStateException("No supported DataSource type found");
		}
	}

	public static XADataSourceBuilder create() {
		return new XADataSourceBuilder();
	}

}
