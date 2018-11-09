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
package org.bytesoft.bytejta.supports.resource.properties;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.support.EncodedResource;

public class ConnectorResourcePropertySource extends PropertySource<Object> {
	private final Map<String, String> aliases = new HashMap<String, String>();

	private boolean enabled;

	public ConnectorResourcePropertySource(String name, EncodedResource source) {
		this(name, source, new HashMap<String, String>());
	}

	public ConnectorResourcePropertySource(String name, EncodedResource source, Map<String, String> aliases) {
		super(name, source);
		this.aliases.putAll(aliases);

		EncodedResource encoded = (EncodedResource) this.getSource();
		AbstractResource resource = (AbstractResource) encoded.getResource();
		String path = resource.getFilename();

		if (StringUtils.isBlank(path)) {
			return;
		}

		String[] values = path.split(":");
		if (values.length != 2) {
			return;
		}

		String protocol = values[0];
		String resName = values[1];
		if ("bytejta".equalsIgnoreCase(protocol) == false) {
			return;
		} else if ("connector.config".equalsIgnoreCase(resName) == false) {
			return;
		}

		this.enabled = true;
	}

	public Object getProperty(String name) {
		if (this.enabled == false) {
			return null;
		} else if (StringUtils.isBlank(name)) {
			return null;
		} else if (StringUtils.startsWith(name, "spring.datasource.") == false) {
			return null;
		}

		int dotIndex = name.lastIndexOf(".");
		String prefix = name.substring(0, dotIndex);
		String suffix = name.substring(dotIndex + 1);

		String alias = this.aliases.get(suffix);
		if (StringUtils.isBlank(alias)) {
			return null;
		}

		return String.format("${%s.%s}", prefix, alias);
	}

}
