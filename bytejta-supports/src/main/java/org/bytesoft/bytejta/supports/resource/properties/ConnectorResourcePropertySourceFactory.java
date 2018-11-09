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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.core.env.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;

public class ConnectorResourcePropertySourceFactory implements PropertySourceFactory {
	static final Map<String, String> aliases = new HashMap<String, String>();
	static {
		aliases.put("user", "username");
		aliases.put("jdbcUrl", "url");
	}

	public static Map<String, String> getAliases() {
		return aliases;
	}

	public PropertySource<?> createPropertySource(String name, EncodedResource resource) throws IOException {
		if (name == null) {
			name = String.format("%s@%s", ConnectorResourcePropertySource.class, System.identityHashCode(resource));
		} // end-if (name == null)

		return new ConnectorResourcePropertySource(name, resource, aliases);
	}

}
