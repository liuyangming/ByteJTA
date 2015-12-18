/**
 * Copyright 2014 yangming.liu<liuyangming@gmail.com>.
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
package org.bytesoft.bytejta.utils;

import java.io.Closeable;

public class CommonUtils {
	public static void close(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	private static int checkEquals(Object o1, Object o2) {
		if (o1 == o2) {
			return 1;
		} else if (o1 == null || o2 == null) {
			return -1;
		} else {
			return 0;
		}
	}

	public static boolean equals(Object o1, Object o2) {
		int flags = checkEquals(o1, o2);
		if (flags > 0) {
			return true;
		} else if (flags < 0) {
			return false;
		} else if (o1.getClass().equals(o2.getClass()) == false) {
			return false;
		} else {
			return o1.equals(o2);
		}
	}

}
