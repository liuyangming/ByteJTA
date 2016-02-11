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
package org.bytesoft.common.utils;

public class ByteUtils {
	public static short byteArrayToShort(final byte[] buf) {
		return byteArrayToShort(buf, 0);
	}

	public static int byteArrayToInt(final byte[] buf) {
		return byteArrayToInt(buf, 0);
	}

	public static long byteArrayToLong(final byte[] buf) {
		return byteArrayToLong(buf, 0);
	}

	public static long byteArrayToLong(final byte[] buf, final int start) {
		if (start < 0 || start + 7 >= buf.length) {
			throw new IllegalArgumentException();
		}

		long value = ((long) buf[start] & 0xff) << 56;
		value |= ((long) buf[start + 1] & 0xff) << 48;
		value |= ((long) buf[start + 2] & 0xff) << 40;
		value |= ((long) buf[start + 3] & 0xff) << 32;
		value |= ((long) buf[start + 4] & 0xff) << 24;
		value |= ((long) buf[start + 5] & 0xff) << 16;
		value |= ((long) buf[start + 6] & 0xff) << 8;
		value |= ((long) buf[start + 7] & 0xff);
		return value;
	}

	public static byte[] longToByteArray(final long value) {
		final byte[] result = new byte[8];
		result[7] = (byte) (value & 0xff);
		result[6] = (byte) (value >> 8 & 0xff);
		result[5] = (byte) (value >> 16 & 0xff);
		result[4] = (byte) (value >> 24 & 0xff);
		result[3] = (byte) (value >> 32 & 0xff);
		result[2] = (byte) (value >> 40 & 0xff);
		result[1] = (byte) (value >> 48 & 0xff);
		result[0] = (byte) (value >> 56 & 0xff);
		return result;
	}

	public static int byteArrayToInt(final byte[] buf, final int start) {
		if (start < 0 || start + 3 >= buf.length) {
			throw new IllegalArgumentException();
		}
		int value = (buf[start] & 0xff) << 24;
		value |= (buf[start + 1] & 0xff) << 16;
		value |= (buf[start + 2] & 0xff) << 8;
		value |= (buf[start + 3] & 0xff);
		return value;
	}

	public static byte[] intToByteArray(final int value) {
		final byte[] result = new byte[4];
		result[3] = (byte) (value & 0xff);
		result[2] = (byte) (value >> 8 & 0xff);
		result[1] = (byte) (value >> 16 & 0xff);
		result[0] = (byte) (value >> 24 & 0xff);
		return result;
	}

	public static short byteArrayToShort(final byte[] buf, final int start) {
		if (start < 0 || start + 1 >= buf.length) {
			throw new IllegalArgumentException();
		}
		int value = (buf[start] & 0xff) << 8;
		value = value | (buf[start + 1] & 0xff);
		return (short) value;
	}

	public static byte[] shortToByteArray(final short value) {
		final byte[] result = new byte[2];
		result[1] = (byte) (value & 0xff);
		result[0] = (byte) (value >> 8 & 0xff);
		return result;
	}

	public static String byteArrayToString(final byte[] bytes, final int startIndex, final int len) {
		StringBuilder ber = new StringBuilder();
		for (int i = startIndex, j = 0; j < len; i++, j++) {
			byte b = bytes[i];
			ber.append(CHARS[(b & 0xf0) >> 4]);
			ber.append(CHARS[(b & 0x0f)]);
		}
		return ber.toString();
	}

	public static String byteArrayToString(final byte[] bytes) {
		return byteArrayToString(bytes, 0, bytes.length);
	}

	public static byte[] stringToByteArray(String str) {
		if (str == null) {
			return new byte[0];
		} else if (str.length() % 2 == 1) {
			throw new IllegalArgumentException();
		}
		char[] array = str.toCharArray();
		byte[] bytes = new byte[array.length / 2];

		for (int i = 0; i < array.length; i = i + 2) {
			int index1 = indexOf(array[i]);
			int index2 = indexOf(array[i + 1]);
			int tempval = index1 << 4;
			int byteval = tempval | index2;
			bytes[i / 2] = (byte) byteval;
		}
		return bytes;
	}

	private static int indexOf(char chr) {
		if (chr >= DIGIT_START && chr <= DIGIT_END) {
			return chr - DIGIT_START;
		} else if (chr >= LETTER_START && chr <= LETTER_END) {
			return chr - LETTER_START + 10;
		} else if (chr >= UPPER_LETTER_START && chr <= UPPER_LETTER_END) {
			return chr - UPPER_LETTER_START + 10;
		} else {
			throw new IllegalArgumentException();
		}
	}

	static final char DIGIT_START = '0';
	static final char DIGIT_END = '9';
	static final char LETTER_START = 'a';
	static final char LETTER_END = 'f';
	static final char UPPER_LETTER_START = 'A';
	static final char UPPER_LETTER_END = 'F';
	static final char[] CHARS = new char[] {
			//
			'0', '1', '2', '3', '4', '5', '6', '7',//
			'8', '9', 'a', 'b', 'c', 'd', 'e', 'f' //
	};
}
