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
package org.bytesoft.common.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.objenesis.strategy.SerializingInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

public class SerializeUtils {
	static final Logger logger = LoggerFactory.getLogger(SerializeUtils.class);

	static final String SERIALIZER_NAME_DEFAULT = "default";
	static final String SERIALIZER_NAME_KRYO = "kryo";
	static final String SERIALIZER_NAME_HESSIAN = "hessian";

	static final int SERIALIZER_DEFAULT = 0x0;
	static final int SERIALIZER_KRYO = 0x1;
	static final int SERIALIZER_HESSIAN = 0x2;

	static int PREFERRED_SERIALIZER = SERIALIZER_KRYO;
	static {
		String serializer = StringUtils.trimToNull(System.getProperty("bytejta.serializer.preferred"));
		if (StringUtils.isNotBlank(serializer) && StringUtils.equalsIgnoreCase(SERIALIZER_NAME_KRYO, serializer)) {
			PREFERRED_SERIALIZER = SERIALIZER_KRYO;
		} else if (StringUtils.isNotBlank(serializer) && StringUtils.equalsIgnoreCase(SERIALIZER_NAME_HESSIAN, serializer)) {
			PREFERRED_SERIALIZER = SERIALIZER_HESSIAN;
		} else if (StringUtils.isNotBlank(serializer)) {
			PREFERRED_SERIALIZER = SERIALIZER_DEFAULT;
		}
	}

	static KryoPool kryoPool = new KryoPool.Builder(new KryoFactory() {
		public Kryo create() {
			Kryo kryo = new Kryo();
			kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new SerializingInstantiatorStrategy()));
			return kryo;
		}
	}).softReferences().build();

	public static byte[] serializeObject(Serializable obj, int serializerType) throws IOException {
		int serializer = SERIALIZER_DEFAULT;
		byte[] dataArray = null;
		if (serializerType == SERIALIZER_KRYO) {
			dataArray = kryoSerialize(obj);
			serializer = SERIALIZER_KRYO;
		} else if (serializerType == SERIALIZER_HESSIAN) {
			dataArray = hessianSerialize(obj);
			serializer = SERIALIZER_HESSIAN;
		} else {
			dataArray = javaSerialize(obj);
			serializer = SERIALIZER_DEFAULT;
		}

		byte[] byteArray = new byte[dataArray.length + 1];
		byteArray[0] = (byte) serializer;
		System.arraycopy(dataArray, 0, byteArray, 1, dataArray.length);

		return byteArray;
	}

	public static byte[] serializeObject(Serializable obj) throws IOException {
		if (PREFERRED_SERIALIZER == SERIALIZER_DEFAULT) {
			return serializeObject(obj, PREFERRED_SERIALIZER);
		} else {
			try {
				return serializeObject(obj, PREFERRED_SERIALIZER);
			} catch (Exception ex) {
				return serializeObject(obj, SERIALIZER_DEFAULT);
			}
		}
	}

	public static Serializable deserializeObject(byte[] bytes) throws IOException {
		if (bytes.length == 0) {
			throw new IllegalArgumentException();
		}

		byte[] byteArray = new byte[bytes.length - 1];
		int serializer = bytes[0];

		if (serializer == SERIALIZER_KRYO) {
			System.arraycopy(bytes, 1, byteArray, 0, byteArray.length);
			return kryoDeserialize(byteArray);
		} else if (serializer == SERIALIZER_HESSIAN) {
			System.arraycopy(bytes, 1, byteArray, 0, byteArray.length);
			return hessianDeserialize(byteArray);
		} else if (serializer == SERIALIZER_DEFAULT) {
			System.arraycopy(bytes, 1, byteArray, 0, byteArray.length);
			return javaDeserialize(byteArray);
		} else {
			throw new IllegalArgumentException();
		}
	}

	public static byte[] javaSerialize(final Serializable obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(baos));
		try {
			oos.writeObject(obj);
		} finally {
			CommonUtils.closeQuietly(oos);
		}
		return baos.toByteArray();
	}

	public static Serializable javaDeserialize(byte[] byteArray) throws IOException {
		ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new ByteArrayInputStream(byteArray)));
		try {
			return (Serializable) ois.readObject();
		} catch (ClassNotFoundException ex) {
			throw new IllegalStateException(ex);
		} finally {
			CommonUtils.closeQuietly(ois);
		}
	}

	public static byte[] kryoSerialize(final Serializable obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final Output output = new Output(baos);

		try {
			kryoPool.run(new KryoCallback<Object>() {
				public Object execute(Kryo kryo) {
					kryo.writeClassAndObject(output, obj);
					return null;
				}
			});
		} finally {
			CommonUtils.closeQuietly(output);
		}

		return baos.toByteArray();
	}

	public static Serializable kryoDeserialize(byte[] byteArray) throws IOException {
		final Input input = new Input(new ByteArrayInputStream(byteArray));

		try {
			return kryoPool.run(new KryoCallback<Serializable>() {
				public Serializable execute(Kryo kryo) {
					return (Serializable) kryo.readClassAndObject(input);
				}
			});
		} finally {
			CommonUtils.closeQuietly(input);
		}
	}

	public static byte[] hessianSerialize(Serializable obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		HessianOutput ho = new HessianOutput(baos);
		try {
			ho.writeObject(obj);
			return baos.toByteArray();
		} finally {
			CommonUtils.closeQuietly(baos);
		}

	}

	public static Serializable hessianDeserialize(byte[] bytes) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		HessianInput hi = new HessianInput(bais);
		try {
			Object result = hi.readObject();
			return (Serializable) result;
		} finally {
			CommonUtils.closeQuietly(bais);
		}
	}

	public static String serializeClass(Class<?> clazz) {
		if (boolean.class.equals(clazz)) {
			return "Z";
		} else if (byte.class.equals(clazz)) {
			return "B";
		} else if (short.class.equals(clazz)) {
			return "S";
		} else if (char.class.equals(clazz)) {
			return "C";
		} else if (int.class.equals(clazz)) {
			return "I";
		} else if (float.class.equals(clazz)) {
			return "F";
		} else if (long.class.equals(clazz)) {
			return "J";
		} else if (double.class.equals(clazz)) {
			return "D";
		} else if (void.class.equals(clazz)) {
			return "V";
		} else if (clazz.isArray()) {
			return clazz.getName();
		} else {
			return String.format("L%s;", clazz.getName().replaceAll("\\.", "/"));
		}
	}

	public static Class<?> deserializeClass(String classDesc) {
		String clazz = StringUtils.trimToEmpty(classDesc);
		if (StringUtils.isBlank(clazz)) {
			throw new IllegalStateException();
		}

		if (clazz.length() > 1) {
			String clazzName = clazz.replaceAll("\\/", ".");
			ClassLoader cl = Thread.currentThread().getContextClassLoader();
			try {
				return cl.loadClass(clazzName);
			} catch (ClassNotFoundException ex) {
				throw new IllegalStateException(ex.getMessage());
			}
		}

		final char character = clazz.charAt(0);
		return SerializeUtils.deserializeClass(character);
	}

	public static Class<?> deserializeClass(final char character) {
		switch (character) {
		case 'Z':
			return boolean.class;
		case 'B':
			return byte.class;
		case 'S':
			return short.class;
		case 'C':
			return char.class;
		case 'I':
			return int.class;
		case 'J':
			return long.class;
		case 'F':
			return float.class;
		case 'D':
			return double.class;
		default:
			throw new IllegalStateException();
		}
	}

	public static String serializeMethod(Method method) {
		StringBuilder ber = new StringBuilder();
		ber.append(method.getName()).append("(");
		Class<?>[] parameterTypes = method.getParameterTypes();
		for (int i = 0; i < parameterTypes.length; i++) {
			Class<?> parameterType = parameterTypes[i];
			String clazzName = SerializeUtils.serializeClass(parameterType);
			ber.append(clazzName);
		}

		ber.append(")").append(SerializeUtils.serializeClass(method.getReturnType()));

		return ber.toString();
	}

	public static Method deserializeMethod(Class<?> interfaceClass, String methodDesc) throws Exception {
		int startIdx = methodDesc.indexOf("(");
		String methodName = methodDesc.substring(0, startIdx);
		int endIndex = methodDesc.indexOf(")");
		String value = methodDesc.substring(startIdx + 1, endIndex);
		char[] values = value.toCharArray();

		List<Class<?>> paramTypeList = new ArrayList<Class<?>>();
		boolean flags = false;
		StringBuilder clazzDesc = new StringBuilder();
		for (int i = 0; i < values.length; i++) {
			char character = values[i];
			if (character == ';') {
				flags = false;
				String paramTypeNameDesc = clazzDesc.toString();
				clazzDesc.delete(0, clazzDesc.length());
				Class<?> paramType = SerializeUtils.deserializeClass(paramTypeNameDesc);
				paramTypeList.add(paramType);
				continue;
			} else if (flags) {
				clazzDesc.append(character);
				continue;
			} else if (character == 'L') {
				flags = true;
				continue;
			}

			Class<?> paramType = SerializeUtils.deserializeClass(character);
			paramTypeList.add(paramType);
		}

		Class<?>[] parameterTypes = new Class<?>[paramTypeList.size()];
		paramTypeList.toArray(parameterTypes);

		return interfaceClass.getDeclaredMethod(methodName, parameterTypes);
	}

}
