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

import org.nustaq.serialization.FSTConfiguration;
import org.objenesis.strategy.InstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.caucho.hessian.io.HessianInput;
import com.caucho.hessian.io.HessianOutput;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

public class SerializeUtils {
	static final Logger logger = LoggerFactory.getLogger(SerializeUtils.class);

	static final int SERIALIZER_DEFAULT = 0x0;
	static final int SERIALIZER_KRYO = 0x1;
	static final int SERIALIZER_HESSIAN = 0x2;
	static final int SERIALIZER_FST = 0x3;

	static final FSTConfiguration fstConfig = FSTConfiguration.createDefaultConfiguration();

	static final KryoPool kryoPool = new KryoPool.Builder(new KryoFactory() {
		public Kryo create() {
			Kryo kryo = new Kryo();
			StdInstantiatorStrategy stdInstantiatorStrategy = new StdInstantiatorStrategy();
			InstantiatorStrategy instantiatorStrategy = new Kryo.DefaultInstantiatorStrategy(stdInstantiatorStrategy);
			kryo.setInstantiatorStrategy(instantiatorStrategy);
			return kryo;
		}
	}).softReferences().build();

	public static byte[] serializeObject(Serializable obj) throws IOException {
		int serializer = SERIALIZER_DEFAULT;
		byte[] dataArray = null;
		try {
			dataArray = kryoSerialize(obj);
			serializer = SERIALIZER_KRYO;
		} catch (Exception ex) {
			dataArray = javaSerialize(obj);
			serializer = SERIALIZER_KRYO;
		}

		byte[] byteArray = new byte[dataArray.length + 1];
		byteArray[0] = (byte) serializer;
		System.arraycopy(dataArray, 0, byteArray, 1, dataArray.length);

		return byteArray;
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
		} else if (serializer == SERIALIZER_FST) {
			System.arraycopy(bytes, 1, byteArray, 0, byteArray.length);
			return fstDeserialize(byteArray);
		} else if (serializer == SERIALIZER_DEFAULT) {
			System.arraycopy(bytes, 1, byteArray, 0, byteArray.length);
			return javaDeserialize(byteArray);
		} else {
			throw new IllegalArgumentException();
		}
	}

	public static byte[] fstSerialize(final Serializable obj) throws IOException {
		return fstConfig.asByteArray(obj);
	}

	public static Serializable fstDeserialize(byte[] byteArray) throws IOException {
		return (Serializable) fstConfig.asObject(byteArray);
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

}
