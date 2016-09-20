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
package org.bytesoft.bytejta.logging.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;

import org.bytesoft.transaction.logging.store.VirtualLoggingTrigger;
import org.bytesoft.transaction.xa.XidFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualLoggingFile {
	static final Logger logger = LoggerFactory.getLogger(VirtualLoggingFile.class);

	static final long DEFAULT_SIZE = 1024 * 1024;
	static final long INCREASE_SIZE = 1024 * 512;

	private MappedByteBuffer readable;
	private MappedByteBuffer writable;

	private RandomAccessFile raf;

	private byte[] identifier;

	private boolean initialized;

	private int startIdx;
	private int endIndex;

	private boolean marked;
	private boolean master;

	private VirtualLoggingTrigger trigger;

	public VirtualLoggingFile(File file) throws IOException {
		this.initialized = file.exists();
		this.raf = new RandomAccessFile(file, "rw");
		if (this.initialized == false) {
			this.configMappedByteBuffer(DEFAULT_SIZE);
		} else {
			this.configMappedByteBuffer(this.raf.length());
		}
	}

	public void clearMarkedFlag() {
		int pos = this.writable.position();

		this.writable.position(identifier.length + 2 + 8 + 4);
		this.writable.put((byte) 0x0);

		this.marked = false;

		this.writable.position(pos);
	}

	public void fixSwitchError() {
		int pos = this.writable.position();

		this.writable.position(identifier.length + 2 + 8 + 4 + 1);
		this.writable.put((byte) 0x1);
		this.writable.position(identifier.length + 2 + 8 + 4);
		this.writable.put((byte) 0x0);

		this.marked = false;
		this.master = true;

		this.writable.position(pos);
	}

	public void initialize(boolean master) {
		this.checkLoggingIdentifier();
		this.checkLoggingVersion();
		this.checkCreatedTime();
		this.checkStartIndex();
		this.checkMasterFlag(master);
		this.checkModifiedTime();
		this.checkEndIndex();

		this.initialized = false;
	}

	private void checkLoggingIdentifier() {
		byte[] array = new byte[identifier.length];
		this.writable.position(0);
		this.writable.get(array);
		if (Arrays.equals(identifier, array)) {
			// ignore
		} else if (this.initialized == false) {
			writable.position(0);
			writable.put(identifier);
		} else {
			throw new IllegalStateException();
		}
	}

	private void checkLoggingVersion() {
		this.writable.position(identifier.length);
		int major = this.writable.get();
		int minor = this.writable.get();
		if (major == 0 && minor == 1) {
			// ignore
		} else if (this.initialized == false) {
			writable.position(identifier.length);
			writable.put((byte) 0x0);
			writable.put((byte) 0x1);
		} else {
			throw new IllegalStateException();
		}
	}

	private void checkCreatedTime() {
		if (this.initialized == false) {
			writable.position(identifier.length + 2);
			writable.putLong(System.currentTimeMillis());
		}
	}

	private void checkStartIndex() {
		this.writable.position(identifier.length + 2 + 8);
		int start = this.writable.getInt();
		if (start == identifier.length + 2 + 8 + 4 + 2 + 8 + 4) {
			this.startIdx = start;
		} else if (this.initialized == false) {
			this.startIdx = identifier.length + 2 + 8 + 4 + 2 + 8 + 4;
			writable.position(identifier.length + 2 + 8);
			writable.putInt(identifier.length + 2 + 8 + 4 + 2 + 8 + 4);
		} else {
			throw new IllegalStateException();
		}
	}

	private void checkMasterFlag(boolean master) {
		if (this.initialized == false) {
			this.master = master;
			this.marked = false;
			writable.position(identifier.length + 2 + 8 + 4);
			writable.put((byte) 0x0);
			writable.put(master ? (byte) 0x1 : (byte) 0x0);
		} else {
			this.writable.position(identifier.length + 2 + 8 + 4);
			this.marked = this.writable.get() == 0x1;
			this.master = this.writable.get() == 0x1;
		}
	}

	private void checkModifiedTime() {
		if (this.initialized == false) {
			writable.position(identifier.length + 2 + 8 + 4 + 2);
			writable.putLong(System.currentTimeMillis());
		}
	}

	private void checkEndIndex() {
		if (this.initialized == false) {
			this.endIndex = identifier.length + 2 + 8 + 4 + 2 + 8 + 4;
			writable.position(identifier.length + 2 + 8 + 4 + 2 + 8);
			writable.putInt(identifier.length + 2 + 8 + 4 + 2 + 8 + 4);
		} else {
			this.writable.position(identifier.length + 2 + 8 + 4 + 2 + 8);
			this.endIndex = this.writable.getInt();
		}
	}

	public void markAsMaster() {
		this.writable.position(identifier.length + 2 + 8 + 4);
		this.writable.put((byte) 0x1);
	}

	public void switchToMaster() {
		this.writable.position(identifier.length + 2 + 8 + 4 + 1);
		this.writable.put((byte) 0x1);
		this.writable.position(identifier.length + 2 + 8 + 4);
		this.writable.put((byte) 0x0);

		this.master = true;
		this.marked = false;

		this.readable.position(this.startIdx);
	}

	public void switchToSlaver() {
		this.writable.position(identifier.length + 2 + 8 + 4);
		this.writable.put((byte) 0x0);
		this.writable.put((byte) 0x0);

		this.master = false;
		this.marked = false;

		writable.position(identifier.length + 2 + 8 + 4 + 2);
		this.writable.putLong(System.currentTimeMillis());
		this.endIndex = this.startIdx;
		this.writable.putInt(this.endIndex);
	}

	public void prepareForReading() {
		this.readable.position(this.startIdx);
	}

	public byte[] read() {
		if (this.readable.position() < this.endIndex) {
			int pos = this.readable.position();
			this.readable.position(pos + XidFactory.GLOBAL_TRANSACTION_LENGTH + 1);
			int size = this.readable.getInt();
			byte[] byteArray = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH + 1 + 4 + size];
			this.readable.position(pos);
			this.readable.get(byteArray);
			return byteArray;
		} else {
			return new byte[0];
		}
	}

	public void write(byte[] byteArray) {
		if (this.writable.capacity() < this.endIndex + byteArray.length) {
			this.resizeMappedByteBuffer(this.endIndex + INCREASE_SIZE);
		}
		this.writable.position(this.endIndex);
		this.writable.put(byteArray);

		writable.position(identifier.length + 2 + 8 + 4 + 2);
		this.writable.putLong(System.currentTimeMillis());
		this.endIndex = this.endIndex + byteArray.length;
		this.writable.putInt(this.endIndex);

		int threshold = (this.writable.capacity() * 2) / 3;
		if (this.endIndex > threshold) {
			this.trigger.fireSwapImmediately();
		}

	}

	private void resizeMappedByteBuffer(long size) {
		try {
			this.raf.setLength(this.endIndex + INCREASE_SIZE);
			this.readable = this.raf.getChannel().map(MapMode.READ_ONLY, 0, size);
			this.writable = this.raf.getChannel().map(MapMode.READ_WRITE, 0, size);
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private void configMappedByteBuffer(long size) throws IOException {
		this.readable = this.raf.getChannel().map(MapMode.READ_ONLY, 0, size);
		this.writable = this.raf.getChannel().map(MapMode.READ_WRITE, 0, size);
	}

	public void flushImmediately() {
		if (this.writable != null) {
			this.writable.force();
		}
	}

	public void closeQuietly() {
		if (this.raf != null) {
			try {
				this.raf.close();
			} catch (Exception ex) {
				logger.debug(ex.getMessage(), ex);
			}
		}
	}

	public byte[] getIdentifier() {
		return identifier;
	}

	public void setIdentifier(byte[] identifier) {
		this.identifier = identifier;
	}

	public VirtualLoggingTrigger getTrigger() {
		return trigger;
	}

	public void setTrigger(VirtualLoggingTrigger trigger) {
		this.trigger = trigger;
	}

	public boolean isMarked() {
		return marked;
	}

	public void setMarked(boolean marked) {
		this.marked = marked;
	}

	public boolean isMaster() {
		return master;
	}

	public void setMaster(boolean master) {
		this.master = master;
	}

}
