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
package org.bytesoft.bytejta.logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.resource.spi.work.Work;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.log4j.Logger;
import org.bytesoft.bytejta.supports.resource.CommonResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.RemoteResourceDescriptor;
import org.bytesoft.bytejta.supports.resource.UnidentifiedResourceDescriptor;
import org.bytesoft.bytejta.supports.wire.RemoteCoordinator;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.TransactionBeanFactory;
import org.bytesoft.transaction.archive.TransactionArchive;
import org.bytesoft.transaction.archive.XAResourceArchive;
import org.bytesoft.transaction.aware.TransactionBeanFactoryAware;
import org.bytesoft.transaction.logger.TransactionLogger;
import org.bytesoft.transaction.supports.resource.XAResourceDescriptor;
import org.bytesoft.transaction.supports.serialize.XAResourceDeserializer;
import org.bytesoft.transaction.xa.TransactionXid;
import org.bytesoft.transaction.xa.XidFactory;

public class SampleTransactionLogger implements TransactionLogger, Work, TransactionBeanFactoryAware {
	static final Logger logger = Logger.getLogger(SampleTransactionLogger.class.getSimpleName());

	static final byte OPERTOR_ADD_TRANSACTION = 0x1;
	static final byte OPERTOR_MOD_TRANSACTION = 0x2;
	static final byte OPERTOR_DEL_TRANSACTION = 0x3;
	static final byte OPERTOR_MOD_RESOURCE = 0x4;

	static final byte[] IDENTIFIER = "org.bytesoft.bytejta.logger.sample".getBytes();
	static final int VERSION_MAJOR = 0;
	static final int VERSION_MINOR = 1;

	static final int SIZE_HEADER = IDENTIFIER.length + 2 + 4 + 8 * 2;
	static final int SIZE_HEADER_SECTION = XidFactory.GLOBAL_TRANSACTION_LENGTH + 6;

	private long minIndex;
	private long maxIndex;

	private final AtomicInteger concurrent = new AtomicInteger();
	private final Lock lock = new ReentrantLock();
	private final Map<Xid, TransactionHolder> archives = new HashMap<Xid, TransactionHolder>();

	private File storage;
	private RandomAccessFile raf;
	private FileChannel channel;
	private boolean released = false;

	private TransactionBeanFactory beanFactory;
	private XAResourceDeserializer deserializer;

	public void createTransaction(TransactionArchive archive) {
		try {
			this.concurrent.incrementAndGet();
			this.lock.lock();

			SerializedObject serializedObj = this.serialize(archive);
			byte[] key = serializedObj.key;
			byte[] value = serializedObj.value;

			int sizeOfBlock = SIZE_HEADER_SECTION + value.length;
			ByteBuffer byteBuf = ByteBuffer.allocate(sizeOfBlock);
			byteBuf.put((byte) 0x1);
			byteBuf.put(key);
			byteBuf.put(OPERTOR_ADD_TRANSACTION);
			byteBuf.putShort((short) sizeOfBlock);
			byteBuf.putShort((short) value.length);
			byteBuf.put(value);
			byteBuf.flip();

			this.channel.position(this.maxIndex);
			this.channel.write(byteBuf);

			XidFactory xidFactory = this.beanFactory.getXidFactory();
			TransactionXid xid = xidFactory.createGlobalXid(key);
			TransactionHolder holder = new TransactionHolder();
			holder.archive = archive;
			this.archives.put(xid, holder);

			long newMaxIndex = this.maxIndex + sizeOfBlock;
			this.updateMaxIndex(newMaxIndex);
			this.maxIndex = newMaxIndex;

		} catch (IOException ex) {
			logger.error("Error occurred while creating add-transaction log.", ex);
		} catch (RuntimeException ex) {
			logger.error("Error occurred while creating add-transaction log.", ex);
		} finally {
			this.lock.unlock();
			this.concurrent.decrementAndGet();
		}
	}

	public void updateTransaction(TransactionArchive archive) {
		try {
			this.concurrent.incrementAndGet();
			this.lock.lock();

			SerializedObject serializedObj = this.serialize(archive);
			byte[] key = serializedObj.key;
			byte[] value = serializedObj.value;

			XidFactory xidFactory = this.beanFactory.getXidFactory();
			TransactionXid xid = xidFactory.createGlobalXid(key);
			TransactionHolder holder = this.archives.get(xid);
			if (holder == null) {
				logger.error("Error occurred while creating mod-transaction log: transaction not exists!");
				return;
			}

			int sizeOfBlock = SIZE_HEADER_SECTION + value.length;
			ByteBuffer byteBuf = ByteBuffer.allocate(sizeOfBlock);
			byteBuf.put((byte) 0x1);
			byteBuf.put(key);
			byteBuf.put(OPERTOR_MOD_TRANSACTION);
			byteBuf.putShort((short) sizeOfBlock);
			byteBuf.putShort((short) value.length);
			byteBuf.put(value);
			byteBuf.flip();

			this.channel.position(this.maxIndex);
			this.channel.write(byteBuf);

			// TransactionArchive that = holder.archive;
			// that.setVote(archive.getVote());
			// that.setStatus(archive.getStatus());
			holder.archive = archive;

			long newMaxIndex = this.maxIndex + sizeOfBlock;
			this.updateMaxIndex(newMaxIndex);
			this.maxIndex = newMaxIndex;

		} catch (IOException ex) {
			logger.error("Error occurred while creating mod-transaction log.", ex);
		} catch (RuntimeException ex) {
			logger.error("Error occurred while creating mod-transaction log.", ex);
		} finally {
			this.lock.unlock();
			this.concurrent.decrementAndGet();
		}
	}

	public void deleteTransaction(TransactionArchive archive) {
		try {
			this.concurrent.incrementAndGet();
			this.lock.lock();

			SerializedObject serializedObj = this.serialize(archive);
			byte[] key = serializedObj.key;
			byte[] value = serializedObj.value;

			XidFactory xidFactory = this.beanFactory.getXidFactory();
			TransactionXid xid = xidFactory.createGlobalXid(key);
			TransactionHolder holder = this.archives.get(xid);
			if (holder == null) {
				return;
			}

			int sizeOfBlock = SIZE_HEADER_SECTION + value.length;
			ByteBuffer byteBuf = ByteBuffer.allocate(sizeOfBlock);
			byteBuf.put((byte) 0x1);
			byteBuf.put(key);
			byteBuf.put(OPERTOR_DEL_TRANSACTION);
			byteBuf.putShort((short) sizeOfBlock);
			byteBuf.putShort((short) value.length);
			byteBuf.put(value);
			byteBuf.flip();

			this.channel.position(this.maxIndex);
			this.channel.write(byteBuf);

			// this.archives.remove(xid); // don't remove immediately.
			holder.deleted = true;

			long newMaxIndex = this.maxIndex + sizeOfBlock;
			this.updateMaxIndex(newMaxIndex);
			this.maxIndex = newMaxIndex;

		} catch (IOException ex) {
			logger.error("Error occurred while creating del-transaction log.", ex);
		} catch (RuntimeException ex) {
			logger.error("Error occurred while creating del-transaction log.", ex);
		} finally {
			this.lock.unlock();
			this.concurrent.decrementAndGet();
		}
	}

	public List<TransactionArchive> getTransactionArchiveList() {
		List<TransactionArchive> results = new ArrayList<TransactionArchive>();
		try {
			this.lock.lock();
			Iterator<Map.Entry<Xid, TransactionHolder>> itr = this.archives.entrySet().iterator();
			while (itr.hasNext()) {
				Map.Entry<Xid, TransactionHolder> entry = itr.next();
				TransactionHolder holder = entry.getValue();
				if (holder.deleted) {
					continue;
				}
				results.add(holder.archive);
			}
		} finally {
			this.lock.unlock();
		}
		return results;
	}

	public void updateResource(XAResourceArchive resourceArchive) {
		try {
			this.concurrent.incrementAndGet();
			this.lock.lock();

			SerializedObject serializedObj = this.serializeResource(resourceArchive);
			byte[] key = serializedObj.key;
			byte[] value = serializedObj.value;

			int sizeOfBlock = SIZE_HEADER_SECTION + value.length;
			ByteBuffer byteBuf = ByteBuffer.allocate(sizeOfBlock);
			byteBuf.put((byte) 0x1);
			byteBuf.put(key);
			byteBuf.put(OPERTOR_MOD_RESOURCE);
			byteBuf.putShort((short) sizeOfBlock);
			byteBuf.putShort((short) value.length);
			byteBuf.put(value);
			byteBuf.flip();

			this.channel.position(this.maxIndex);
			this.channel.write(byteBuf);

			XidFactory xidFactory = this.beanFactory.getXidFactory();
			TransactionXid xid = xidFactory.createGlobalXid(key);
			TransactionHolder holder = this.archives.get(xid);
			if (holder == null) {
				logger.error("Error occurred while creating mod-resource log: transaction not exists!");
				return;
			}
			TransactionArchive archive = holder.archive;
			this.updateResourceArchive(archive, resourceArchive);

			long newMaxIndex = this.maxIndex + sizeOfBlock;
			this.updateMaxIndex(newMaxIndex);
			this.maxIndex = newMaxIndex;

		} catch (IOException ex) {
			logger.error("Error occurred while creating del-transaction log.", ex);
		} catch (RuntimeException ex) {
			logger.error("Error occurred while creating del-transaction log.", ex);
		} finally {
			this.lock.unlock();
			this.concurrent.decrementAndGet();
		}
	}

	public synchronized void initialize() throws IOException {
		if (this.storage == null) {
			throw new IllegalStateException("The storage file should be specified!");
		} else if (this.storage.exists() == false) {
			this.mkdirDirectoryIfNecessary();
		}
		this.raf = new RandomAccessFile(this.storage, "rw");
		this.channel = this.raf.getChannel();

		try {
			this.lock.lock();

			this.validation();
			this.scanTransactionLog();
		} catch (IllegalStateException ex) {
			ByteBuffer buffer = ByteBuffer.allocate(SIZE_HEADER);
			buffer.put(IDENTIFIER);
			buffer.put((byte) VERSION_MAJOR);
			buffer.put((byte) VERSION_MINOR);

			this.minIndex = SIZE_HEADER;
			this.maxIndex = SIZE_HEADER;
			buffer.putLong(this.minIndex);
			buffer.putLong(this.maxIndex);

			this.channel.position(0);
			buffer.flip();
			this.channel.write(buffer);

			this.forceChannel();
		} finally {
			this.lock.unlock();
		}

	}

	private void scanTransactionLog() throws IOException {
		this.channel.position(SIZE_HEADER);
		while (this.channel.position() < this.maxIndex) {
			long beginIndex = this.channel.position();
			ByteBuffer headBuf = ByteBuffer.allocate(SIZE_HEADER_SECTION);
			this.channel.read(headBuf);
			byte enabled = headBuf.get();
			byte[] globalTransactionId = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
			headBuf.get(globalTransactionId);
			byte operator = headBuf.get();
			short sizeOfBlock = headBuf.getShort();
			short sizeOfText = headBuf.getShort();
			if (enabled == 0x0) {
				this.channel.position(beginIndex + sizeOfBlock);
				continue;
			}
			ByteBuffer textBuf = ByteBuffer.allocate(sizeOfText);
			this.channel.read(textBuf);
			textBuf.flip();
			byte[] textByteArray = new byte[sizeOfText];
			textBuf.get(textByteArray);

			SerializedObject serializedObj = new SerializedObject();
			serializedObj.key = globalTransactionId;
			serializedObj.value = textByteArray;

			XidFactory xidFactory = this.beanFactory.getXidFactory();
			TransactionXid xid = xidFactory.createGlobalXid(globalTransactionId);

			if (operator == OPERTOR_ADD_TRANSACTION) {
				TransactionArchive archive = this.deserialize(serializedObj);
				TransactionHolder holder = this.archives.get(xid);
				if (holder == null) {
					holder = new TransactionHolder();
				}
				holder.archive = archive;
				this.archives.put(xid, holder);
			} else if (operator == OPERTOR_MOD_TRANSACTION) {
				TransactionArchive archive = this.deserialize(serializedObj);
				TransactionHolder holder = this.archives.get(xid);
				if (holder == null) {
					holder = new TransactionHolder();
				}
				holder.archive = archive;
				this.archives.put(xid, holder);
			} else if (operator == OPERTOR_DEL_TRANSACTION) {
				TransactionHolder holder = this.archives.get(xid);
				if (holder != null) {
					holder.deleted = true;
				}
			} else if (operator == OPERTOR_MOD_RESOURCE) {
				XAResourceArchive resourceArchive = this.deserializeResource(serializedObj);
				TransactionHolder holder = this.archives.get(xid);
				if (holder != null) {
					this.updateResourceArchive(holder.archive, resourceArchive);
				}
			} else {
				logger.error("invalid info.");
				continue;
			}

		}
	}

	private void updateResourceArchive(TransactionArchive archive, XAResourceArchive thatResourceArchive) {
		List<XAResourceArchive> nativeResourceList = archive.getNativeResources();
		List<XAResourceArchive> remoteResourceList = archive.getRemoteResources();

		XAResourceArchive resourceArchive = null;
		for (int i = 0; nativeResourceList != null && i < nativeResourceList.size(); i++) {
			XAResourceArchive thisResourceArchive = nativeResourceList.get(i);
			Xid thisXid = thisResourceArchive.getXid();
			Xid thatXid = thatResourceArchive.getXid();
			if (CommonUtils.equals(thisXid, thatXid)) {
				resourceArchive = thisResourceArchive;
				break;
			}
		}
		if (resourceArchive == null) {
			for (int i = 0; remoteResourceList != null && i < remoteResourceList.size(); i++) {
				XAResourceArchive thisResourceArchive = remoteResourceList.get(i);
				Xid thisXid = thisResourceArchive.getXid();
				Xid thatXid = thatResourceArchive.getXid();
				if (CommonUtils.equals(thisXid, thatXid)) {
					resourceArchive = thisResourceArchive;
					break;
				}
			}
		}

		if (resourceArchive != null) {
			resourceArchive.setCommitted(thatResourceArchive.isCommitted());
			resourceArchive.setCompleted(thatResourceArchive.isCompleted());
			resourceArchive.setDelisted(thatResourceArchive.isDelisted());
			resourceArchive.setHeuristic(thatResourceArchive.isHeuristic());
			// resourceArchive.setIdentified(thatResourceArchive.isIdentified());
			resourceArchive.setReadonly(thatResourceArchive.isReadonly());
			resourceArchive.setRolledback(thatResourceArchive.isRolledback());
			resourceArchive.setVote(thatResourceArchive.getVote());
		}

	}

	public synchronized void complete() throws IOException {
		this.forceChannel();
		this.closeRaf();
	}

	private void validation() throws IllegalStateException, IOException {
		if (this.raf.length() < SIZE_HEADER) {
			throw new IllegalStateException();
		}

		ByteBuffer buffer = ByteBuffer.allocate(SIZE_HEADER);
		channel.read(buffer);
		buffer.flip();

		byte[] bytes = new byte[IDENTIFIER.length];
		buffer.get(bytes);
		if (Arrays.equals(bytes, IDENTIFIER) == false) {
			throw new IllegalStateException();
		}

		int major = buffer.get();
		int minor = buffer.get();
		if (major != VERSION_MAJOR || minor != VERSION_MINOR) {
			throw new IllegalStateException();
		}

		this.minIndex = buffer.getLong();
		this.maxIndex = buffer.getLong();

		if (this.minIndex != SIZE_HEADER) {
			throw new IllegalStateException();
		} else if (this.maxIndex > this.raf.length()) {
			throw new IllegalStateException();
		} else if (this.minIndex > this.maxIndex) {
			throw new IllegalStateException();
		}
	}

	public void run() {
		while (this.released == false) {
			this.execute();
			this.sleepMillis(1000L * 1);
		}
	}

	private void execute() {

		CleanupObject resultObj = new CleanupObject();
		resultObj.cleanPos = this.minIndex;
		resultObj.startPos = this.minIndex;
		resultObj.closed = true;
		do {
			CleanupObject paramsObj = new CleanupObject();
			paramsObj.cleanPos = resultObj.cleanPos;
			paramsObj.startPos = resultObj.startPos;

			resultObj = this.cleanup(paramsObj);
		} while (resultObj.closed == false);

	}

	private CleanupObject cleanup(CleanupObject paramsObj) {
		CleanupObject resultObj = new CleanupObject();
		resultObj.cleanPos = paramsObj.cleanPos;
		resultObj.startPos = paramsObj.startPos;
		try {
			this.lock.lock();
			boolean writed = false;
			do {
				CleanupObject cleanupObj = this.unitCleanup(paramsObj);
				paramsObj.cleanPos = cleanupObj.cleanPos;
				paramsObj.startPos = cleanupObj.startPos;
				if (cleanupObj.error) {
					resultObj.error = true;
					break;
				}
				writed = cleanupObj.writed ? true : writed;
				resultObj.cleanPos = cleanupObj.cleanPos;
				resultObj.startPos = cleanupObj.startPos;
			} while (this.concurrent.get() == 0);
			resultObj.writed = writed;
			resultObj.closed = false;
		} catch (IllegalStateException ex) {
			resultObj.closed = true;
			if (this.maxIndex != resultObj.cleanPos) {
				this.updateMaxIndexQuietly(resultObj.cleanPos);
				this.maxIndex = resultObj.cleanPos;
				this.forceChannel();
				this.setSizeOfRafQuietly();
			}

		} finally {
			this.lock.unlock();
		}
		return resultObj;
	}

	private CleanupObject unitCleanup(CleanupObject currentObj) throws IllegalStateException {
		long paramCleanPos = currentObj.cleanPos;
		long paramStartPos = currentObj.startPos;

		if (paramStartPos == this.maxIndex) {
			throw new IllegalStateException();
		}

		CleanupObject result = new CleanupObject();
		try {
			this.channel.position(paramStartPos);
			ByteBuffer buffer = ByteBuffer.allocate(SIZE_HEADER_SECTION);
			this.channel.read(buffer);
			buffer.flip();

			byte enabled = buffer.get();
			byte[] globalTransactionId = new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH];
			buffer.get(globalTransactionId);
			byte operator = buffer.get();
			short sizeOfBlock = buffer.getShort();
			short sizeOfText = buffer.getShort();

			XidFactory xidFactory = this.beanFactory.getXidFactory();
			TransactionXid globalXid = xidFactory.createGlobalXid(globalTransactionId);
			TransactionHolder holder = this.archives.get(globalXid);
			boolean removeRequired = holder != null && holder.deleted;
			if (enabled == 0x0) {
				result.cleanPos = paramCleanPos;
				result.startPos = paramStartPos + sizeOfBlock;
			} else if (removeRequired) {
				result.cleanPos = paramCleanPos;
				result.startPos = paramStartPos + sizeOfBlock;

				ByteBuffer byteBuf = ByteBuffer.allocate(1);
				byteBuf.put((byte) 0x0);
				byteBuf.flip();

				this.channel.position(paramStartPos);
				this.channel.write(byteBuf);
				result.writed = true;

				if (operator == OPERTOR_DEL_TRANSACTION) {
					this.archives.remove(globalXid);
				}
			} else if (paramCleanPos < paramStartPos) {
				ByteBuffer textBuffer = ByteBuffer.allocate(sizeOfText);
				this.channel.position(paramStartPos + SIZE_HEADER_SECTION);
				this.channel.read(textBuffer);

				long startIndex = paramCleanPos;
				int length = 0;
				do {
					ByteBuffer byteBuf = ByteBuffer.allocate(SIZE_HEADER_SECTION);
					this.channel.position(startIndex);
					this.channel.read(byteBuf);
					byteBuf.flip();

					byteBuf.get();
					byteBuf.get(new byte[XidFactory.GLOBAL_TRANSACTION_LENGTH]);
					byteBuf.get();
					int total = byteBuf.getShort();
					int size = byteBuf.getShort();
					if (length == 0) {
						length = length + size;
					} else {
						length = length + total;
					}
					startIndex = startIndex + total;
				} while (length < sizeOfText);

				ByteBuffer headBuffer = ByteBuffer.allocate(SIZE_HEADER_SECTION);
				if (startIndex > paramStartPos) {
					headBuffer.put(enabled);
					headBuffer.put(globalTransactionId);
					headBuffer.put(operator);
					headBuffer.putShort((short) (startIndex - paramCleanPos));
					headBuffer.putShort(sizeOfText);
				} else if (paramCleanPos + sizeOfBlock < startIndex) {
					headBuffer.put(enabled);
					headBuffer.put(globalTransactionId);
					headBuffer.put(operator);
					headBuffer.putShort((short) (startIndex - paramCleanPos));
					headBuffer.putShort(sizeOfText);

					ByteBuffer byteBuf = ByteBuffer.allocate(1);
					byteBuf.put((byte) 0x0);
					byteBuf.flip();

					this.channel.position(paramStartPos);
					this.channel.write(byteBuf);
				} else {
					headBuffer.put(enabled);
					headBuffer.put(globalTransactionId);
					headBuffer.put(operator);
					headBuffer.putShort(sizeOfBlock);
					headBuffer.putShort(sizeOfText);
				}

				headBuffer.flip();
				textBuffer.flip();

				this.channel.position(paramCleanPos);
				this.channel.write(headBuffer);
				this.channel.write(textBuffer);

				result.writed = true;
				result.cleanPos = startIndex;
				result.startPos = paramStartPos + sizeOfBlock;
			} else {
				result.cleanPos = paramCleanPos + sizeOfBlock;
				result.startPos = paramStartPos + sizeOfBlock;
			}
		} catch (IOException ex) {
			if (result.cleanPos == 0 || result.startPos == 0) {
				result.cleanPos = paramCleanPos;
				result.startPos = paramStartPos;
			}
			result.error = true;
			logger.error("Error occurred while cleaning up the transaction logger.", ex);
		} catch (RuntimeException ex) {
			if (result.cleanPos == 0 || result.startPos == 0) {
				result.cleanPos = paramCleanPos;
				result.startPos = paramStartPos;
			}
			result.error = true;
			logger.error("Error occurred while cleaning up the transaction logger.", ex);
		}

		return result;
	}

	private void sleepMillis(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException ex) {
			logger.debug(ex.getMessage());
		}
	}

	public void release() {
		this.released = true;
	}

	private void mkdirDirectoryIfNecessary() {
		File absoluteFile = this.storage.getAbsoluteFile();
		File directory = absoluteFile.getParentFile();
		if (directory != null && directory.exists() == false) {
			directory.mkdirs();
		}
	}

	private void updateMaxIndex(long maxIndex) throws IOException {
		this.channel.position(IDENTIFIER.length + 2 + 4 + 8);
		ByteBuffer byteBuf = ByteBuffer.allocate(8);
		byteBuf.putLong(maxIndex);
		byteBuf.flip();
		this.channel.write(byteBuf);
	}

	private void updateMaxIndexQuietly(long maxIndex) {
		try {
			this.channel.position(IDENTIFIER.length + 2 + 4 + 8);
			ByteBuffer byteBuf = ByteBuffer.allocate(8);
			byteBuf.putLong(maxIndex);
			byteBuf.flip();
			this.channel.write(byteBuf);
		} catch (IOException ex) {
			logger.warn("Error occurred while modify max-index.");
		}
	}

	private void setSizeOfRafQuietly() {
		try {
			this.raf.setLength(this.maxIndex);
		} catch (IOException ex) {
			logger.warn("Error occurred while modify max-index.");
		}
	}

	private void closeRaf() {
		this.closeChannel();
		if (this.raf != null) {
			try {
				this.raf.close();
			} catch (IOException ioex) {
				logger.warn("Error occurred while closing the raf.");
			}
		}
	}

	private void closeChannel() {
		if (this.channel != null) {
			try {
				this.channel.close();
			} catch (IOException ioex) {
				logger.warn("Error occurred while closing the channel.");
			}
		}
	}

	private void forceChannel() {
		if (this.channel != null) {
			try {
				this.channel.force(false);
			} catch (IOException ex) {
				logger.warn("Error occurred while forcing the raf.");
			}
		}
	}

	public void setStorage(String storageFile) {
		this.storage = new File(storageFile);
	}

	public SerializedObject serialize(TransactionArchive archive) {

		SerializedObject serializedObj = new SerializedObject();

		Xid xid = archive.getXid();
		serializedObj.key = xid.getGlobalTransactionId();

		ByteBuffer buffer = ByteBuffer.allocate(4096);
		int status = archive.getStatus();
		buffer.put((byte) status);
		int vote = archive.getVote();
		buffer.put((byte) vote);
		byte coordinator = archive.isCoordinator() ? (byte) 1 : (byte) 0;
		buffer.put((byte) coordinator);

		List<XAResourceArchive> nativeResources = archive.getNativeResources();
		List<XAResourceArchive> remoteResources = archive.getRemoteResources();
		int nativeNumber = nativeResources.size();
		int remoteNumber = remoteResources.size();
		buffer.put((byte) nativeNumber);
		buffer.put((byte) remoteNumber);

		for (int i = 0; i < nativeNumber; i++) {
			XAResourceArchive resourceArchive = nativeResources.get(i);

			SerializedObject serialized = this.serializeResource(resourceArchive);
			byte[] resourceByteArray = serialized.value;

			buffer.put((byte) resourceByteArray.length);
			buffer.put(resourceByteArray);
		}

		for (int i = 0; i < remoteNumber; i++) {
			XAResourceArchive resourceArchive = remoteResources.get(i);

			SerializedObject serialized = this.serializeResource(resourceArchive);
			byte[] resourceByteArray = serialized.value;

			buffer.put((byte) resourceByteArray.length);
			buffer.put(resourceByteArray);
		}

		int pos = buffer.position();
		byte[] byteArray = new byte[pos];
		buffer.flip();
		buffer.get(byteArray);

		serializedObj.value = byteArray;

		return serializedObj;
	}

	public TransactionArchive deserialize(SerializedObject serializedObj) throws IOException {
		byte[] key = serializedObj.key;
		byte[] value = serializedObj.value;
		ByteBuffer buffer = ByteBuffer.wrap(value);

		TransactionArchive archive = new TransactionArchive();
		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionXid globalXid = xidFactory.createGlobalXid(key);
		archive.setXid(globalXid);

		int status = buffer.get();
		int vote = buffer.get();
		int coordinatorValue = buffer.get();

		archive.setStatus(status);
		archive.setVote(vote);
		archive.setCoordinator(coordinatorValue != 0);

		int nativeNumber = buffer.get();
		int remoteNumber = buffer.get();
		for (int i = 0; i < nativeNumber; i++) {
			int length = buffer.get();
			byte[] resourceByteArray = new byte[length];
			buffer.get(resourceByteArray);

			SerializedObject serialized = new SerializedObject();
			serialized.key = key;
			serialized.value = resourceByteArray;

			XAResourceArchive resourceArchive = this.deserializeResource(serialized);
			archive.getNativeResources().add(resourceArchive);
		}
		for (int i = 0; i < remoteNumber; i++) {
			int length = buffer.get();
			byte[] resourceByteArray = new byte[length];
			buffer.get(resourceByteArray);

			SerializedObject serialized = new SerializedObject();
			serialized.key = key;
			serialized.value = resourceByteArray;

			XAResourceArchive resourceArchive = this.deserializeResource(serialized);
			archive.getRemoteResources().add(resourceArchive);
		}
		return archive;
	}

	private SerializedObject serializeResource(XAResourceArchive resourceArchive) {

		SerializedObject serializedObj = new SerializedObject();

		Xid branchXid = resourceArchive.getXid();
		byte[] globalTransactionId = branchXid.getGlobalTransactionId();
		serializedObj.key = globalTransactionId;

		ByteBuffer buffer = ByteBuffer.allocate(4096);

		byte[] branchQualifier = branchXid.getBranchQualifier();
		buffer.put(branchQualifier);

		XAResourceDescriptor descriptor = resourceArchive.getDescriptor();
		if (CommonResourceDescriptor.class.isInstance(descriptor)) {
			String identifier = descriptor.getIdentifier();
			byte[] identifierByteArray = identifier.getBytes();
			buffer.put((byte) 0x1);
			buffer.put((byte) identifierByteArray.length);
			buffer.put(identifierByteArray);
		} else if (RemoteResourceDescriptor.class.isInstance(descriptor)) {
			String identifier = descriptor.getIdentifier();
			byte[] identifierByteArray = identifier.getBytes();
			buffer.put((byte) 0x2);
			buffer.put((byte) identifierByteArray.length);
			buffer.put(identifierByteArray);
		} else {
			buffer.put((byte) 0x0);
			buffer.put((byte) 0x0);
		}

		byte branchVote = (byte) resourceArchive.getVote();
		byte readonly = resourceArchive.isReadonly() ? (byte) 1 : (byte) 0;
		byte committed = resourceArchive.isCommitted() ? (byte) 1 : (byte) 0;
		byte rolledback = resourceArchive.isRolledback() ? (byte) 1 : (byte) 0;
		byte completed = resourceArchive.isCompleted() ? (byte) 1 : (byte) 0;
		byte heuristic = resourceArchive.isHeuristic() ? (byte) 1 : (byte) 0;

		buffer.put(branchVote);
		buffer.put(readonly);
		buffer.put(committed);
		buffer.put(rolledback);
		buffer.put(completed);
		buffer.put(heuristic);

		int position = buffer.position();
		byte[] byteArray = new byte[position];
		buffer.flip();
		buffer.get(byteArray);

		serializedObj.value = byteArray;

		return serializedObj;
	}

	private XAResourceArchive deserializeResource(SerializedObject serializedObj) throws IOException {

		byte[] key = serializedObj.key;
		byte[] value = serializedObj.value;

		ByteBuffer buffer = ByteBuffer.wrap(value);

		XAResourceArchive archive = new XAResourceArchive();

		byte[] branchQualifier = new byte[XidFactory.BRANCH_QUALIFIER_LENGTH];
		buffer.get(branchQualifier);
		XidFactory xidFactory = this.beanFactory.getXidFactory();
		TransactionXid globalXid = xidFactory.createGlobalXid(key);
		TransactionXid branchXid = xidFactory.createBranchXid(globalXid, branchQualifier);
		archive.setXid(branchXid);

		XAResourceDescriptor descriptor = null;
		byte resourceType = buffer.get();
		byte length = buffer.get();
		byte[] byteArray = new byte[length];
		buffer.get(byteArray);
		String identifier = new String(byteArray);

		if (resourceType == 0x01) {
			archive.setIdentified(true);
			CommonResourceDescriptor resourceDescriptor = new CommonResourceDescriptor();
			XAResource resource = this.deserializer.deserialize(identifier);
			resourceDescriptor.setDelegate(resource);
			descriptor = resourceDescriptor;
		} else if (resourceType == 0x02) {
			archive.setIdentified(true);
			RemoteResourceDescriptor resourceDescriptor = new RemoteResourceDescriptor();
			XAResource resource = this.deserializer.deserialize(identifier);
			resourceDescriptor.setDelegate((RemoteCoordinator) resource);
			descriptor = resourceDescriptor;
			descriptor = resourceDescriptor;
		} else {
			UnidentifiedResourceDescriptor resourceDescriptor = new UnidentifiedResourceDescriptor();
			descriptor = resourceDescriptor;
		}
		archive.setDescriptor(descriptor);

		int branchVote = buffer.get();
		int readonly = buffer.get();
		int committedValue = buffer.get();
		int rolledbackValue = buffer.get();
		int completedValue = buffer.get();
		int heuristicValue = buffer.get();
		archive.setVote(branchVote);
		archive.setReadonly(readonly != 0);
		archive.setCommitted(committedValue != 0);
		archive.setRolledback(rolledbackValue != 0);
		archive.setCompleted(completedValue != 0);
		archive.setHeuristic(heuristicValue != 0);

		return archive;

	}

	static class TransactionHolder {
		public TransactionArchive archive;
		public boolean deleted;
	}

	static class SerializedObject {
		public byte[] key;
		public byte[] value;
	}

	static class CleanupObject {
		public long cleanPos;
		public long startPos;
		public boolean error;
		public boolean closed;
		public boolean writed;
	}

	public void setDeserializer(XAResourceDeserializer deserializer) {
		this.deserializer = deserializer;
	}

	public void setBeanFactory(TransactionBeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

}
