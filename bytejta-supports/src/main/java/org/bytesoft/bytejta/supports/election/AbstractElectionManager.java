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
package org.bytesoft.bytejta.supports.election;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.bytesoft.common.utils.ByteUtils;
import org.bytesoft.common.utils.CommonUtils;
import org.bytesoft.transaction.TransactionLock;
import org.bytesoft.transaction.aware.TransactionEndpointAware;
import org.bytesoft.transaction.xa.TransactionXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

public abstract class AbstractElectionManager
		implements InitializingBean, TransactionEndpointAware, TransactionLock, LeaderSelectorListener {
	static final Logger logger = LoggerFactory.getLogger(AbstractElectionManager.class);

	static final String CONSTANTS_ROOT_PATH = "/org/bytesoft/bytejta";

	private Lock lock = new ReentrantLock();
	private Condition condition = this.lock.newCondition();

	private String endpoint;

	private volatile ConnectionState state;
	private LeaderSelector leadSelector;

	public boolean lockTransaction(TransactionXid transactionXid, String identifier) {
		byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
		String basePath = String.format("%s/%s", CONSTANTS_ROOT_PATH, this.getApplication());
		String globalId = String.format("%s/%s", basePath, ByteUtils.byteArrayToString(globalTransactionId));
		byte[] byteData = StringUtils.isBlank(identifier) ? new byte[0] : identifier.getBytes();
		try {
			this.getCuratorFramework().create().creatingParentContainersIfNeeded() //
					.withMode(CreateMode.PERSISTENT).forPath(globalId, byteData);
			return true;
		} catch (NodeExistsException nex) {
			logger.error("Path exists(path= {})!", globalId, nex);
		} catch (Exception ex) {
			logger.error("Error occurred while locking transaction(global= {})!",
					ByteUtils.byteArrayToString(globalTransactionId), ex);
		}

		return false;
	}

	public void unlockTransaction(TransactionXid transactionXid, String identifier) {
		byte[] globalTransactionId = transactionXid.getGlobalTransactionId();
		String basePath = String.format("%s/%s", CONSTANTS_ROOT_PATH, this.getApplication());
		String globalId = String.format("%s/%s", basePath, ByteUtils.byteArrayToString(globalTransactionId));
		try {
			this.getCuratorFramework().delete().forPath(globalId);
		} catch (Exception ex) {
			logger.error("Error occurred while unlocking transaction(global= {})!",
					ByteUtils.byteArrayToString(globalTransactionId), ex);
		}
	}

	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		try {
			this.lock.lock();
			this.state = newState;
			this.condition.signal();
		} finally {
			this.lock.unlock();
		}
	}

	public void takeLeadership(CuratorFramework client) throws Exception {
		try {
			this.lock.lock();
			if (ConnectionState.CONNECTED.equals(this.state)) {
				this.condition.await();
			} else {
				logger.debug("Wrong state! Re-elect the master node.");
			}
		} finally {
			this.lock.unlock();
		}
	}

	public boolean hasLeadership() {
		return this.leadSelector == null ? false : this.leadSelector.hasLeadership();
	}

	public void afterPropertiesSet() throws Exception {
		String basePath = String.format("%s/%s", CONSTANTS_ROOT_PATH, this.getApplication());
		this.createPersistentPathIfNecessary(basePath);

		String masterPath = String.format("%s/master", basePath);
		this.leadSelector = new LeaderSelector(this.getCuratorFramework(), masterPath, this);
		this.leadSelector.start();
	}

	private void createPersistentPathIfNecessary(String path) throws Exception {
		try {
			this.getCuratorFramework().create() //
					.creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
		} catch (NodeExistsException nex) {
			logger.debug("Path exists(path= {})!", path);
		}
	}

	private String getApplication() {
		return CommonUtils.getApplication(this.endpoint);
	}

	public abstract CuratorFramework getCuratorFramework();

	public void setEndpoint(String identifier) {
		this.endpoint = identifier;
	}

}
