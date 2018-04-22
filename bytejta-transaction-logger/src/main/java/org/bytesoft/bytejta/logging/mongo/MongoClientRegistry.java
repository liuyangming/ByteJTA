/**
 * Copyright 2014-2017 yangming.liu<bytefox@126.com>.
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
package org.bytesoft.bytejta.logging.mongo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;

public class MongoClientRegistry {
	static final Logger logger = LoggerFactory.getLogger(MongoClientRegistry.class);

	private static MongoClientRegistry instance = new MongoClientRegistry();

	private MongoClientRegistry() {
		if (instance != null) {
			throw new IllegalStateException();
		}
	}

	public static MongoClientRegistry getInstance() {
		return instance;
	}

	private MongoClient mongoClient;
	private Lock lock = new ReentrantLock();
	private Condition condition = this.lock.newCondition();

	public MongoClient getMongoClient() {
		if (this.mongoClient == null) {
			return this.waitMongoClient();
		} else {
			return mongoClient;
		}
	}

	private MongoClient waitMongoClient() {
		try {
			this.lock.lock();
			while (this.mongoClient == null) {
				try {
					this.condition.await(100, TimeUnit.MILLISECONDS);
				} catch (InterruptedException ex) {
					logger.debug(ex.getMessage(), ex);
				}
			}
		} finally {
			this.lock.unlock();
		}
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		try {
			this.lock.lock();
			this.mongoClient = mongoClient;
			this.condition.signalAll();
		} finally {
			this.lock.unlock();
		}
	}

}
