package org.bytesoft.transaction.internal;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.bytesoft.transaction.TransactionListener;

public class TransactionListenerList implements TransactionListener {
	static final Logger logger = Logger.getLogger(TransactionListenerList.class.getSimpleName());

	private final List<TransactionListener> listeners = new ArrayList<TransactionListener>();

	public void registerTransactionListener(TransactionListener listener) {
		this.listeners.add(listener);
	}

	public void prepareStart() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.prepareStart();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void prepareComplete(boolean success) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.prepareComplete(success);
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void commitStart() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.commitStart();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void commitSuccess() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.commitSuccess();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void commitFailure(int optcode) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.commitFailure(optcode);
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void rollbackStart() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.rollbackStart();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void rollbackSuccess() {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.rollbackSuccess();
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

	public void rollbackFailure(int optcode) {
		for (int i = 0; i < this.listeners.size(); i++) {
			try {
				TransactionListener listener = this.listeners.get(i);
				listener.rollbackFailure(optcode);
			} catch (RuntimeException rex) {
				logger.debug(rex.getMessage(), rex);
			}
		}
	}

}
