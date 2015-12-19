package org.bytesoft.transaction.internal;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;

public class SynchronizationList implements Synchronization {
	private final List<Synchronization> synchronizations = new ArrayList<Synchronization>();

	public synchronized void registerSynchronization(Synchronization sync) throws RollbackException, IllegalStateException,
			SystemException {
		SynchronizationImpl synchronization = new SynchronizationImpl(sync);
		this.synchronizations.add(synchronization);
	}

	public void beforeCompletion() {
		int length = this.synchronizations.size();
		for (int i = 0; i < length; i++) {
			Synchronization synchronization = this.synchronizations.get(i);
			synchronization.beforeCompletion();
		}// end-for
	}

	public void afterCompletion(int status) {
		int length = this.synchronizations.size();
		for (int i = 0; i < length; i++) {
			Synchronization synchronization = this.synchronizations.get(i);
			synchronization.afterCompletion(status);
		}// end-for
	}

}
