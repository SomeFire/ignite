package org.apache.ignite.transactions;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Marker of transactional runtime exceptions.
 *
 * {@link TransactionDeadlockException}
 * {@link TransactionHeuristicException} If operation performs within transaction that entered an unknown state.
 * {@link TransactionOptimisticException}
 * {@link TransactionRollbackException} If operation performs within transaction that automatically rolled back.
 * {@link TransactionTimeoutException} If operation performs within transaction and timeout occurred.
 */
public class TransactionException extends IgniteException {
	/** Serial version UID. */
	private static final long serialVersionUID = 0L;

	public TransactionException(String msg) {
		super(msg);
	}

	public TransactionException(String msg, @Nullable Throwable cause) {
		super(msg, cause);
	}
}