/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Transaction savepoint implementation for near local transactions.
 */
public class TxSavepointLocal implements TxSavepoint {

    /** Savepoint ID */
    @GridToStringInclude
	private final String name;

    /** Per-transaction read map. */
    @GridToStringInclude
	private final Map<IgniteTxKey, IgniteTxEntry> txMap;

    /**
     * @param name Savepoint ID.
     * @param tx Transaction, which state should be saved.
     */
	public TxSavepointLocal(String name,
                            IgniteInternalTx tx) {
		this.name = name;

        Collection<IgniteTxEntry> stateEntries = tx.txState().allEntries();

        txMap = U.newLinkedHashMap(stateEntries.size());

        putCopies(stateEntries, txMap, tx);
	}

	/** {@inheritDoc} */
	public String getName() {
		return name;
	}

    /**
     * @return Entries stored in savepoint.
     */
	public Map<IgniteTxKey, IgniteTxEntry> getTxMap() {
		return txMap;
	}

    /**
     *
     * @param key Key object.
     * @return True if savepoint contains entry with specified key.
     */
	public boolean containsKey(IgniteTxKey key) {
        return txMap.containsKey(key);
    }

    /**
     *
     * @param from Takes this.
     * @param to And put all members here.
     * @param tx Transaction where entries occurs.
     */
    public void putCopies(Collection<IgniteTxEntry> from, Map<IgniteTxKey, IgniteTxEntry> to, IgniteInternalTx tx) {
        for (IgniteTxEntry entry : from) {
            IgniteTxEntry e = entry.cleanCopy(entry.context());

            e.cached(entry.cached());

            if (tx.pessimistic() && entry.locked())
                e.markLocked();

            to.put(entry.txKey(), e);
        }
    }

    /** Equality of savepoints depends on their IDs. */
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TxSavepointLocal savepoint = (TxSavepointLocal) o;

		return name.equals(savepoint.name);
	}

    /** Hash code depends on savepoint ID. */
	@Override
	public int hashCode() {
		return name.hashCode();
	}
}