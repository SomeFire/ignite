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

/**
 * A savepoint is a special mark inside a transaction that allows all commands
 * that are executed after it was established to be rolled back,
 * restoring the transaction state to what it was at the time of the savepoint.
 *
 * <h1 class="header">Usage</h1>
 *
 * The execution result guarantee that only values 1 and 3 are inserted into cache:
 *
 * <pre name="code" class="java">
 * Ignite ignite = ....;
 * IgniteCache<Integer, Integer> c = ....;
 *
 * try (Transaction tx = ignite.transactions().txStart()) {
 *     c.put(1, 1);
 *
 *     tx.savepoint("mysavepoint");
 *
 *     c.put(2, 2);
 *
 *     tx.rollbackToSavepoint("mysavepoint");
 *
 *     c.put(3, 3);
 *
 *     tx.commit();
 * }
 * </pre>
 *
 * The result of this transaction is:
 * <p>
 * cache.get(1) - 1,
 * <br>
 * cache.get(2) - null,
 * <br>
 * cache.get(3) - null.
 * </p>
 * <h1 class="header">Restrictions</h1>
 *
 * <ul>
 * <li>Savepoints works only with {@code TRANSACTIONAL} caches on the node where transaction started.</li>
 * <li>In case of {@code ATOMIC} caches and caches from other nodes - they will be ignored.
 * There is no need in savepoints because every action commits immediately.</li>
 * </ul>
 */
public interface TxSavepoint {

    /**
     * @return Savepoint ID.
     */
	public String getName();

}