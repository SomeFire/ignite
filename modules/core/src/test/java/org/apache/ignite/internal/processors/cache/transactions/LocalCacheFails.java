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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public class LocalCacheFails extends GridCommonAbstractTest {
    /** */
    private CacheConfiguration<Integer, Integer> getConfig(CacheMode cacheMode) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

        cfg.setCacheMode(cacheMode);
        cfg.setName(cacheMode.name());
        cfg.setAtomicityMode(TRANSACTIONAL);

        return cfg;
    }

    /** */
    public void testLocalCache() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, Integer> locCache = ignite.createCache(getConfig(LOCAL));
        IgniteCache<Integer, Integer> partCache = ignite.createCache(getConfig(PARTITIONED));

        try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
            locCache.put(1, 1);
            partCache.put(1, 1);

            tx.commit();
        }
    }
}