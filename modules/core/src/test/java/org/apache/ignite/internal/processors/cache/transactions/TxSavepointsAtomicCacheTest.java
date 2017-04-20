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

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class TxSavepointsAtomicCacheTest extends TxSavepointsTest {

    /** {@inheritDoc} */
    @Override protected void checkResult(String errMsg, IgniteCache<Integer, Integer> cache) {
        assertEquals(errMsg, (Integer) 1, cache.get(1));
        assertEquals(errMsg, (Integer) 33, cache.get(2));
        assertEquals(errMsg, null, cache.get(3));
        assertEquals(errMsg, null, cache.get(4));
        assertEquals(errMsg, (Integer) 5, cache.get(5));
        assertEquals(errMsg, (Integer) 33, cache.get(6));
        assertEquals(errMsg, null, cache.get(7));
        assertEquals(errMsg, null, cache.get(8));
        assertEquals(errMsg, (Integer) 9, cache.get(9));
        assertEquals(errMsg, (Integer) 33, cache.get(10));
        assertEquals(errMsg, null, cache.get(11));
        assertEquals(errMsg, null, cache.get(12));
        assertEquals(errMsg, (Integer) 33, cache.get(13));
        assertEquals(errMsg, null, cache.get(14));
        assertEquals(errMsg, null, cache.get(15));
        assertEquals(errMsg, null, cache.get(16));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLocal() throws Exception {
        checkSavepoints(cacheConfig(ATOMIC, LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicated() throws Exception {
        checkSavepoints(cacheConfig(ATOMIC, REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicPartitioned() throws Exception {
        checkSavepoints(cacheConfig(ATOMIC, PARTITIONED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransLocalMultipleCaches() throws Exception {
        checkSavepointsWithTwoCaches(cacheConfig(ATOMIC, LOCAL));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransReplicatedMultipleCaches() throws Exception {
        checkSavepointsWithTwoCaches(cacheConfig(ATOMIC, REPLICATED));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransPartitionedMultipleCaches() throws Exception {
        checkSavepointsWithTwoCaches(cacheConfig(ATOMIC, PARTITIONED));
    }
}