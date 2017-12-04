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

package org.apache.ignite;

import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

public class RecordJob implements IgniteCallable<Integer> {
    private String cacheName;

    private TestJobsKey key;

    @IgniteInstanceResource
    private Ignite ignite;

    @LoggerResource
    private IgniteLogger log;

    public RecordJob(String name, TestJobsKey key) {
        this.cacheName = name;
        this.key = key;
    }

    @Override public Integer call() throws Exception {
        log.info("[RecordJob][Remote][key:" + key + "][Start]");

        IgniteAtomicSequence idGen = ignite.atomicSequence("IG_GENERATOR", 22_08_1984L, true);

        IgniteCache<TestJobsKey, TestJobsValue> cache = ignite.cache(cacheName);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 2*60000L, 40)) {
            int i = (int)idGen.getAndIncrement();

            TestJobsValue oldVal = cache.get(key);

            TestJobsValue val = new TestJobsValue(i * 10);

            cache.put(key, val);

            log.info("[RecordJob][Remote][key:" + key + ", oldVal=" + oldVal + ", val=" + val + "][Finish]");

            tx.commit();
        }

        log.info("[RecordJob][Remote][key:" + key + "][Finish]");
        return null;
    }
}