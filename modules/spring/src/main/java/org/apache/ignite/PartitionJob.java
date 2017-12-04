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

import java.util.ArrayList;
import java.util.Iterator;
import javax.cache.Cache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

public class PartitionJob implements IgniteRunnable {
    private String cacheName;

    private int partition;

    private int processOneOf;

    private int maxRunningJobs;

    private long waitForJobs;

    @IgniteInstanceResource
    private Ignite ignite;
    @LoggerResource
    private IgniteLogger log;

    public PartitionJob(String cacheName, int partition, int processOneOf, int maxRunningJobs, int waitForJobs) {
        this.cacheName = cacheName;
        this.partition = partition;
        this.processOneOf = processOneOf;
        this.maxRunningJobs = maxRunningJobs;
        this.waitForJobs = waitForJobs;
    }

    @Override public void run() {
        log.info("[Remote][partition:" + partition + "][cacheName:" + cacheName + "][Start]");

        IgniteCache<TestJobsKey, TestJobsValue> productCache = ignite.cache(cacheName);

        try (QueryCursor<Cache.Entry<TestJobsKey, TestJobsValue>> query =
                 productCache.query(new ScanQuery<TestJobsKey, TestJobsValue>(partition))) {
            int cnt = 0;

            ArrayList<IgniteFuture<Integer>> recordJobs = new ArrayList<>();

            for (Cache.Entry<TestJobsKey, TestJobsValue> entry : query) {
                log.info("[Remote][partition:" + partition + "][QueryCursor] key=" + entry.getKey().k + ", val=" + entry.getValue().v);

                IgniteFuture<Integer> fut = ignite.compute().affinityCallAsync(cacheName, entry.getKey(),
                    new RecordJob(cacheName, entry.getKey()));

                recordJobs.add(fut);
            }

            for (IgniteFuture<Integer> job : recordJobs) {
                job.get();
            }
        }

        log.info("[Remote][partition:" + partition + "][cacheName:" + cacheName + "][End]");
    }

    private void printCount(QueryCursor<Cache.Entry<TestJobsKey, TestJobsValue>> q) {
        int i = 0;
        Iterator<Cache.Entry<TestJobsKey, TestJobsValue>> it = q.iterator();
        while (it.hasNext()) {
            log.info("[Remote][partition:" + partition + "][printCount] " + i);
            i++;

            Cache.Entry<TestJobsKey, TestJobsValue> e = it.next();
            log.info("[Remote][partition:" + partition + "][printCount] " + i + ": k="+e.getKey()+", v="+e.getValue());

            IgniteCache<TestJobsKey, TestJobsValue> c = ignite.cache(cacheName);
            TestJobsValue v = c.get(new TestJobsKey(partition));
            log.info("[Remote][partition:" + partition + "][printCount] " + i + ": TestJobValue="+c);
        }
        log.info("[Remote][partition:" + partition + "][printCount] count=" + i);
    }
}