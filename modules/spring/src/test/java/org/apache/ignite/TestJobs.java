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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Стартуем ноду, с которой отсылаются недостающие классы.
 */
public class TestJobs {
    private static Ignite i;
    private static int processOneOf = Integer.valueOf(System.getProperty("PROCESS_ONE_OF", "3"));
    private static int maxRunningJobs = Integer.valueOf(System.getProperty("MAX_RUNNING_JOBS", "100"));
    private static int maxRunningParts = Integer.valueOf(System.getProperty("MAX_RUNNING_PARTS", "5"));
    private static int waitForParts = Integer.valueOf(System.getProperty("WAIT_FOR_PARTS", "10000"));
    private static int waitForJobs = Integer.valueOf(System.getProperty("WAIT_FOR_JOBS", "2500"));
    private static String cacheName = "cacheName";

    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws Exception {
        i = Ignition.start("examples/config/example-ignite.xml");

        fillCache();

        doPartitionJob();
    }

    private static void doPartitionJob() throws IgniteCheckedException {
        ((IgniteEx)i).context().deploy().deploy(PartitionJob.class, PartitionJob.class.getClassLoader());

        ArrayList<IgniteFuture<Void>> jobs = new ArrayList<>(maxRunningJobs);

        int partCount = i.affinity(cacheName).partitions();
        int part = 0;
        int runningParts = 0;

        while(part != partCount) {
            IgniteFuture<Void> fut = i.compute().affinityRunAsync(cacheName, part,
                new PartitionJob(cacheName, part, processOneOf, maxRunningJobs, waitForJobs));

            jobs.add(fut);
            part += 1;
            runningParts += 1;

            if (runningParts >= maxRunningParts) {
                waitJobs2Finish(jobs, maxRunningParts);
                runningParts = jobs.size();
            }
        }



    }

    private static void waitJobs2Finish(ArrayList<IgniteFuture<Void>> jobs, int maxCount) {
        removeFinished(jobs);
        int notFinished = jobs.size();

        while(notFinished >= maxCount) {
            System.out.println("[WaitingForPartsFinish][notFinished:"+notFinished+"] - sleeping for "+(waitForParts/1000)+"} sec");

            try {
                Thread.sleep(waitForParts);
            } catch (InterruptedException ignore) {}

            removeFinished(jobs);

            notFinished = jobs.size();
        }
    }

    private static void removeFinished(ArrayList<IgniteFuture<Void>> jobs) {
        Iterator<IgniteFuture<Void>> iter = jobs.iterator();

        while (iter.hasNext()) {
            IgniteFuture fut = iter.next();

            if (fut.isDone())
                iter.remove();
        }
    }

    private static void fillCache() {
        IgniteCache<TestJobsKey, TestJobsValue> cache = i.getOrCreateCache(cacheName);

        for (int j = 0; j < 1_000; j++) {
            cache.put(new TestJobsKey(j), new TestJobsValue(j));
        }
    }
}