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
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Checks how atomic cache works inside a transaction.
 */
public class AtomicCacheAndTxTest extends GridCommonAbstractTest {

	/** {@inheritDoc} */
	@Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
		IgniteConfiguration cfg = super.getConfiguration(gridName);

		CacheConfiguration ccfg  = new CacheConfiguration();

		ccfg.setAtomicityMode(ATOMIC);

		cfg.setCacheConfiguration(ccfg);

		return cfg;
	}

	/** {@inheritDoc} */
	@Override protected void beforeTestsStarted() throws Exception {
		super.beforeTestsStarted();
		startGrid(0);
	}

	/** {@inheritDoc} */
	@Override protected void afterTest() throws Exception {
		grid(0).cache(null).remove(1);
	}

	/**
	 * Checks that atomic cache works inside a transaction.
	 */
	public void testAllowed() throws Exception {
		checkTransaction(grid(0).cache(null).withAllowInTx(), true);
	}

	/**
	 * Checks that atomic cache throws exception inside a transaction.
	 */
	public void testNotAllowed() {
		checkTransaction(grid(0).cache(null), false);
	}

	/**
	 * @param cache Atomic cache which is allowed or not allowed for use in transactions.
	 * @param isAtomicCacheAllowedInTx Variable to compare with result.
	 */
	private void checkTransaction(IgniteCache<Integer, Integer> cache, boolean isAtomicCacheAllowedInTx) {
		IgniteException err = null;
		try (Transaction tx = grid(0).transactions().txStart()) {
			cache.put(1, 1);
			tx.commit();
		} catch (IgniteException e) {
			err = e;
		}
		assertTrue(isAtomicCacheAllowedInTx && err == null ||
				!isAtomicCacheAllowedInTx && err != null &&
						err.getMessage().startsWith("Transaction spans operations on atomic cache"));

	}
}