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

package org.apache.ignite.failure;

import junit.framework.TestCase;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.GridAbstractTest;

import static org.apache.ignite.testframework.junits.GridAbstractTest.testIsRunning;

/**
 * Stops node and fails test.
 */
public class TestFailingFailureHandler extends StopNodeFailureHandler {
    /** Test. */
    @GridToStringExclude
    private final GridAbstractTest test;

    /**
     * @param test Test.
     */
    public TestFailingFailureHandler(GridAbstractTest test) {
        this.test = test;
    }

    /** {@inheritDoc} */
    @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
        if (!testIsRunning) {
            ignite.log().info("Critical issue detected after test finished. Test failure handler ignore it.");

            return true;
        }

        boolean nodeStopped = super.handle(ignite, failureCtx);

        TestCase.fail(failureCtx.toString());

        test.handleFailure(failureCtx.error(), false);

        return nodeStopped;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TestFailingFailureHandler.class, this);
    }
}
