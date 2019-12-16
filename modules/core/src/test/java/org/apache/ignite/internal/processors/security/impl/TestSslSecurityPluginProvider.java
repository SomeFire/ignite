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

package org.apache.ignite.internal.processors.security.impl;

import java.security.Permissions;
import java.util.Arrays;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

/**
 *
 */
public class TestSslSecurityPluginProvider extends TestSecurityPluginProvider {
    /** Security certificates attribute name. Attribute is not available via public API. */
    public static final String ATTR_SECURITY_CERTIFICATES = /*ATTR_PREFIX +*/ "s.security.certs";

    /** Check ssl certificates. */
    protected final boolean checkSslCerts;

    /** */
    public TestSslSecurityPluginProvider(String login, String pwd, SecurityPermissionSet perms,
        boolean globalAuth, boolean checkSslCerts, TestSecurityData... clientData) {
        super(login, pwd, perms, globalAuth, clientData);

        this.checkSslCerts = checkSslCerts;
    }

    /** {@inheritDoc} */
    @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
        return new TestSslSecurityProcessor(ctx,
            new TestSecurityData(login, pwd, perms, new Permissions()),
            Arrays.asList(clientData),
            globalAuth,
            checkSslCerts);
    }
}
