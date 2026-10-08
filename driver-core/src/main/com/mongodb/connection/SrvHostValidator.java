/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.connection;

/**
 * Checks that a host name in a discovered SRV record is allowed.
 *
 * <p>The purpose of this interface is to allow custom validation logic for SRV records.</p>
 *
 * <p><b>WARNING:</b> Modifying the default SRV domain name validation can create vulnerabilities.</p>
 */
public interface SrvHostValidator {

    /**
     * Validate the host name discovered in an SRV record.
     *
     * <p>Checks the host name provided to determine whether the driver should connect to it or not.</p>
     * @param discoveredHostName the discovered hostname; the name will be lowercase Punycode (ASCII) without a trailing period
     * @return true if the host name can be used; false otherwise
     */
    boolean isValidHost(String discoveredHostName);
}

