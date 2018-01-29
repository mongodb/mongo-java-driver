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

package com.mongodb;

import java.util.HashMap;
import java.util.Map;

/**
 * An enumeration of the MongodDB-supported authentication mechanisms.
 *
 * @since 3.0
 */
public enum AuthenticationMechanism {
    /**
     * The GSSAPI mechanism.  See the <a href="http://tools.ietf.org/html/rfc4752">RFC</a>.
     */
    GSSAPI("GSSAPI"),

    /**
     * The PLAIN mechanism.  See the <a href="http://www.ietf.org/rfc/rfc4616.txt">RFC</a>.
     */
    PLAIN("PLAIN"),

    /**
     * The MongoDB X.509 mechanism. This mechanism is available only with client certificates over SSL.
     */
    MONGODB_X509("MONGODB-X509"),

    /**
     * The MongoDB Challenge Response mechanism.
     */
    MONGODB_CR("MONGODB-CR"),

    /**
     * The SCRAM-SHA-1 mechanism.  See the <a href="http://tools.ietf.org/html/rfc5802">RFC</a>.
     */
    SCRAM_SHA_1("SCRAM-SHA-1");

    private static final Map<String, AuthenticationMechanism> AUTH_MAP = new HashMap<String, AuthenticationMechanism>();
    private final String mechanismName;

    AuthenticationMechanism(final String mechanismName) {
        this.mechanismName = mechanismName;
    }

    /**
     * Get the mechanism name.
     *
     * @return the mechanism name
     */
    public String getMechanismName() {
        return mechanismName;
    }

    @Override
    public String toString() {
        return mechanismName;
    }

    static {
        for (final AuthenticationMechanism value : values()) {
            AUTH_MAP.put(value.getMechanismName(), value);
        }
    }

    /**
     * Gets the mechanism by its name.
     *
     * @param mechanismName the mechanism name
     * @return the mechanism
     * @see #getMechanismName()
     */
    public static AuthenticationMechanism fromMechanismName(final String mechanismName) {
        AuthenticationMechanism mechanism = AUTH_MAP.get(mechanismName);
        if (mechanism == null) {
            throw new IllegalArgumentException("Unsupported authMechanism: " + mechanismName);
        }
        return mechanism;
    }
}
