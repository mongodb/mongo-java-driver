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

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.lang.Nullable;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedAction;

/**
 * This interface enables applications to take full control of the lifecycle of the {@link Subject} with which authentication requests
 * are executed.  For each authentication request, the driver will call the {@link #getSubject()} method and execute the SASL
 * conversation via a call to {@link Subject#doAs(Subject, PrivilegedAction)}.
 * <p>
 * Implementations of this interface will typically cache a {@code Subject} instance for some period of time before replacing it with a
 * different instance, say, after the expiration time of a ticket has passed.
 * </p>
 * <p>
 * Applications should register an instance of a class implementation this interface as a mechanism property of a {@link MongoCredential}
 * via a call to {@link MongoCredential#withMechanismProperty(String, Object)} using the key
 * {@link MongoCredential#JAVA_SUBJECT_PROVIDER_KEY}
 * </p>
 * <p>
 * If use of the same {@code Subject} for the lifetime of the application is sufficient, an application can simply create a single
 * {@code Subject} and associate it with a {@code MongoCredential} as a mechanism property using the key
 * {@link MongoCredential#JAVA_SUBJECT_KEY}.
 *
 * </p>
 * @see MongoCredential
 * @see MongoCredential#JAVA_SUBJECT_PROVIDER_KEY
 * @since 4.2
 */
@ThreadSafe
public interface SubjectProvider {

    /**
     * Gets the Subject to use for an authentication request.
     *
     * @return the {@code Subject}, which may be null
     * @throws LoginException a login exception
     */
    @Nullable
    Subject getSubject() throws LoginException;
}
