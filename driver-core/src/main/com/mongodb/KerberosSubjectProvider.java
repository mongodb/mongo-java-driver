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
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.Locks.checkedWithLock;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * An implementation of {@link SubjectProvider} suitable for use as the value of the {@link MongoCredential#JAVA_SUBJECT_PROVIDER_KEY}
 * mechanism property for Kerberos credentials, created via {@link MongoCredential#createGSSAPICredential(String)}.
 * <p>
 * An instance of this class will cache a Kerberos {@link Subject} until its TGT is close to expiration, at which point it will replace
 * the {@code Subject} with a new one.
 * </p>
 * <p>
 * {@code Subject} instances are created by first constructing a {@link LoginContext} with the specified name, then calling its
 * {@link LoginContext#login()} method, and finally acquiring the {@code Subject} via a call to {@link LoginContext#getSubject()}.
 * </p>
 *
 * @see LoginContext
 * @see Subject
 * @see KerberosTicket
 * @since 4.2
 */
@ThreadSafe
public class KerberosSubjectProvider implements SubjectProvider {
    private static final Logger LOGGER = Loggers.getLogger("authenticator");
    private static final String TGT_PREFIX = "krbtgt/";

    private final ReentrantLock lock = new ReentrantLock();
    private String loginContextName;
    private String fallbackLoginContextName;
    private Subject subject;

    /**
     * Construct an instance with the default login context name {@code "com.sun.security.jgss.krb5.initiate"}.
     * <p>For compatibility, falls back to {@code "com.sun.security.jgss.initiate"}</p>
     */
    public KerberosSubjectProvider() {
        this("com.sun.security.jgss.krb5.initiate", "com.sun.security.jgss.initiate");
    }

    /**
     * Construct an instance with the specified login context name
     *
     * @param loginContextName the login context name
     */
    public KerberosSubjectProvider(final String loginContextName) {
        this(loginContextName, null);
    }

    private KerberosSubjectProvider(final String loginContextName, @Nullable final String fallbackLoginContextName) {
        this.loginContextName = notNull("loginContextName", loginContextName);
        this.fallbackLoginContextName = fallbackLoginContextName;
    }

    /**
     * Gets a {@code Subject} instance associated with a {@link LoginContext} after its been logged in.
     *
     * @return the non-null {@code Subject} instance
     * @throws LoginException any exception resulting from a call to {@link LoginContext#login()}
     */
    @NonNull
    public Subject getSubject() throws LoginException {
        return checkedWithLock(lock, () -> {
            if (subject == null || needNewSubject(subject)) {
                subject = createNewSubject();
            }
            return subject;
        });
    }

    private Subject createNewSubject() throws LoginException {
        LoginContext loginContext;
        try {
            LOGGER.debug(format("Creating LoginContext with name '%s'", loginContextName));
            loginContext = new LoginContext(loginContextName);
        } catch (LoginException e) {
            if (fallbackLoginContextName == null) {
                throw e;
            }
            LOGGER.debug(format("Creating LoginContext with fallback name '%s'", fallbackLoginContextName));
            loginContext = new LoginContext(fallbackLoginContextName);
            loginContextName = fallbackLoginContextName;
            fallbackLoginContextName = null;
        }

        loginContext.login();
        LOGGER.debug("Login successful");
        return loginContext.getSubject();
    }

    private static boolean needNewSubject(final Subject subject) {
        for (KerberosTicket cur : subject.getPrivateCredentials(KerberosTicket.class)) {
            if (cur.getServer().getName().startsWith(TGT_PREFIX)) {
                if (System.currentTimeMillis() > cur.getEndTime().getTime() - MILLISECONDS.convert(5, MINUTES)) {
                    LOGGER.info("The TGT is close to expiring. Time to reacquire.");
                    return true;
                }
                break;
            }
        }
        return false;
    }
}
