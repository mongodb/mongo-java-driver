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

import com.mongodb.lang.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Top level Exception for all Exceptions, server-side or client-side, that come from the driver.
 */
public class MongoException extends RuntimeException {

    /**
     * An error label indicating that the exception can be treated as a transient transaction error.
     *
     * @see #hasErrorLabel(String)
     * @since 3.8
     */
    public static final String TRANSIENT_TRANSACTION_ERROR_LABEL = "TransientTransactionError";

    /**
     * An error label indicating that the exception can be treated as an unknown transaction commit result.
     *
     * @see #hasErrorLabel(String)
     * @since 3.8
     */
    public static final String UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL = "UnknownTransactionCommitResult";

    private static final long serialVersionUID = -4415279469780082174L;

    private final int code;
    private final Set<String> errorLabels = new HashSet<String>();

    /**
     * Static helper to create or cast a MongoException from a throwable
     *
     * @param t a throwable, which may be null
     * @return a MongoException
     */
    @Nullable
    public static MongoException fromThrowable(@Nullable final Throwable t) {
        if (t == null) {
            return null;
        } else {
            return fromThrowableNonNull(t);
        }
    }

    /**
     * Static helper to create or cast a MongoException from a throwable
     *
     * @param t a throwable, which may not be null
     * @return a MongoException
     * @since 3.7
     */
    public static MongoException fromThrowableNonNull(final Throwable t) {
      if (t instanceof MongoException) {
            return (MongoException) t;
        } else {
            return new MongoException(t.getMessage(), t);
        }
    }

    /**
     * @param msg the message
     */
    public MongoException(final String msg) {
        super(msg);
        code = -3;
    }

    /**
     * @param code the error code
     * @param msg  the message
     */
    public MongoException(final int code, final String msg) {
        super(msg);
        this.code = code;
    }

    /**
     * @param msg the message
     * @param t   the throwable cause
     */
    public MongoException(final String msg, final Throwable t) {
        super(msg, t);
        code = -4;
    }

    /**
     * @param code the error code
     * @param msg  the message
     * @param t    the throwable cause
     */
    public MongoException(final int code, final String msg, final Throwable t) {
        super(msg, t);
        this.code = code;
        if (t instanceof MongoException) {
            for (final String errorLabel : ((MongoException) t).getErrorLabels()) {
                addLabel(errorLabel);
            }
        }
    }

    /**
     * Gets the exception code
     *
     * @return the error code.
     */
    public int getCode() {
        return code;
    }

    /**
     * Adds the given error label to the exception.
     *
     * @param errorLabel the non-null error label to add to the exception
     *
     * @since 3.8
     */
    public void addLabel(final String errorLabel) {
        notNull("errorLabel", errorLabel);
        errorLabels.add(errorLabel);
    }

    /**
     * Removes the given error label from the exception.
     *
     * @param errorLabel the non-null error label to remove from the exception
     *
     * @since 3.8
     */
    public void removeLabel(final String errorLabel) {
        notNull("errorLabel", errorLabel);
        errorLabels.remove(errorLabel);
    }

    /**
     * Gets the set of error labels associated with this exception.
     *
     * @return the error labels, which may not be null but may be empty
     * @since 3.8
     */
    public Set<String> getErrorLabels() {
        return Collections.unmodifiableSet(errorLabels);
    }

    /**
     * Return true if the exception is labelled with the given error label, and false otherwise.
     *
     * @param errorLabel the non-null error label
     * @return true if the exception is labelled with the given error label
     * @since 3.8
     */
    public boolean hasErrorLabel(final String errorLabel) {
        notNull("errorLabel", errorLabel);
        return errorLabels.contains(errorLabel);
    }

}
