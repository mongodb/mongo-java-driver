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
package com.mongodb.internal.operation;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.function.RetryContext;
import com.mongodb.internal.async.function.RetryPolicy;
import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.OperationContext.ServerDeprioritization;
import com.mongodb.lang.Nullable;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.TimeoutContext.createMongoTimeoutException;
import static com.mongodb.internal.operation.CommandOperationHelper.NO_WRITES_PERFORMED_ERROR_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.RETRYABLE_WRITE_ERROR_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabelIfNeeded;
import static com.mongodb.internal.operation.CommandOperationHelper.isRetryableException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SpecRetryPolicy.Descriptor.READ;
import static com.mongodb.internal.operation.SpecRetryPolicy.Descriptor.WRITE;
import static com.mongodb.internal.operation.SpecRetryPolicy.Descriptor.assertNoConflicts;
import static com.mongodb.internal.operation.SpecRetryPolicy.ExplicitMaxRetries.NO_RETRIES;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;

/**
 * Implements all specification retry policies.
 */
final class SpecRetryPolicy implements RetryPolicy {
    private static final int INFINITE_ATTEMPTS = Integer.MAX_VALUE;

    private final Set<Descriptor> descriptors;
    private final boolean effectiveRetrySetting;
    private final boolean retryRequirementsMaybeMet;
    private final ExplicitMaxRetries explicitMaxRetries;
    private final int maxAttempts;
    private final ServerDeprioritization serverDeprioritization;
    private Supplier<String> commandDescriptionSupplier;
    @Nullable
    private Boolean writeRetryRequirementsMet;
    @Nullable
    private Integer maxWireVersion;

    /**
     * @param descriptors A set of descriptors of the applicable specification retry policies.
     * @param effectiveRetrySetting See {@link MongoClientSettings#getRetryWrites()}, {@link MongoClientSettings#getRetryReads()}.
     * Note that for some commands, like {@code commitTransaction}/{@code abortTransaction},
     * retries are deemed to be enabled regardless of the settings; this argument must be {@code true} for them.
     * @param retryRequirementsMaybeMet {@code false} iff the retry requirements are known not to be met.
     * For example, if {@code effectiveRetrySetting} is {@code false}, then {@code retryRequirementsMaybeMet} must be {@code false}.
     * <p>
     * For {@link Descriptor#READ}, this parameter specifies whether the read retry requirements are met;
     * for {@link Descriptor#WRITE}, see {@link #onWriteRetryRequirements(boolean, ConnectionDescription)}.
     */
    SpecRetryPolicy(
            final Set<Descriptor> descriptors,
            final boolean effectiveRetrySetting,
            final boolean retryRequirementsMaybeMet,
            final ExplicitMaxRetries explicitMaxRetries,
            final ServerDeprioritization serverDeprioritization) {
        this.descriptors = assertNoConflicts(descriptors);
        assertTrue(effectiveRetrySetting || !retryRequirementsMaybeMet);
        this.effectiveRetrySetting = effectiveRetrySetting;
        this.retryRequirementsMaybeMet = retryRequirementsMaybeMet;
        if (!retryRequirementsMaybeMet) {
            assertTrue(explicitMaxRetries == NO_RETRIES);
        }
        this.explicitMaxRetries = explicitMaxRetries;
        this.maxAttempts = explicitMaxRetries.maxAttempts(descriptors);
        this.serverDeprioritization = serverDeprioritization;
        commandDescriptionSupplier = () -> null;
        writeRetryRequirementsMet = null;
        maxWireVersion = null;
    }

    void onAttemptStart(final RetryContext retryContext, final OperationContext operationContext) {
        if (LOGGER.isDebugEnabled() && !retryContext.isFirstAttempt()) {
            String commandDescription = commandDescriptionSupplier.get();
            long operationId = operationContext.getId();
            Throwable prospectiveFailedResult = retryContext.getProspectiveFailedResult().orElseThrow(Assertions::fail);
            int oneBasedAttempt = retryContext.attempt() + 1;
            LOGGER.debug(commandDescription == null
                    ? format("Retrying a command within the operation with operation ID %s due to the error \"%s\". Retry attempt number: #%d",
                            operationId, prospectiveFailedResult, oneBasedAttempt)
                    : format("Retrying the command '%s' within the operation with operation ID %s due to the error \"%s\". Retry attempt number: #%d",
                            commandDescription, operationId, prospectiveFailedResult, oneBasedAttempt));
        }
    }

    /**
     * @return {@code this}.
     */
    SpecRetryPolicy onCommand(final Supplier<String> commandDescriptionSupplier) {
        this.commandDescriptionSupplier = commandDescriptionSupplier;
        return this;
    }

    /**
     * The information gathered via this method is reset after each invocation of {@link #onAttemptFailure(RetryContext, Throwable)}.
     *
     * @param remainingWriteRequirementsMet This argument, combined with
     * {@link SpecRetryPolicy#SpecRetryPolicy(Set, boolean, boolean, ExplicitMaxRetries, ServerDeprioritization) retryRequirementsMaybeMet},
     * specifies whether the write retry requirements are met.
     * <p>
     * Specifying {@code false}, or not calling this method at all, does not completely prevent retrying,
     * but affects logging and which failed results may be eligible for retry.
     * For example, {@link MongoConnectionPoolClearedException} may be eligible for retry regardless of this flag.
     * @return {@code this}.
     */
    SpecRetryPolicy onWriteRetryRequirements(final boolean remainingWriteRequirementsMet, final ConnectionDescription connectionDescription) {
        assertNull(writeRetryRequirementsMet);
        assertTrue(descriptors.contains(WRITE));
        writeRetryRequirementsMet = retryRequirementsMaybeMet && remainingWriteRequirementsMet;
        maxWireVersion = connectionDescription.getMaxWireVersion();
        return this;
    }

    private void resetWriteRetryRequirementsInfo() {
        maxWireVersion = null;
        writeRetryRequirementsMet = null;
    }

    @Override
    public Decision onAttemptFailure(final RetryContext retryContext, final Throwable attemptFailedResult) {
        serverDeprioritization.onAttemptFailure(attemptFailedResult);
        int attempt = retryContext.attempt();
        assertTrue(attempt < INFINITE_ATTEMPTS);
        assertTrue(attempt < maxAttempts);
        boolean retryableError;
        if (descriptors.contains(WRITE)) {
            retryableError = decideRetryableAndAddRetryableWriteErrorLabelIfNeeded(attemptFailedResult);
        } else if (descriptors.contains(READ)) {
            retryableError = isRetryableReadError(attemptFailedResult);
        } else {
            throw fail(descriptors.toString());
        }
        boolean maxAttemptsReached = attempt == maxAttempts - 1;
        if (retryRequirementsMaybeMet) {
            if (retryableError && maxAttemptsReached) {
                logUnableToRetryMaxAttemptsReached();
            } else if (!retryableError) {
                logUnableToRetryError(attemptFailedResult);
            }
        }
        boolean retry = retryableError && !maxAttemptsReached;
        Decision decision = new Decision(
                decideProspectiveFailedResult(retryContext.getProspectiveFailedResult().orElse(null), attemptFailedResult),
                retry ? new RetryAttemptInfo() : null);
        resetWriteRetryRequirementsInfo();
        return decision;
    }

    /**
     * Returns {@code true} iff another attempt must be executed;
     * in this case, also adds the {@value CommandOperationHelper#RETRYABLE_WRITE_ERROR_LABEL} label if needed.
     */
    private boolean decideRetryableAndAddRetryableWriteErrorLabelIfNeeded(final Throwable maybeInternalException) {
        Throwable exception = stripResourceSupplierInternalException(maybeInternalException);
        if (!(exception instanceof MongoException)) {
            return false;
        }
        MongoException mongoException = (MongoException) exception;
        boolean connectionPoolClearedException = mongoException instanceof MongoConnectionPoolClearedException;
        if (connectionPoolClearedException && effectiveRetrySetting) {
            // We would have retried regardless of the settings,
            // but we add `RETRYABLE_WRITE_ERROR_LABEL` only if retries are enabled via settings.
            mongoException.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        }
        boolean retryRegardlessOfRequirementsHavingBeenMet = connectionPoolClearedException || isRetryableMongoSecurityException(mongoException);
        boolean retry;
        if (TRUE.equals(writeRetryRequirementsMet)) {
            addRetryableWriteErrorLabelIfNeeded(mongoException, assertNotNull(maxWireVersion));
            retry = mongoException.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL);
        } else {
            retry = retryRegardlessOfRequirementsHavingBeenMet;
        }
        return retry;
    }

    private static Throwable stripResourceSupplierInternalException(final Throwable maybeInternal) {
        Throwable external;
        if (maybeInternal instanceof OperationHelper.ResourceSupplierInternalException) {
            external = maybeInternal.getCause();
        } else {
            external = maybeInternal;
        }
        return external;
    }

    private static boolean isRetryableMongoSecurityException(final MongoException exception) {
        return exception instanceof MongoSecurityException
                && exception.getCause() != null && isRetryableException(exception.getCause());
    }

    private static boolean isRetryableReadError(final Throwable exception) {
        assertFalse(exception instanceof OperationHelper.ResourceSupplierInternalException);
        if (!(exception instanceof MongoException)) {
            return false;
        }
        MongoException mongoException = (MongoException) exception;
        return isRetryableMongoSecurityException(mongoException) || isRetryableException(mongoException);
    }

    private void logUnableToRetryMaxAttemptsReached() {
        if (LOGGER.isDebugEnabled()) {
            String commandDescription = commandDescriptionSupplier.get();
            int maxRetries = ExplicitMaxRetries.maxRetries(maxAttempts);
            LOGGER.debug(commandDescription == null
                    ? format("Unable to retry a command due to reaching the max retry attempts limit of %d", maxRetries)
                    : format("Unable to retry the command '%s' due to reaching the max retry attempts limit of %d", commandDescription, maxRetries));
        }
    }

    private void logUnableToRetryError(final Throwable maybeInternalException) {
        if (LOGGER.isDebugEnabled()) {
            Throwable exception = stripResourceSupplierInternalException(maybeInternalException);
            String commandDescription = commandDescriptionSupplier.get();
            LOGGER.debug(commandDescription == null
                    ? format("Unable to retry a command due to the error \"%s\"", exception)
                    : format("Unable to retry the command '%s' due to the error \"%s\"", commandDescription, exception));
        }
    }

    private Throwable decideProspectiveFailedResult(
            @Nullable final Throwable currentProspectiveFailedResult, final Throwable mostRecentAttemptFailedResult) {
        Throwable newProspectiveFailedResult;
        if (descriptors.contains(WRITE)) {
            newProspectiveFailedResult = decideWriteProspectiveFailedResult(currentProspectiveFailedResult, mostRecentAttemptFailedResult);
        } else if (descriptors.contains(READ)) {
            newProspectiveFailedResult = decideReadProspectiveFailedResult(currentProspectiveFailedResult, mostRecentAttemptFailedResult);
        } else {
            throw fail(descriptors.toString());
        }
        if (mostRecentAttemptFailedResult instanceof MongoOperationTimeoutException) {
            newProspectiveFailedResult = createMongoTimeoutException(newProspectiveFailedResult);
        }
        return newProspectiveFailedResult;
    }

    private static Throwable decideWriteProspectiveFailedResult(
            @Nullable final Throwable currentProspectiveFailedResult, final Throwable mostRecentAttemptFailedResult) {
        if (currentProspectiveFailedResult == null) {
            return stripResourceSupplierInternalException(mostRecentAttemptFailedResult);
        } else if (mostRecentAttemptFailedResult instanceof OperationHelper.ResourceSupplierInternalException
                || (mostRecentAttemptFailedResult instanceof MongoException
                        && ((MongoException) mostRecentAttemptFailedResult).hasErrorLabel(NO_WRITES_PERFORMED_ERROR_LABEL))) {
            return currentProspectiveFailedResult;
        } else {
            return mostRecentAttemptFailedResult;
        }
    }

    private static Throwable decideReadProspectiveFailedResult(
            @Nullable final Throwable currentProspectiveFailedResult, final Throwable mostRecentAttemptFailedResult) {
        assertFalse(mostRecentAttemptFailedResult instanceof OperationHelper.ResourceSupplierInternalException);
        if (currentProspectiveFailedResult == null
                || mostRecentAttemptFailedResult instanceof MongoSocketException
                || mostRecentAttemptFailedResult instanceof MongoServerException) {
            return mostRecentAttemptFailedResult;
        } else {
            return currentProspectiveFailedResult;
        }
    }

    @Override
    public String toString() {
        return "SpecRetryPolicy{"
                + "descriptors=" + descriptors
                + ", effectiveRetrySetting=" + effectiveRetrySetting
                + ", retryRequirementsMaybeMet=" + retryRequirementsMaybeMet
                + ", explicitMaxRetries=" + explicitMaxRetries
                + ", maxAttempts=" + maxAttempts
                + ", serverDeprioritization=" + serverDeprioritization
                + ", commandDescription=" + commandDescriptionSupplier.get()
                + ", writeRetryRequirementsMet=" + writeRetryRequirementsMet
                + ", maxWireVersion=" + maxWireVersion
                + '}';
    }

    enum Descriptor {
        /**
         * See <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.md">Retryable Writes</a>.
         */
        WRITE,
        /**
         * See <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.md">Retryable Reads</a>.
         */
        READ;

        private static final EnumMap<Descriptor, EnumSet<Descriptor>> CONFLICTS;

        static {
            CONFLICTS = new EnumMap<>(Descriptor.class);
            CONFLICTS.put(WRITE, EnumSet.of(READ));
            CONFLICTS.put(READ, EnumSet.of(WRITE));
        }

        static Set<Descriptor> assertNoConflicts(final Set<Descriptor> descriptors) {
            for (Descriptor descriptor : descriptors) {
                for (Descriptor conflictingDescriptor : CONFLICTS.get(descriptor)) {
                    assertFalse(descriptors.contains(conflictingDescriptor));
                }
            }
            return descriptors;
        }
    }

    enum ExplicitMaxRetries {
        NO_RETRIES_LIMIT,
        /**
         * See {@link Descriptor}.
         */
        RETRIES_LIMITED_BY_DESCRIPTORS,
        NO_RETRIES;

        private int maxAttempts(final Set<Descriptor> descriptors) {
            switch (this) {
                case NO_RETRIES_LIMIT: {
                    return INFINITE_ATTEMPTS;
                }
                case RETRIES_LIMITED_BY_DESCRIPTORS: {
                    return maxAttempts(maxRetries(descriptors));
                }
                case NO_RETRIES: {
                    return maxAttempts(0);
                }
                default: {
                    throw fail(this.toString());
                }
            }
        }

        private static int maxRetries(final Set<Descriptor> descriptors) {
            if (descriptors.contains(WRITE) || descriptors.contains(READ)) {
                return 1;
            } else {
                throw fail(descriptors.toString());
            }
        }

        private static int maxAttempts(final int maxRetries) {
            assertTrue(maxRetries < INFINITE_ATTEMPTS - 1);
            return maxRetries + 1;
        }

        static int maxRetries(final int maxAttempts) {
            assertTrue(maxAttempts > 0);
            return maxAttempts - 1;
        }
    }
}
