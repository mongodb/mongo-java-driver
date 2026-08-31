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
import com.mongodb.internal.async.function.RetryControl;
import com.mongodb.internal.async.function.RetryPolicy;
import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.OperationContext.ServerDeprioritization;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.time.ExponentialBackoff;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.mongodb.MongoException.RETRYABLE_ERROR_LABEL;
import static com.mongodb.MongoException.SYSTEM_OVERLOADED_ERROR_LABEL;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.TimeoutContext.createMongoTimeoutException;
import static com.mongodb.internal.operation.CommandOperationHelper.DEFAULT_MAX_ADAPTIVE_RETRIES;
import static com.mongodb.internal.operation.CommandOperationHelper.NO_WRITES_PERFORMED_ERROR_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.RETRYABLE_WRITE_ERROR_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabelIfNeeded;
import static com.mongodb.internal.operation.CommandOperationHelper.isRetryableException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.isReadRetryRequirementsMet;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Implements all {@linkplain IndividualPolicies individual specification retry policies}.
 * A new instance of {@link SpecRetryPolicy} must be used for each execution of a command
 * (a command execution may involve multiple execution attempts).
 */
final class SpecRetryPolicy implements RetryPolicy {
    private static final int INFINITE_ATTEMPTS = Integer.MAX_VALUE;

    private final IndividualPolicies policies;
    private int maxAttempts;
    private final ServerDeprioritization serverDeprioritization;
    private Supplier<String> commandDescriptionSupplier;

    /**
     * @param policies The included individual specification retry policies.
     */
    SpecRetryPolicy(
            final IndividualPolicies policies,
            final ExplicitMaxRetries explicitMaxRetries,
            final ServerDeprioritization serverDeprioritization) {
        this.policies = policies.assertValid();
        this.maxAttempts = explicitMaxRetries.maxAttempts(policies);
        this.serverDeprioritization = serverDeprioritization;
        commandDescriptionSupplier = () -> null;
    }

    void onAttemptStart(final RetryContext retryContext, final OperationContext operationContext) {
        policies.overload().ifPresent(state ->
                state.onAttemptStart(retryContext, operationContext.getSessionContext().getOverloadRetryPolicyState()));
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
     * <p>
     * This method may be called only if the {@linkplain IndividualPolicies#includeWrite() write retry policy is incluided}.
     *
     * @param remainingWriteRequirementsMet This argument, combined with the information passed to
     * {@link IndividualPolicies#IndividualPolicies(boolean)} and {@link IndividualPolicies#includeWrite()}, specifies whether the write retry requirements are met.
     * <p>
     * Specifying {@code false}, or not calling this method at all, does not completely prevent retrying,
     * but affects logging and which failed results may be eligible for retry.
     * For example, {@link MongoConnectionPoolClearedException} may be eligible for retry regardless of this flag.
     * @return {@code this}.
     */
    SpecRetryPolicy onWriteRetryRequirements(final boolean remainingWriteRequirementsMet, final ConnectionDescription connectionDescription) {
        return policies.write().map(state -> {
            state.onRequirements(remainingWriteRequirementsMet, connectionDescription);
            return this;
        }).orElseThrow(() -> fail());
    }

    @Override
    public void onAttemptFatalFailure() {
        policies.overload().ifPresent(IndividualPolicies.State.Overload::onAttemptFatalFailure);
    }

    @Override
    public Decision onAttemptFailure(final RetryContext retryContext, final Throwable maybeInternalAttemptFailedResult) {
        Throwable attemptFailedResult = stripResourceSupplierInternalException(maybeInternalAttemptFailedResult);
        serverDeprioritization.onAttemptFailure(attemptFailedResult);
        int attempt = retryContext.attempt();
        assertTrue(attempt < INFINITE_ATTEMPTS);
        assertTrue(attempt < maxAttempts);
        boolean retryableError = false;
        retryableError |= policies.write().map(state ->
            decideRetryableAndAddRetryableWriteErrorLabelIfNeeded(state, attemptFailedResult)).orElse(false);
        retryableError |= policies.read().map(state ->
            decideRetryable(state, attemptFailedResult)).orElse(false);
        retryableError |= policies.overload().map(state ->
            decideRetryableAndUpdateMaxAttempts(state, attemptFailedResult)).orElse(false);
        boolean maxAttemptsReached = attempt >= maxAttempts - 1;
        if (policies.isRetrySettingEffectivelyTrue() && !retryableError) {
            logUnableToRetryError(attemptFailedResult);
        }
        boolean retry = retryableError && !maxAttemptsReached;
        Decision decision = new Decision(
                decideProspectiveFailedResult(retryContext.getProspectiveFailedResult().orElse(null), maybeInternalAttemptFailedResult),
                retry ? new RetryAttemptInfo(calculateOverloadBackoff(attemptFailedResult, attempt + 1)) : null);
        policies.write().ifPresent(IndividualPolicies.State.Write::resetRequirementsInfo);
        return decision;
    }

    void onLastAttemptCompletion() {
        policies.overload().ifPresent(state -> state.onLastAttemptCompletion());
    }

    private static Throwable stripResourceSupplierInternalException(final Throwable maybeInternal) {
        if (maybeInternal instanceof OperationHelper.ResourceSupplierInternalException) {
            return maybeInternal.getCause();
        }
        return maybeInternal;
    }

    /**
     * Returns {@code true} iff another attempt must be executed provided that {@link #maxAttempts} has not been reached;
     * in this case, also adds the {@value CommandOperationHelper#RETRYABLE_WRITE_ERROR_LABEL} label if needed.
     */
    private boolean decideRetryableAndAddRetryableWriteErrorLabelIfNeeded(final IndividualPolicies.State.Write state, final Throwable exception) {
        assertFalse(exception instanceof OperationHelper.ResourceSupplierInternalException);
        if (!(exception instanceof MongoException)) {
            return false;
        }
        MongoException mongoException = (MongoException) exception;
        boolean retryableConnectionPoolClearedException = mongoException instanceof MongoConnectionPoolClearedException
                && policies.isRetrySettingEffectivelyTrue();
        if (retryableConnectionPoolClearedException) {
            mongoException.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        }
        boolean retryRegardlessOfTheRestOfRequirementsBeingMet = retryableConnectionPoolClearedException
                || (policies.isRetrySettingEffectivelyTrue() && isRetryableMongoSecurityException(mongoException));
        boolean retry;
        if (state.isRequirementsMet()) {
            addRetryableWriteErrorLabelIfNeeded(mongoException, state.getMaxWireVersion());
            retry = mongoException.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL);
        } else {
            retry = retryRegardlessOfTheRestOfRequirementsBeingMet;
        }
        return retry;
    }

    private static boolean isRetryableMongoSecurityException(final MongoException exception) {
        return exception instanceof MongoSecurityException
                && exception.getCause() != null && isRetryableException(exception.getCause());
    }

    /**
     * Returns {@code true} iff another attempt must be executed provided that {@link #maxAttempts} has not been reached.
     */
    private static boolean decideRetryable(final IndividualPolicies.State.Read state, final Throwable exception) {
        assertFalse(exception instanceof OperationHelper.ResourceSupplierInternalException);
        if (!state.isRequirementsMet() || !(exception instanceof MongoException)) {
            return false;
        }
        MongoException mongoException = (MongoException) exception;
        return isRetryableMongoSecurityException(mongoException) || isRetryableException(mongoException);
    }

    /**
     * Returns {@code true} iff another attempt must be executed provided that {@link #maxAttempts} has not been reached.
     */
    private boolean decideRetryableAndUpdateMaxAttempts(final IndividualPolicies.State.Overload state, final Throwable exception) {
        IndividualPolicies.State.Overload.LabelInfo overloadLabelInfo = state.onAttemptFailure(exception);
        if (overloadLabelInfo.hasSystemOverloadErrorLabel() && policies.isRetrySettingEffectivelyTrue()) {
            maxAttempts = maxAttempts(state.getMaxRetries());
        }
        return state.isRequirementsMet() && overloadLabelInfo.hasRetryableErrorLabel() && overloadLabelInfo.hasSystemOverloadErrorLabel();
    }

    private void logUnableToRetryError(final Throwable exception) {
        assertFalse(exception instanceof OperationHelper.ResourceSupplierInternalException);
        if (LOGGER.isDebugEnabled()) {
            String commandDescription = commandDescriptionSupplier.get();
            LOGGER.debug(commandDescription == null
                    ? format("Unable to retry a command due to the error \"%s\"", exception)
                    : format("Unable to retry the command '%s' due to the error \"%s\"", commandDescription, exception));
        }
    }

    private Throwable decideProspectiveFailedResult(
            @Nullable final Throwable currentProspectiveFailedResult, final Throwable mostRecentAttemptFailedResult) {
        Throwable newProspectiveFailedResult;
        boolean writePolicyErrorPropagation = policies.write().isPresent()
                || policies.overload().map(overload -> overload.errorPropagation() == ErrorPropagation.AS_WRITE_POLICY).orElse(false);
        boolean readPolicyErrorPropagation = policies.read().isPresent()
                || policies.overload().map(overload -> overload.errorPropagation() == ErrorPropagation.AS_READ_POLICY).orElse(false);
        if (writePolicyErrorPropagation) {
            newProspectiveFailedResult = decideWriteProspectiveFailedResult(currentProspectiveFailedResult, mostRecentAttemptFailedResult);
        } else if (readPolicyErrorPropagation) {
            newProspectiveFailedResult = decideReadProspectiveFailedResult(currentProspectiveFailedResult, mostRecentAttemptFailedResult);
        } else {
            throw fail(toString());
        }
        if (mostRecentAttemptFailedResult instanceof MongoOperationTimeoutException) {
            newProspectiveFailedResult = createMongoTimeoutException(newProspectiveFailedResult);
        }
        return newProspectiveFailedResult;
    }

    private static Throwable decideWriteProspectiveFailedResult(
            @Nullable final Throwable currentProspectiveFailedResult, final Throwable maybeInternalMostRecentAttemptFailedResult) {
        if (currentProspectiveFailedResult == null) {
            return stripResourceSupplierInternalException(maybeInternalMostRecentAttemptFailedResult);
        } else if (maybeInternalMostRecentAttemptFailedResult instanceof OperationHelper.ResourceSupplierInternalException
                || (maybeInternalMostRecentAttemptFailedResult instanceof MongoException
                        && ((MongoException) maybeInternalMostRecentAttemptFailedResult).hasErrorLabel(NO_WRITES_PERFORMED_ERROR_LABEL))) {
            return currentProspectiveFailedResult;
        } else {
            return maybeInternalMostRecentAttemptFailedResult;
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

    private static Duration calculateOverloadBackoff(final Throwable attemptFailedResult, final int immediateNextAttempt) {
        assertFalse(attemptFailedResult instanceof OperationHelper.ResourceSupplierInternalException);
        if (attemptFailedResult instanceof MongoException
                && ((MongoException) attemptFailedResult).hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL)) {
            return ExponentialBackoff.calculateOverloadBackoff(immediateNextAttempt);
        }
        return Duration.ZERO;
    }

    private static int maxAttempts(final int maxRetries) {
        if (maxRetries == INFINITE_ATTEMPTS) {
            return INFINITE_ATTEMPTS;
        }
        return maxRetries + 1;
    }

    @Override
    public String toString() {
        return "SpecRetryPolicy{"
                + "policies=" + policies
                + ", maxAttempts=" + maxAttempts
                + ", serverDeprioritization=" + serverDeprioritization
                + ", commandDescription=" + commandDescriptionSupplier.get()
                + '}';
    }

    static final class IndividualPolicies {
        private static final EnumMap<Descriptor, EnumSet<Descriptor>> CONFLICTS;

        private final boolean effectiveRetrySetting;
        private final EnumMap<Descriptor, State> policies;

        static {
            CONFLICTS = new EnumMap<>(Descriptor.class);
            CONFLICTS.put(Descriptor.WRITE, EnumSet.of(Descriptor.READ));
            CONFLICTS.put(Descriptor.READ, EnumSet.of(Descriptor.WRITE));
            CONFLICTS.put(Descriptor.OVERLOAD, EnumSet.noneOf(Descriptor.class));
            assertTrue(CONFLICTS.keySet().containsAll(asList(Descriptor.values())));
        }

        /**
         * @param effectiveRetrySetting See {@link MongoClientSettings#getRetryWrites()}, {@link MongoClientSettings#getRetryReads()}.
         * Note that for some commands, like {@code commitTransaction}/{@code abortTransaction},
         * retries are deemed to be enabled regardless of the settings; this argument must be {@code true} for them.
         */
        IndividualPolicies(final boolean effectiveRetrySetting) {
            this.effectiveRetrySetting = effectiveRetrySetting;
            this.policies = new EnumMap<>(Descriptor.class);
        }

        private IndividualPolicies assertValid() {
            assertFalse(policies.isEmpty());
            assertNoConflicts(policies.keySet());
            overload().ifPresent(overload -> {
                if (overload.errorPropagation() != null) {
                    // Overload-only composition: no other policy may be included.
                    assertTrue(policies.size() == 1);
                } else {
                    assertTrue(policies.containsKey(Descriptor.READ) || policies.containsKey(Descriptor.WRITE));
                }
            });
            return this;
        }

        private static void assertNoConflicts(final Set<Descriptor> descriptors) {
            for (Descriptor descriptor : descriptors) {
                for (Descriptor conflictingDescriptor : assertNotNull(CONFLICTS.get(descriptor))) {
                    assertFalse(descriptors.contains(conflictingDescriptor));
                }
            }
        }

        private boolean isRetrySettingEffectivelyTrue() {
            return effectiveRetrySetting;
        }

        /**
         * See
         * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/retryable-writes.md">Retryable Writes</a>,
         * which specifies the write retry policy.
         */
        IndividualPolicies includeWrite() {
            include(Descriptor.WRITE, new State.Write(effectiveRetrySetting));
            return this;
        }

        /**
         * See
         * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/retryable-reads.md">Retryable Reads</a>,
         * which specifies the read retry policy.
         */
        IndividualPolicies includeRead(final OperationContext operationContext) {
            include(Descriptor.READ, new State.Read(effectiveRetrySetting, operationContext));
            return this;
        }

        /**
         * See
         * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/client-backpressure.md">Client Backpressure</a>,
         * which specifies the overload retry policy.
         * <p>
         * Use this overload when {@link #includeRead(OperationContext)} or {@link #includeWrite()} is also included;
         */
        IndividualPolicies includeOverload(@Nullable final Integer maxAdaptiveRetriesSetting) {
            include(Descriptor.OVERLOAD, new State.Overload(effectiveRetrySetting, maxAdaptiveRetriesSetting, null, false));
            return this;
        }

        /**
         * See {@link #includeOverload(Integer)}.
         * <p>
         * Use this overload only for overload-only compositions (no read or write policy included).
         *
         * @param errorPropagation selects the error propagation shape applied by
         *                         {@link SpecRetryPolicy#decideProspectiveFailedResult}.
         */
        IndividualPolicies includeOverload(@Nullable final Integer maxAdaptiveRetriesSetting,
                                           final ErrorPropagation errorPropagation) {
            return includeOverload(maxAdaptiveRetriesSetting, errorPropagation, false);
        }

        /**
         * See {@link #includeOverload(Integer, ErrorPropagation)}.
         *
         * @param sessionScopeUpdatesSuspended when {@code true}, this policy will not open, mutate, or close the
         *                                     session's
         *                                     {@link BaseClientSessionImpl.OverloadRetryPolicyState.CommandExecutionScoped command execution scope}.
         *                                     Set to {@code true} only for child retry controls executed inside
         *                                     another retry supplier's execution - for example, the cursor
         *                                     {@code getMore} retry in {@link CommandCursor} / {@link AsyncCommandCursor}
         *                                     invoked while iterating a bulk-write cursor inside
         *                                     {@link RetryControl#doWhileDisabled(Supplier)}
         *                                     where the parent {@link ClientBulkWriteOperation} attempt's scope is
         *                                     still open.
         */
        IndividualPolicies includeOverload(@Nullable final Integer maxAdaptiveRetriesSetting,
                                           final ErrorPropagation errorPropagation,
                                           final boolean sessionScopeUpdatesSuspended) {
            include(Descriptor.OVERLOAD, new State.Overload(
                    effectiveRetrySetting, maxAdaptiveRetriesSetting, errorPropagation, sessionScopeUpdatesSuspended));
            return this;
        }

        private void include(final Descriptor descriptor, final State state) {
            State previous = policies.put(descriptor, state);
            assertNull(previous);
        }

        private int getMaxRetries() {
            if (policies.containsKey(Descriptor.WRITE)) {
                return Descriptor.WRITE.maxRetries;
            } else if (policies.containsKey(Descriptor.READ)) {
                return Descriptor.READ.maxRetries;
            } else if (policies.containsKey(Descriptor.OVERLOAD)) {
                return overload().orElseThrow(Assertions::fail).getMaxRetries();
            } else {
                throw fail(toString());
            }
        }

        private Optional<State.Write> write() {
            return Optional.ofNullable((State.Write) policies.get(Descriptor.WRITE));
        }

        private Optional<State.Read> read() {
            return Optional.ofNullable((State.Read) policies.get(Descriptor.READ));
        }

        private Optional<State.Overload> overload() {
            return Optional.ofNullable((State.Overload) policies.get(Descriptor.OVERLOAD));
        }

        @Override
        public String toString() {
            return "IndividualPolicies{"
                    + "effectiveRetrySetting=" + effectiveRetrySetting
                    + ", policies=" + policies
                    + '}';
        }

        private enum Descriptor {
            /**
             * See {@link #includeWrite()}.
             */
            WRITE(1),
            /**
             * See {@link #includeRead(OperationContext)}.
             */
            READ(1),
            /**
             * See {@link #includeOverload(Integer)}.
             */
            OVERLOAD(DEFAULT_MAX_ADAPTIVE_RETRIES);

            private final int maxRetries;

            Descriptor(final int maxRetries) {
                this.maxRetries = maxRetries;
            }
        }

        /**
         * The state specific to an individual retry policy.
         */
        private abstract static class State {
            private State() {
            }

            static final class Write extends State {
                private final boolean requirementsMaybeMet;
                @Nullable
                private Boolean requirementsMet;
                @Nullable
                private Integer maxWireVersion;

                Write(final boolean effectiveRetryWritesSetting) {
                    this.requirementsMaybeMet = effectiveRetryWritesSetting;
                }

                /**
                 * @see #isRequirementsMet()
                 * @see #resetRequirementsInfo()
                 * @see SpecRetryPolicy#onWriteRetryRequirements(boolean, ConnectionDescription)
                 */
                void onRequirements(
                        final boolean remainingRequirementsMet,
                        final ConnectionDescription connectionDescription) {
                    assertNull(requirementsMet);
                    requirementsMet = requirementsMaybeMet && remainingRequirementsMet;
                    if (requirementsMet) {
                        maxWireVersion = connectionDescription.getMaxWireVersion();
                    }
                }

                /**
                 * @see #onRequirements(boolean, ConnectionDescription)
                 * @see SpecRetryPolicy#onWriteRetryRequirements(boolean, ConnectionDescription)
                 */
                void resetRequirementsInfo() {
                    maxWireVersion = null;
                    requirementsMet = null;
                }

                /**
                 * See {@link Read#isRequirementsMet()}.
                 *
                 * @see #onRequirements(boolean, ConnectionDescription)
                 */
                boolean isRequirementsMet() {
                    return TRUE.equals(requirementsMet);
                }

                /**
                 * May be called only if {@link #isRequirementsMet()}.
                 */
                int getMaxWireVersion() {
                    return assertNotNull(maxWireVersion);
                }

                @Override
                public String toString() {
                    return "Write{"
                            + "requirementsMaybeMet=" + requirementsMaybeMet
                            + ", requirementsMet=" + requirementsMet
                            + ", maxWireVersion=" + maxWireVersion
                            + '}';
                }
            }

            static final class Read extends State {
                private final boolean requirementsMet;

                Read(final boolean effectiveRetryReadsSetting, final OperationContext operationContext) {
                    requirementsMet = isReadRetryRequirementsMet(effectiveRetryReadsSetting, operationContext);
                }

                /**
                 * The requirements in question include settings requirements, server requirements, command requirements, etc.,
                 * but exclude error requirements, {@link #maxAttempts} requirements.
                 */
                boolean isRequirementsMet() {
                    return requirementsMet;
                }

                @Override
                public String toString() {
                    return "Read{"
                            + "requirementsMet=" + requirementsMet
                            + '}';
                }
            }

            static final class Overload extends State {
                private final boolean requirementsMet;
                @Nullable
                private final Integer maxAdaptiveRetriesSetting;
                @Nullable
                private final ErrorPropagation errorPropagation;
                /**
                 * When {@code true}, this policy does not open, mutate, or close the session's
                 * {@link BaseClientSessionImpl.OverloadRetryPolicyState.CommandExecutionScoped command execution scope}.
                 * Set for child retry controls executed inside another retry supplier's execution — for example, the
                 * cursor {@code getMore} retry loop in {@link CommandCursor} / {@link AsyncCommandCursor} invoked
                 * while iterating a bulk-write cursor inside
                 * {@link com.mongodb.internal.async.function.RetryControl#doWhileDisabled(java.util.function.Supplier)}
                 * where the parent {@link ClientBulkWriteOperation} attempt still holds an open command execution
                 * scope on the session.
                 */
                private final boolean sessionScopeUpdatesSuspended;
                @Nullable
                private BaseClientSessionImpl.OverloadRetryPolicyState sessionScopedState;

                Overload(
                        final boolean effectiveRetrySetting,
                        @Nullable
                        final Integer maxAdaptiveRetriesSetting,
                        @Nullable
                        final ErrorPropagation errorPropagation,
                        final boolean sessionScopeUpdatesSuspended) {
                    requirementsMet = effectiveRetrySetting;
                    this.maxAdaptiveRetriesSetting = maxAdaptiveRetriesSetting;
                    this.errorPropagation = errorPropagation;
                    this.sessionScopeUpdatesSuspended = sessionScopeUpdatesSuspended;
                    sessionScopedState = null;
                }

                @Nullable
                ErrorPropagation errorPropagation() {
                    return errorPropagation;
                }

                int getMaxRetries() {
                    return maxAdaptiveRetriesSetting == null
                            ? IndividualPolicies.Descriptor.OVERLOAD.maxRetries
                            : maxAdaptiveRetriesSetting;
                }

                void onAttemptStart(final RetryContext retryContext, final BaseClientSessionImpl.OverloadRetryPolicyState sessionScopedState) {
                    this.sessionScopedState = sessionScopedState;
                    if (retryContext.isFirstAttempt() && !sessionScopeUpdatesSuspended) {
                        sessionScopedState.openCommandExecutionScope();
                    }
                }

                void onAttemptFatalFailure() {
                    onAnyAttemptFailure(false);
                }

                LabelInfo onAttemptFailure(final Throwable exception) {
                    LabelInfo labelInfo = new LabelInfo(exception);
                    onAnyAttemptFailure(labelInfo.hasRetryableErrorLabel() && labelInfo.hasSystemOverloadErrorLabel());
                    return labelInfo;
                }

                private void onAnyAttemptFailure(final boolean retryableOverloadError) {
                    if (sessionScopeUpdatesSuspended) {
                        return;
                    }
                    BaseClientSessionImpl.OverloadRetryPolicyState localSessionScopedState = assertNotNull(sessionScopedState);
                    assertNotNull(localSessionScopedState.getCommandExecutionScoped()).onAnyAttemptFailure(retryableOverloadError);
                    BaseClientSessionImpl.OverloadRetryPolicyState.CommitScoped commitScopedState = localSessionScopedState.getCommitScoped();
                    if (commitScopedState != null) {
                        commitScopedState.onAnyAttemptFailure(retryableOverloadError);
                    }
                }

                /**
                 * See {@link Read#isRequirementsMet()}.
                 */
                boolean isRequirementsMet() {
                    return requirementsMet;
                }

                void onLastAttemptCompletion() {
                    if (!sessionScopeUpdatesSuspended) {
                        assertNotNull(sessionScopedState).closeCommandExecutionScope();
                    }
                }

                @Override
                public String toString() {
                    return "Overload{"
                            + "requirementsMet=" + requirementsMet
                            + ", maxAdaptiveRetriesSetting=" + maxAdaptiveRetriesSetting
                            + ", errorPropagation=" + errorPropagation
                            + ", sessionScopedState=" + sessionScopedState
                            + '}';
                }

                static final class LabelInfo {
                    private final boolean hasRetryableErrorLabel;
                    private final boolean hasSystemOverloadErrorLabel;

                    private LabelInfo(final Throwable exception) {
                        if (exception instanceof MongoException) {
                            MongoException mongoException = (MongoException) exception;
                            hasRetryableErrorLabel = mongoException.hasErrorLabel(RETRYABLE_ERROR_LABEL);
                            hasSystemOverloadErrorLabel = mongoException.hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL);
                        } else {
                            hasRetryableErrorLabel = false;
                            hasSystemOverloadErrorLabel = false;
                        }
                    }

                    boolean hasRetryableErrorLabel() {
                        return hasRetryableErrorLabel;
                    }

                    boolean hasSystemOverloadErrorLabel() {
                        return hasSystemOverloadErrorLabel;
                    }

                    @Override
                    public String toString() {
                        return "LabelInfo{"
                                + "hasRetryableErrorLabel=" + hasRetryableErrorLabel
                                + ", hasSystemOverloadErrorLabel=" + hasSystemOverloadErrorLabel
                                + '}';
                    }
                }
            }
        }
    }

    enum ExplicitMaxRetries {
        NO_RETRIES_LIMIT,
        /**
         * See {@link IndividualPolicies}.
         */
        RETRIES_LIMITED_BY_INDIVIDUAL_POLICIES;

        private int maxAttempts(final IndividualPolicies policies) {
            switch (this) {
                case NO_RETRIES_LIMIT: {
                    return INFINITE_ATTEMPTS;
                }
                case RETRIES_LIMITED_BY_INDIVIDUAL_POLICIES: {
                    return SpecRetryPolicy.maxAttempts(policies.getMaxRetries());
                }
                default: {
                    throw fail(toString());
                }
            }
        }
    }

    /**
     * Selects the error propagation shape for overload-only policy
     * compositions (see {@link IndividualPolicies#includeOverload(Integer, ErrorPropagation)}).
     */
    enum ErrorPropagation {
        AS_READ_POLICY,
        AS_WRITE_POLICY
    }
}
