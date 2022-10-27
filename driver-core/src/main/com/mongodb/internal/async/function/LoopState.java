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
package com.mongodb.internal.async.function;

import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * Represents both the state associated with a loop and a handle that can be used to affect looping, e.g.,
 * to {@linkplain #breakAndCompleteIf(Supplier, SingleResultCallback) break} it.
 * {@linkplain #attachment(AttachmentKey) Attachments} may be used by the associated loop
 * to preserve a state between iterations.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @see AsyncCallbackLoop
 */
@NotThreadSafe
public final class LoopState {
    private int iteration;
    private boolean lastIteration;
    @Nullable
    private Map<AttachmentKey<?>, AttachmentValueContainer> attachments;

    public LoopState() {
        iteration = 0;
    }

    /**
     * Advances this {@link LoopState} such that it represents the state of a new iteration.
     * Must not be called before the {@linkplain #isFirstIteration() first iteration}, must be called before each subsequent iteration.
     *
     * @return {@code true} if the next iteration must be executed; {@code false} iff the loop was {@link #isLastIteration() broken}.
     */
    boolean advance() {
        if (lastIteration) {
            return false;
        } else {
            iteration++;
            removeAutoRemovableAttachments();
            return true;
        }
    }

    /**
     * Returns {@code true} iff the current iteration is the first one.
     *
     * @see #iteration()
     */
    public boolean isFirstIteration() {
        return iteration == 0;
    }

    /**
     * Returns {@code true} iff {@link #breakAndCompleteIf(Supplier, SingleResultCallback)} / {@link #markAsLastIteration()} was called.
     */
    boolean isLastIteration() {
        return lastIteration;
    }

    /**
     * A 0-based iteration number.
     */
    public int iteration() {
        return iteration;
    }

    /**
     * This method emulates executing the <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html#jls-14.15">
     * {@code break}</a> statement. Must not be called more than once per {@link LoopState}.
     *
     * @param predicate {@code true} iff the associated loop needs to be broken.
     * @return {@code true} iff the {@code callback} was completed, which happens iff any of the following is true:
     * <ul>
     *     <li>the {@code predicate} completed abruptly, in which case the exception thrown is relayed to the {@code callback};</li>
     *     <li>this method broke the associated loop.</li>
     * </ul>
     * If {@code true} is returned, the caller must complete the ongoing attempt.
     * @see #isLastIteration()
     */
    public boolean breakAndCompleteIf(final Supplier<Boolean> predicate, final SingleResultCallback<?> callback) {
        assertFalse(lastIteration);
        try {
            lastIteration = predicate.get();
        } catch (Throwable t) {
            callback.onResult(null, t);
            return true;
        }
        if (lastIteration) {
            callback.onResult(null, null);
            return true;
        } else {
            return false;
        }
    }

    /**
     * This method is similar to {@link #breakAndCompleteIf(Supplier, SingleResultCallback)}.
     * The difference is that it allows the current iteration to continue, yet no more iterations will happen.
     *
     * @see #isLastIteration()
     */
    void markAsLastIteration() {
        assertFalse(lastIteration);
        lastIteration = true;
    }

    /**
     * The associated loop may use this method to preserve a state between iterations.
     *
     * @param autoRemove Specifies whether the attachment must be automatically removed before (in the happens-before order) the next
     * {@linkplain #iteration() iteration} as if this removal were the very first action of the iteration.
     * Note that there is no guarantee that the attachment is removed after the {@linkplain #isLastIteration() last iteration}.
     * @return {@code this}.
     * @see #attachment(AttachmentKey)
     */
    public <V> LoopState attach(final AttachmentKey<V> key, final V value, final boolean autoRemove) {
        attachments().put(assertNotNull(key), new AttachmentValueContainer(assertNotNull(value), autoRemove));
        return this;
    }

    /**
     * @see #attach(AttachmentKey, Object, boolean)
     */
    public <V> Optional<V> attachment(final AttachmentKey<V> key) {
        final AttachmentValueContainer valueContainer = attachments().get(assertNotNull(key));
        @SuppressWarnings("unchecked")
        final V value = valueContainer == null ? null : (V) valueContainer.value();
        return Optional.ofNullable(value);
    }

    private Map<AttachmentKey<?>, AttachmentValueContainer> attachments() {
        if (attachments == null) {
            attachments = new HashMap<>();
        }
        return attachments;
    }

    private void removeAutoRemovableAttachments() {
        if (attachments == null) {
            return;
        }
        attachments.entrySet().removeIf(entry -> entry.getValue().autoRemove());
    }

    @Override
    public String toString() {
        return "LoopState{"
                + "iteration=" + iteration
                + ", attachments=" + attachments
                + '}';
    }

    /**
     * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a>
     * identifier of an attachment.
     *
     * @param <V> The type of the corresponding attachment value.
     */
    @Immutable
    // the type parameter V is of the essence even though it is not used in the interface itself
    @SuppressWarnings("unused")
    public interface AttachmentKey<V> {
    }

    private static final class AttachmentValueContainer {
        @Nullable
        private final Object value;
        private final boolean autoRemove;

        AttachmentValueContainer(final Object value, final boolean autoRemove) {
            this.value = value;
            this.autoRemove = autoRemove;
        }

        @Nullable
        Object value() {
            return value;
        }

        boolean autoRemove() {
            return autoRemove;
        }

        @Override
        public String toString() {
            return "AttachmentValueContainer{"
                    + "value=" + value
                    + ", autoRemove=" + autoRemove
                    + '}';
        }
    }
}
