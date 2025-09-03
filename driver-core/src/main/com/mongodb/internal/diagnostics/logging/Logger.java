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

package com.mongodb.internal.diagnostics.logging;

/**
 * This class is not part of the public API.  It may be removed or changed at any time.
 *
 */
public interface Logger {
    /**
     * Return the name of this <code>Logger</code> instance.
     *
     * @return name of this logger instance
     */
    String getName();

    /**
     * Is the logger instance enabled for the TRACE level?
     *
     * @return True if this Logger is enabled for the TRACE level, false otherwise.
     * @since 1.4
     */
    default boolean isTraceEnabled() {
        return false;
    }

    /**
     * Log a message at the TRACE level.
     *
     * @param msg the message string to be logged
     * @since 1.4
     */
    default void trace(String msg) {
    }

    /**
     * Log an exception (throwable) at the TRACE level with an accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     * @since 1.4
     */
    default void trace(String msg, Throwable t) {
    }

    /**
     * Is the logger instance enabled for the DEBUG level?
     *
     * @return True if this Logger is enabled for the DEBUG level, false otherwise.
     */
    default boolean isDebugEnabled() {
        return false;
    }

    /**
     * Log a message at the DEBUG level.
     *
     * @param msg the message string to be logged
     */
    default void debug(String msg) {
    }


    /**
     * Log an exception (throwable) at the DEBUG level with an accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    default void debug(String msg, Throwable t) {
    }

    /**
     * Is the logger instance enabled for the INFO level?
     *
     * @return True if this Logger is enabled for the INFO level, false otherwise.
     */
    default boolean isInfoEnabled() {
        return false;
    }

    /**
     * Log a message at the INFO level.
     *
     * @param msg the message string to be logged
     */
    default void info(String msg) {
    }

    /**
     * Log an exception (throwable) at the INFO level with an accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    default void info(String msg, Throwable t) {
    }

    /**
     * Is the logger instance enabled for the WARN level?
     *
     * @return True if this Logger is enabled for the WARN level, false otherwise.
     */
    default boolean isWarnEnabled() {
        return false;
    }

    /**
     * Log a message at the WARN level.
     *
     * @param msg the message string to be logged
     */
    default void warn(String msg) {
    }

    /**
     * Log an exception (throwable) at the WARN level with an accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    default void warn(String msg, Throwable t) {
    }

    /**
     * Is the logger instance enabled for the ERROR level?
     *
     * @return True if this Logger is enabled for the ERROR level, false otherwise.
     */
    default boolean isErrorEnabled() {
        return false;
    }

    /**
     * Log a message at the ERROR level.
     *
     * @param msg the message string to be logged
     */
    default void error(String msg) {
    }

    /**
     * Log an exception (throwable) at the ERROR level with an accompanying message.
     *
     * @param msg the message accompanying the exception
     * @param t   the exception (throwable) to log
     */
    default void error(String msg, Throwable t) {
    }
}
