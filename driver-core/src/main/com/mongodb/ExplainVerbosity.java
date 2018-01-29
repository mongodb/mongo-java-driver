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

/**
 * An enumeration of the verbosity levels available for explaining query execution.
 *
 * @since 3.0
 * @mongodb.server.release 3.0
 */
public enum ExplainVerbosity {
    /**
     * Runs the query planner and chooses the winning plan, but does not actually execute it. The use case for this verbosity level is
     * "Which plan will MongoDB choose to run my query."
     */
    QUERY_PLANNER,

    /**
     * Runs the query optimizer, and then runs the winning plan to completion. In addition to the planner information, this makes execution
     * stats available. The use case for this verbosity level is "Is my query performing well."
     */
    EXECUTION_STATS,

    /**
     * Runs the query optimizer and chooses the winning plan, but then runs all generated plans to completion. This makes execution
     * stats available for all of the query plans. The use case for this verbosity level is "I have a problem with this query,
     * and I want as much information as possible in order to diagnose why it might be slow."
     */
    ALL_PLANS_EXECUTIONS
}
