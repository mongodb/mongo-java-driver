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

package org.mongodb.scala.internal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import com.mongodb.{ WriteConcern => JWriteConcern }

import org.mongodb.scala.WriteConcern

private[scala] trait WriteConcernImplicits {

  implicit class ScalaWriteConcern[T](jWriteConcern: JWriteConcern) {

    /**
     * Constructs a new WriteConcern from the current one and the specified wTimeout in the given time unit.
     *
     * @param wTimeout the wTimeout, which must be &gt;= 0 and &lt;= Integer.MAX_VALUE after conversion to milliseconds
     * @return the WriteConcern with the given wTimeout
     * @deprecated Prefer using the operation execution timeout configuration options available at the following levels:
     *
     *             - [[org.mongodb.scala.MongoClientSettings.Builder timeout(long, TimeUnit)]]
     *             - [[org.mongodb.scala.MongoDatabase.withTimeout withTimeout(long, TimeUnit)]]
     *             - [[org.mongodb.scala.MongoCollection.withTimeout withTimeout(long, TimeUnit)]]
     *             - [[org.mongodb.scala.ClientSessionOptions]]
     *             - [[org.mongodb.scala.TransactionOptions]]
     *
     * When executing an operation, any explicitly set timeout at these levels takes precedence, rendering this wTimeout
     * irrelevant. If no timeout is specified at these levels, the wTimeout will be used.
     */
    @deprecated
    def withWTimeout(wTimeout: Duration): WriteConcern =
      jWriteConcern.withWTimeout(wTimeout.toMillis, TimeUnit.MILLISECONDS)
  }

}
