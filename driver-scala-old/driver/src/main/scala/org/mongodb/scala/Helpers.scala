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

package org.mongodb.scala

import java.util.concurrent.Executor

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Custom helpers for the client
 */
private[scala] object Helpers {

  /**
   * Helper to get the class from a classTag
   *
   * @param ct the classTag we want to implicitly get the class of
   * @tparam C the class type
   * @return the classOf[C]
   */
  implicit def classTagToClassOf[C](ct: ClassTag[C]): Class[C] = ct.runtimeClass.asInstanceOf[Class[C]]

  /**
   * Direct Execution Context uses the current context to run the command in.
   */
  final val DirectExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(new Executor {
    override def execute(command: Runnable): Unit = command.run()
  })
}
