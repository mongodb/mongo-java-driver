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

import scala.jdk.CollectionConverters._

import com.mongodb.{ TagSet => JTagSet }

/**
 * An immutable set of tags, used to select members of a replica set to use for read operations.
 */
object TagSet {

  /**
   * An empty set of tags.
   */
  def apply(): TagSet = new JTagSet()

  /**
   * A set of tags contain the single given tag
   *
   * @param tag the tag
   */
  def apply(tag: Tag): TagSet = new JTagSet(tag)

  /**
   * A set of tags containing the given list of tags.
   *
   * @param tagList the list of tags
   */
  def apply(tagList: Seq[Tag]): TagSet = new JTagSet(tagList.asJava)
}
