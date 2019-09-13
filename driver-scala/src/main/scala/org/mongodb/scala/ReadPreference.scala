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

import java.util.concurrent.TimeUnit.MILLISECONDS

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

import com.mongodb.{ ReadPreference => JReadPreference }

/**
 * The preferred replica set members to which a query or command can be sent.
 *
 * @since 1.0
 */
object ReadPreference {

  /**
   * Gets a read preference that forces read to the primary.
   *
   * @return ReadPreference which reads from primary only
   */
  def primary(): ReadPreference = JReadPreference.primary()

  /**
   * Gets a read preference that forces reads to the primary if available, otherwise to a secondary.
   *
   * @return ReadPreference which reads primary if available.
   */
  def primaryPreferred(): ReadPreference = JReadPreference.primaryPreferred()

  /**
   * Gets a read preference that forces reads to a secondary.
   *
   * @return ReadPreference which reads secondary.
   */
  def secondary(): ReadPreference = JReadPreference.secondary()

  /**
   * Gets a read preference that forces reads to a secondary if one is available, otherwise to the primary.
   *
   * @return ReadPreference which reads secondary if available, otherwise from primary.
   */
  def secondaryPreferred(): ReadPreference = JReadPreference.secondaryPreferred()

  /**
   * Gets a read preference that forces reads to a primary or a secondary.
   *
   * @return ReadPreference which reads nearest
   */
  def nearest(): ReadPreference = JReadPreference.nearest()

  /**
   * Gets a read preference that forces reads to the primary if available, otherwise to a secondary.
   *
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads primary if available.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def primaryPreferred(maxStaleness: Duration): ReadPreference =
    JReadPreference.primaryPreferred(maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to a secondary.
   *
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads secondary.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def secondary(maxStaleness: Duration): ReadPreference = JReadPreference.secondary(maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to a secondary if one is available, otherwise to the primary.
   *
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads secondary if available, otherwise from primary.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def secondaryPreferred(maxStaleness: Duration): ReadPreference =
    JReadPreference.secondaryPreferred(maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to a primary or a secondary.
   *
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads nearest
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def nearest(maxStaleness: Duration): ReadPreference = JReadPreference.nearest(maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with the given set of tags.
   *
   * @param tagSet the set of tags to limit the list of secondaries to.
   * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
   */
  def primaryPreferred(tagSet: TagSet): TaggableReadPreference = JReadPreference.primaryPreferred(tagSet)

  /**
   * Gets a read preference that forces reads to a secondary with the given set of tags.
   *
   * @param tagSet the set of tags to limit the list of secondaries to
   * @return ReadPreference which reads secondary respective of tags.
   */
  def secondary(tagSet: TagSet): TaggableReadPreference = JReadPreference.secondary(tagSet)

  /**
   * Gets a read preference that forces reads to a secondary with the given set of tags, or the primary is none are available.
   *
   * @param tagSet the set of tags to limit the list of secondaries to
   * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
   */
  def secondaryPreferred(tagSet: TagSet): TaggableReadPreference = JReadPreference.secondaryPreferred(tagSet)

  /**
   * Gets a read preference that forces reads to the primary or a secondary with the given set of tags.
   *
   * @param tagSet the set of tags to limit the list of secondaries to
   * @return ReadPreference which reads nearest node respective of tags.
   */
  def nearest(tagSet: TagSet): TaggableReadPreference = JReadPreference.nearest(tagSet)

  /**
   * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with the given set of tags.
   *
   * @param tagSet       the set of tags to limit the list of secondaries to.
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def primaryPreferred(tagSet: TagSet, maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.primaryPreferred(tagSet, maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to a secondary with the given set of tags.
   *
   * @param tagSet       the set of tags to limit the list of secondaries to
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads secondary respective of tags.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def secondary(tagSet: TagSet, maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.secondary(tagSet, maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to a secondary with the given set of tags, or the primary is none are available.
   *
   * @param tagSet       the set of tags to limit the list of secondaries to
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def secondaryPreferred(tagSet: TagSet, maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.secondaryPreferred(tagSet, maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to the primary or a secondary with the given set of tags.
   *
   * @param tagSet       the set of tags to limit the list of secondaries to
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads nearest node respective of tags.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def nearest(tagSet: TagSet, maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.nearest(tagSet, maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with one of the given sets of tags.
   * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
   * or failing if no secondary can be found that matches any of the tag sets in the list.
   *
   * @param tagSetList the list of tag sets to limit the list of secondaries to
   * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
   */
  def primaryPreferred(tagSetList: Seq[TagSet]): TaggableReadPreference =
    JReadPreference.primaryPreferred(tagSetList.asJava)

  /**
   * Gets a read preference that forces reads to a secondary with one of the given sets of tags.
   * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
   * or failing if no secondary can be found that matches any of the tag sets in the list.
   *
   * @param tagSetList the list of tag sets to limit the list of secondaries to
   * @return ReadPreference which reads secondary respective of tags.
   */
  def secondary(tagSetList: Seq[TagSet]): TaggableReadPreference = JReadPreference.secondary(tagSetList.asJava)

  /**
   * Gets a read preference that forces reads to a secondary with one of the given sets of tags.
   * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
   * or the primary if none are available.
   *
   * @param tagSetList the list of tag sets to limit the list of secondaries to
   * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
   */
  def secondaryPreferred(tagSetList: Seq[TagSet]): TaggableReadPreference =
    JReadPreference.secondaryPreferred(tagSetList.asJava)

  /**
   * Gets a read preference that forces reads to the primary or a secondary with one of the given sets of tags.
   * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
   * or the primary if none are available.
   *
   * @param tagSetList the list of tag sets to limit the list of secondaries to
   * @return ReadPreference which reads nearest node respective of tags.
   */
  def nearest(tagSetList: Seq[TagSet]): TaggableReadPreference = JReadPreference.nearest(tagSetList.asJava)

  /**
   * Gets a read preference that forces reads to the primary if available, otherwise to a secondary with one of the given sets of tags.
   * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
   * or failing if no secondary can be found that matches any of the tag sets in the list.
   *
   * @param tagSetList   the list of tag sets to limit the list of secondaries to
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads primary if available, otherwise a secondary respective of tags.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def primaryPreferred(tagSetList: Seq[TagSet], maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.primaryPreferred(tagSetList.asJava, maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to a secondary with one of the given sets of tags.
   * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
   * or failing if no secondary can be found that matches any of the tag sets in the list.
   *
   * @param tagSetList   the list of tag sets to limit the list of secondaries to
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads secondary respective of tags.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def secondary(tagSetList: Seq[TagSet], maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.secondary(tagSetList.asJava, maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to a secondary with one of the given sets of tags.
   * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
   * or the primary if none are available.
   *
   * @param tagSetList   the list of tag sets to limit the list of secondaries to
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads secondary if available respective of tags, otherwise from primary irrespective of tags.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def secondaryPreferred(tagSetList: Seq[TagSet], maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.secondaryPreferred(tagSetList.asJava, maxStaleness.toMillis, MILLISECONDS)

  /**
   * Gets a read preference that forces reads to the primary or a secondary with one of the given sets of tags.
   * The driver will look for a secondary with each tag set in the given list, stopping after one is found,
   * or the primary if none are available.
   *
   * @param tagSetList   the list of tag sets to limit the list of secondaries to
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return ReadPreference which reads nearest node respective of tags.
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def nearest(tagSetList: Seq[TagSet], maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.nearest(tagSetList.asJava, maxStaleness.toMillis, MILLISECONDS)

  /**
   * Creates a read preference from the given read preference name.
   *
   * @param name the name of the read preference
   * @return the read preference
   */
  def valueOf(name: String): ReadPreference = JReadPreference.valueOf(name)

  /**
   * Creates a taggable read preference from the given read preference name and list of tag sets.
   *
   * @param name       the name of the read preference
   * @param tagSetList the list of tag sets
   * @return the taggable read preference
   */
  def valueOf(name: String, tagSetList: Seq[TagSet]): TaggableReadPreference =
    JReadPreference.valueOf(name, tagSetList.asJava)

  /**
   * Creates a taggable read preference from the given read preference name and list of tag sets.
   *
   * @param name       the name of the read preference
   * @param tagSetList the list of tag sets
   * @param maxStaleness the max allowable staleness of secondaries. The minimum value is either 90 seconds, or the heartbeat frequency
   *                     plus 10 seconds, whichever is greatest.
   * @return the taggable read preference
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def valueOf(name: String, tagSetList: Seq[TagSet], maxStaleness: Duration): TaggableReadPreference =
    JReadPreference.valueOf(name, tagSetList.asJava, maxStaleness.toMillis, MILLISECONDS)

}
