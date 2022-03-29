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

package org.mongodb.scala.model

import scala.util.Try

import com.mongodb.client.model.{ BucketGranularity => JBucketGranularity }

/**
 * Granularity values for automatic bucketing.
 *
 * @see [[https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucketAuto/ \$bucketAuto]]
 * @see [[https://en.wikipedia.org/wiki/Preferred_number">Preferred numbers]]
 * @since 1.2
 */
object BucketGranularity {
  val R5: BucketGranularity = JBucketGranularity.R5

  val R10: BucketGranularity = JBucketGranularity.R10

  val R20: BucketGranularity = JBucketGranularity.R20

  val R40: BucketGranularity = JBucketGranularity.R40

  val R80: BucketGranularity = JBucketGranularity.R80

  val SERIES_125: BucketGranularity = JBucketGranularity.SERIES_125

  val E6: BucketGranularity = JBucketGranularity.E6

  val E12: BucketGranularity = JBucketGranularity.E12

  val E24: BucketGranularity = JBucketGranularity.E24

  val E48: BucketGranularity = JBucketGranularity.E48

  val E96: BucketGranularity = JBucketGranularity.E96

  val E192: BucketGranularity = JBucketGranularity.E192

  val POWERSOF2: BucketGranularity = JBucketGranularity.POWERSOF2

  /**
   * Returns the BucketGranularity from the string value.
   *
   * @param value the string value.
   * @return the Bucket Granularity
   */
  def fromString(value: String): Try[BucketGranularity] = Try(JBucketGranularity.fromString(value))

}
