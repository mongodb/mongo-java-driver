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

import com.mongodb.client.model.vault.{ DataKeyOptions => JDataKeyOptions }
import com.mongodb.client.model.vault.{ EncryptOptions => JEncryptOptions }
import com.mongodb.client.model.vault.{ RangeOptions => JRangeOptions }
import com.mongodb.client.model.vault.{ RewrapManyDataKeyResult => JRewrapManyDataKeyResult }
import com.mongodb.client.model.vault.{ RewrapManyDataKeyOptions => JRewrapManyDataKeyOptions }

/**
 * This package contains options classes for the key vault API
 *
 * @since 2.7
 */
package object vault {

  /**
   * The options for creating a data key.
   */
  type DataKeyOptions = JDataKeyOptions

  object DataKeyOptions {
    def apply(): DataKeyOptions = new JDataKeyOptions()
  }

  /**
   * The options for explicit encryption.
   */
  type EncryptOptions = JEncryptOptions

  /**
   * The options for explicit encryption.
   */
  object EncryptOptions {

    /**
     * Construct an instance with the given algorithm.
     *
     * @param algorithm the encryption algorithm
     */
    def apply(algorithm: String): EncryptOptions = new JEncryptOptions(algorithm)
  }

  /**
   * Range options specifies index options for a Queryable Encryption field supporting "range" queries.
   * @since 4.9
   */
  type RangeOptions = JRangeOptions

  object RangeOptions {
    def apply(): RangeOptions = new JRangeOptions()
  }

  /**
   * The result of the rewrapping of data keys
   *
   * @since 5.6
   */
  type RewrapManyDataKeyResult = JRewrapManyDataKeyResult

  /**
   * The result of the rewrapping of data keys
   *
   * @since 5.6
   */
  type RewrapManyDataKeyOptions = JRewrapManyDataKeyOptions

  /**
   * The rewrap many data key options
   *
   * The `getMasterKey` document MUST have the fields corresponding to the given provider as specified in masterKey.
   *
   * @since 5.6
   */
  object RewrapManyDataKeyOptions {
    def apply() = new JRewrapManyDataKeyOptions()
  }
}
