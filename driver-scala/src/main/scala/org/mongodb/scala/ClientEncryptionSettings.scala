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
 */ package org.mongodb.scala

import com.mongodb.{ClientEncryptionSettings => JClientEncryptionSettings}

/**
 * The client-side settings for data key creation and explicit encryption.
 *
 * Explicit encryption/decryption is a community feature, enabled with the new `com.mongodb.client.vault.ClientEncryption` type,
 * for which this is the settings.
 *
 * @since 2.7
 */
object ClientEncryptionSettings {

  /**
   * Gets a Builder for creating a new AutoEncryptionSettings instance.
   *
   * @return a new Builder for creating AutoEncryptionSettings.
   */
  def builder(): Builder = JClientEncryptionSettings.builder()

  /**
   * AutoEncryptionSettings builder type
   */
  type Builder = JClientEncryptionSettings.Builder
}
