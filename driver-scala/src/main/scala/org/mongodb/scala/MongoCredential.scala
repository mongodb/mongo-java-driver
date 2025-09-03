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

import com.mongodb.{ MongoCredential => JMongoCredential }

/**
 * Represents credentials to authenticate to a MongoDB server, as well as the source of the credentials and the authentication mechanism
 * to use.
 *
 * @since 1.0
 */
object MongoCredential {

  /**
   * The GSSAPI mechanism.  See the <a href="http://tools.ietf.org/html/rfc4752">RFC</a>.
   *
    @see [[https://www.mongodb.com/docs/manual/core/authentication/#kerberos-authentication GSSAPI]]
   * @since 4.0
   */
  val GSSAPI_MECHANISM: String = JMongoCredential.GSSAPI_MECHANISM

  /**
   * The PLAIN mechanism.  See the <a href="http://www.ietf.org/rfc/rfc4616.txt">RFC</a>.
   *
    @see [[https://www.mongodb.com/docs/manual/core/authentication/#ldap-proxy-authority-authentication PLAIN]]
   * @since 4.0
   */
  val PLAIN_MECHANISM: String = JMongoCredential.PLAIN_MECHANISM

  /**
   * The MongoDB X.509
   *
    @see [[https://www.mongodb.com/docs/manual/core/authentication/#x-509-certificate-authentication X-509]]
   * @since 4.0
   */
  val MONGODB_X509_MECHANISM: String = JMongoCredential.MONGODB_X509_MECHANISM

  /**
   * The SCRAM-SHA-1 Mechanism.
   *
   * @note Requires MongoDB 3.0 or greater
    @see [[https://www.mongodb.com/docs/manual/core/authentication/#authentication-scram-sha-1 SCRAM-SHA-1]]
   * @since 4.0
   */
  val SCRAM_SHA_1_MECHANISM: String = JMongoCredential.SCRAM_SHA_1_MECHANISM

  /**
   * The SCRAM-SHA-256 Mechanism.
   *
   * @since 3.8
   * @note Requires MongoDB 4.0 or greater
    @see [[https://www.mongodb.com/docs/manual/core/authentication/#authentication-scram-sha-256 SCRAM-SHA-256]]
   */
  val SCRAM_SHA_256_MECHANISM: String = JMongoCredential.SCRAM_SHA_256_MECHANISM

  /**
   * Mechanism property key for overriding the service name for GSSAPI authentication.
   *
   * @see #createGSSAPICredential(String)
   * @see #withMechanismProperty(String, Object)
   * @since 4.0
   */
  val SERVICE_NAME_KEY: String = JMongoCredential.SERVICE_NAME_KEY

  /**
   * Mechanism property key for specifying whether to canonicalize the host name for GSSAPI authentication.
   *
   * @see #createGSSAPICredential(String)
   * @see #withMechanismProperty(String, Object)
   * @since 4.0
   */
  val CANONICALIZE_HOST_NAME_KEY: String = JMongoCredential.CANONICALIZE_HOST_NAME_KEY

  /**
   * Mechanism property key for overriding the SaslClient properties for GSSAPI authentication.
   *
   * The value of this property must be a `Map[String, Object]`.  In most cases there is no need to set this mechanism property.
   * But if an application does:
   *
   *   - Generally it must set the `javax.security.sasl.Sasl#CREDENTIALS` property to an instance of `org.ietf.jgss.GSSCredential`
   *   - It's recommended that it set the `javax.security.sasl.Sasl#MAX_BUFFER` property to "0" to ensure compatibility with all
   *     versions of MongoDB.
   *
   * @see #createGSSAPICredential(String)
   * @see #withMechanismProperty(String, Object)
   * @see javax.security.sasl.Sasl
   * @see javax.security.sasl.Sasl#CREDENTIALS
   * @see javax.security.sasl.Sasl#MAX_BUFFER
   * @since 4.0
   */
  val JAVA_SASL_CLIENT_PROPERTIES_KEY: String = JMongoCredential.JAVA_SASL_CLIENT_PROPERTIES_KEY

  /**
   * Mechanism property key for overriding the `javax.security.auth.Subject` under which GSSAPI authentication executes.
   *
   * @see #createGSSAPICredential(String)
   * @see #withMechanismProperty(String, Object)
   * @since 4.0
   */
  val JAVA_SUBJECT_KEY: String = JMongoCredential.JAVA_SUBJECT_KEY

  /**
   * Creates a MongoCredential instance with an unspecified mechanism.  The client will negotiate the best mechanism based on the
   * version of the server that the client is authenticating to.  If the server version is 3.0 or higher,
   * the driver will authenticate using the SCRAM-SHA-1 mechanism.  Otherwise, the driver will authenticate using the MONGODB_CR
   * mechanism.
   *
   *
   * @param userName the user name
   * @param database the database where the user is defined
   * @param password the user's password
   * @return the credential
   *
   * @see [[https://www.mongodb.com/docs/manual/core/authentication/#mongodb-cr-authentication MONGODB-CR]]
   * @see [[https://www.mongodb.com/docs/manual/core/authentication/#authentication-scram-sha-1 SCRAM-SHA-1]]
   */
  def createCredential(userName: String, database: String, password: Array[Char]): JMongoCredential =
    JMongoCredential.createCredential(userName, database, password)

  /**
   * Creates a MongoCredential instance for the SCRAM-SHA-1 SASL mechanism. Use this method only if you want to ensure that
   * the driver uses the MONGODB_CR mechanism regardless of whether the server you are connecting to supports a more secure
   * authentication mechanism.  Otherwise use the [[createCredential]] method to allow the driver to
   * negotiate the best mechanism based on the server version.
   *
   *
   * @param userName the non-null user name
   * @param source the source where the user is defined.
   * @param password the non-null user password
   * @return the credential
   * @see [[createCredential]]
   *
   * @see [[https://www.mongodb.com/docs/manual/core/authentication/#authentication-scram-sha-1 SCRAM-SHA-1]]
   */
  def createScramSha1Credential(userName: String, source: String, password: Array[Char]): JMongoCredential =
    JMongoCredential.createScramSha1Credential(userName, source, password)

  /**
   * Creates a MongoCredential instance for the SCRAM-SHA-256 SASL mechanism.
   *
   *
   * @param userName the non-null user name
   * @param source the source where the user is defined.
   * @param password the non-null user password
   * @return the credential
   * @note Requires MongoDB 4.0 or greater
   * @see [[https://www.mongodb.com/docs/manual/core/authentication/#authentication-scram-sha-256 SCRAM-SHA-256]]
   */
  def createScramSha256Credential(userName: String, source: String, password: Array[Char]): JMongoCredential =
    JMongoCredential.createScramSha256Credential(userName, source, password)

  /**
   * Creates a MongoCredential instance for the MongoDB X.509 protocol.
   *
   * @param userName the user name
   * @return the credential
   * @see [[https://www.mongodb.com/docs/manual/core/authentication/#x-509-certificate-authentication X-509]]
   */
  def createMongoX509Credential(userName: String): JMongoCredential =
    JMongoCredential.createMongoX509Credential(userName)

  /**
   * Creates a MongoCredential instance for the MongoDB X.509 protocol where the distinguished subject name of the client certificate
   * acts as the userName.
   *
   * @return the credential
   * @see [[https://www.mongodb.com/docs/manual/core/authentication/#x-509-certificate-authentication X-509]]
   * @since 1.2
   * @note Requires MongoDB 3.4 or greater
   */
  def createMongoX509Credential(): JMongoCredential = JMongoCredential.createMongoX509Credential()

  /**
   * Creates a MongoCredential instance for the PLAIN SASL mechanism.
   *
   * @param userName the non-null user name
   * @param source   the source where the user is defined.  This can be either `\$external` or the name of a database.
   * @param password the non-null user password
   * @return the credential
   * @see [[https://www.mongodb.com/docs/manual/core/authentication/#ldap-proxy-authority-authentication PLAIN]]
   */
  def createPlainCredential(userName: String, source: String, password: Array[Char]): JMongoCredential =
    JMongoCredential.createPlainCredential(userName, source, password)

  /**
   * Creates a MongoCredential instance for the GSSAPI SASL mechanism.  To override the default service name of `mongodb`, add a
   * mechanism property with the name `SERVICE_NAME`. To force canonicalization of the host name prior to authentication, add a
   * mechanism property with the name `CANONICALIZE_HOST_NAME` with the value `true`.
   *
   * @param userName the non-null user name
   * @return the credential
   * @see [[https://www.mongodb.com/docs/manual/core/authentication/#kerberos-authentication GSSAPI]]
   */
  def createGSSAPICredential(userName: String): JMongoCredential = JMongoCredential.createGSSAPICredential(userName)

}
