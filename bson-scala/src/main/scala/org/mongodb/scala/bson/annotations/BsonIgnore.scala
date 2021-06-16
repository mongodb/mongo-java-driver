package org.mongodb.scala.bson.annotations

import scala.annotation.StaticAnnotation

/**
 * Annotation to ignore a property.
 *
 * @since 4.2
 */
case class BsonIgnore() extends StaticAnnotation
