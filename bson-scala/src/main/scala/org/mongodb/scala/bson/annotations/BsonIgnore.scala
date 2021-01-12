package org.mongodb.scala.bson.annotations

import scala.annotation.StaticAnnotation

/**
 * Annotation to ignore a property.
 */
case class BsonIgnore() extends StaticAnnotation
