package org.bson.codecs.record.samples;

import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

import javax.annotation.Nullable;

public record TestRecordWithNullableAnnotation (@BsonId ObjectId id, @Nullable String name) {
}
