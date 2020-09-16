package org.bson.codecs.pojo.entities.records;

import org.bson.codecs.pojo.annotations.BsonProperty;

public record BsonPropUnderscoreIdNonIdName(@BsonProperty("_id") String name) {
}
