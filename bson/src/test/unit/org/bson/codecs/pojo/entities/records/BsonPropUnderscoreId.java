package org.bson.codecs.pojo.entities.records;

import org.bson.codecs.pojo.annotations.BsonProperty;

public record BsonPropUnderscoreId(@BsonProperty("_id") String id) {
}
