package org.bson.codecs.record.samples;

public record BarRecord<T>(BazRecord<T> baz) {
}
