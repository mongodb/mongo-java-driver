package org.bson.codecs.record.samples;

import java.util.List;

public record BazRecord<T>(List<T> list, T single) {
}
