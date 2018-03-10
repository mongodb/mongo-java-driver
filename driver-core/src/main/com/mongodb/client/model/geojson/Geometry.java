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

package com.mongodb.client.model.geojson;

import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider;
import com.mongodb.lang.Nullable;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;

/**
 * An abstract class for representations of GeoJSON geometry objects.
 *
 * @since 3.1
 */
public abstract class Geometry {

    private static final CodecRegistry REGISTRY = CodecRegistries.fromProviders(new GeoJsonCodecProvider());

    private final CoordinateReferenceSystem coordinateReferenceSystem;

    /**
     * Construct an instance with no specified coordinate reference system.
     *
     */
    protected Geometry() {
        this(null);
    }

    /**
     * Construct an instance with the specified coordinate reference system.
     *
     * @param coordinateReferenceSystem the coordinate reference system
     */
    protected Geometry(@Nullable final CoordinateReferenceSystem coordinateReferenceSystem) {
        this.coordinateReferenceSystem = coordinateReferenceSystem;
    }

    /**
     * Gets the GeoJSON object type.
     *
     * @return the type
     */
    public abstract GeoJsonObjectType getType();

    /**
     * Converts to GeoJSON representation
     *
     * @return the GeoJSON representation
     */
    @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
    public String toJson() {
        StringWriter stringWriter = new StringWriter();
        JsonWriter writer = new JsonWriter(stringWriter, new JsonWriterSettings());
        Codec codec = getRegistry().get(getClass());
        codec.encode(writer, this, EncoderContext.builder().build());
        return stringWriter.toString();
    }

    static CodecRegistry getRegistry() {
        return REGISTRY;
    }

    /**
     * Gets the coordinate reference system, which may be null
     *
     * @return the possibly-null coordinate reference system
     */
    @Nullable
    public CoordinateReferenceSystem getCoordinateReferenceSystem() {
        return coordinateReferenceSystem;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Geometry geometry = (Geometry) o;

        if (coordinateReferenceSystem != null ? !coordinateReferenceSystem.equals(geometry.coordinateReferenceSystem)
                                              : geometry.coordinateReferenceSystem != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return coordinateReferenceSystem != null ? coordinateReferenceSystem.hashCode() : 0;
    }
}
