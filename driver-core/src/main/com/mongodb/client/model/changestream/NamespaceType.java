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

package com.mongodb.client.model.changestream;

import com.mongodb.lang.Nullable;

/**
 * Represents the type of the newly created namespace object in change stream events.
 * <p>
 * Only present for operations of type {@code create} and when the {@code showExpandedEvents}
 * change stream option is enabled.
 * </p>
 *
 * @since 5.6
 * @mongodb.server.release 8.1
 */
public enum NamespaceType {
    COLLECTION("collection"),
    TIMESERIES("timeseries"),
    VIEW("view"),
    /**
     * The other namespace type.
     *
     * <p>A placeholder for newer namespace types issued by the server.
     * Users encountering OTHER namespace types are advised to update the driver to get the actual namespace type.</p>
     *
     * @since 5.6
     */
    OTHER("other");

    private final String value;
    NamespaceType(final String namespaceTypeName) {
        this.value = namespaceTypeName;
    }

    /**
     * @return the String representation of the namespace type
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the ChangeStreamNamespaceType from the string value.
     *
     * @param namespaceTypeName the string value.
     * @return the namespace type.
     */
    public static NamespaceType fromString(@Nullable final String namespaceTypeName) {
        if (namespaceTypeName != null) {
            for (NamespaceType namespaceType : NamespaceType.values()) {
                if (namespaceTypeName.equals(namespaceType.value)) {
                    return namespaceType;
                }
            }
        }
        return OTHER;
    }

    @Override
    public String toString() {
        return "NamespaceType{"
                + "value='" + value + "'"
                + "}";
    }
}
