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

package org.bson.codecs.pojo.entities;

import java.util.Objects;

public class PropertyFieldAndMethodPrefixModel {
    private static final String GET_PREFIX = "[get] ";
    private static final String SET_PREFIX = "[set] ";
    public String publicProperty;
    String protectedProperty;
    private String privateProperty;

    public PropertyFieldAndMethodPrefixModel(){
    }

    public PropertyFieldAndMethodPrefixModel(final String publicProperty, final String protectedProperty, final String privateProperty) {
        this.publicProperty = publicProperty;
        this.protectedProperty = protectedProperty;
        this.privateProperty = privateProperty;
    }

    public String getPublicProperty() {
        return GET_PREFIX + publicProperty;
    }

    public PropertyFieldAndMethodPrefixModel setPublicProperty(final String publicProperty) {
        this.publicProperty = SET_PREFIX + publicProperty;
        return this;
    }

    public String getProtectedProperty() {
        return GET_PREFIX + protectedProperty;
    }

    public PropertyFieldAndMethodPrefixModel setProtectedProperty(final String protectedProperty) {
        this.protectedProperty = SET_PREFIX + protectedProperty;
        return this;
    }

    public String getPrivateProperty() {
        return GET_PREFIX + privateProperty;
    }

    public PropertyFieldAndMethodPrefixModel setPrivateProperty(final String privateProperty) {
        this.privateProperty = SET_PREFIX + privateProperty;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PropertyFieldAndMethodPrefixModel that = (PropertyFieldAndMethodPrefixModel) o;
        return Objects.equals(publicProperty, that.publicProperty)
                && Objects.equals(protectedProperty, that.protectedProperty)
                && Objects.equals(privateProperty, that.privateProperty);
    }

    @Override
    public int hashCode() {
        return Objects.hash(publicProperty, protectedProperty, privateProperty);
    }

    @Override
    public String toString() {
        return "PropertyFieldOnlyModel{"
                + "publicProperty='" + publicProperty + '\''
                + ", protectedProperty='" + protectedProperty + '\''
                + ", privateProperty='" + privateProperty + '\''
                + '}';
    }
}
