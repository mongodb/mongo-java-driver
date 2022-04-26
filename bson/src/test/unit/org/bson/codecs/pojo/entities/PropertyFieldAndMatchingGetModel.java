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

public class PropertyFieldAndMatchingGetModel {
    public String publicProperty;
    String protectedProperty;
    private String privateProperty;

    public PropertyFieldAndMatchingGetModel(){
    }

    public PropertyFieldAndMatchingGetModel(final String publicProperty, final String protectedProperty, final String privateProperty) {
        this.publicProperty = publicProperty;
        this.protectedProperty = protectedProperty;
        this.privateProperty = privateProperty;
    }

    public String getPublicProperty() {
        return publicProperty;
    }

    String getProtectedProperty() {
        return protectedProperty;
    }

    private String getPrivateProperty() {
        return privateProperty;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PropertyFieldAndMatchingGetModel that = (PropertyFieldAndMatchingGetModel) o;
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
        return "PropertyFieldAndGetModel{"
                + "publicProperty='" + publicProperty + '\''
                + ", protectedProperty='" + protectedProperty + '\''
                + ", privateProperty='" + privateProperty + '\''
                + '}';
    }
}
