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

package org.bson.codecs.pojo.entities.conventions;

import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.InstanceCreator;
import org.bson.codecs.pojo.PropertyModel;
import org.bson.codecs.pojo.entities.InterfaceModelB;
import org.bson.codecs.pojo.entities.InterfaceModelImpl;

public class InterfaceModelBInstanceCreatorConvention implements Convention {
    @Override
    @SuppressWarnings("unchecked")
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        if (classModelBuilder.getType().equals(InterfaceModelB.class)) {
            // Simulate a custom implementation of InstanceCreator factory
            // (This one can be generated automatically, but, a real use case can have an advanced reflection based
            // solution that the POJO Codec doesn't support out of the box)
            ((ClassModelBuilder<InterfaceModelB>) classModelBuilder).instanceCreatorFactory(() -> {
                InterfaceModelB interfaceModelB = new InterfaceModelImpl();
                return new InstanceCreator<InterfaceModelB>() {
                    @Override
                    public <S> void set(final S value, final PropertyModel<S> propertyModel) {
                        if (propertyModel.getName().equals("propertyA")) {
                            interfaceModelB.setPropertyA((String) value);
                        } else if (propertyModel.getName().equals("propertyB")) {
                            interfaceModelB.setPropertyB((String) value);
                        }
                    }

                    @Override
                    public InterfaceModelB getInstance() {
                        return interfaceModelB;
                    }
                };
            });
        }
    }
}
