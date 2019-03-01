package org.bson.codecs.pojo;

import org.bson.codecs.configuration.CodecConfigurationException;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isPublic;

public class ConventionSetAllFieldImpl implements Convention {
    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        for (PropertyModelBuilder<?> propertyModelBuilder : classModelBuilder.getPropertyModelBuilders()) {
            if (!(propertyModelBuilder.getPropertyAccessor() instanceof PropertyAccessorImpl)) {
                throw new CodecConfigurationException(format("The SET_ALL_FIELDS_CONVENTION is not compatible with "
                                + "propertyModelBuilder instance that have custom implementations of org.bson.codecs.pojo.PropertyAccessor: %s",
                        propertyModelBuilder.getPropertyAccessor().getClass().getName()));
            }
            PropertyAccessorImpl<?> defaultAccessor = (PropertyAccessorImpl<?>) propertyModelBuilder.getPropertyAccessor();
            PropertyMetadata<?> propertyMetaData = defaultAccessor.getPropertyMetadata();
            if (!propertyMetaData.isDeserializable() && propertyMetaData.getField() != null
                    && !isPublic(propertyMetaData.getField().getModifiers())) {
                setPropertyAccessor(propertyModelBuilder);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void setPropertyAccessor(final PropertyModelBuilder<T> propertyModelBuilder) {
        propertyModelBuilder.propertyAccessor(new AnyPropertyAccessor<T>(
                (PropertyAccessorImpl<T>) propertyModelBuilder.getPropertyAccessor()));
    }

    private static final class AnyPropertyAccessor<T> implements PropertyAccessor<T> {
        private final PropertyAccessorImpl<T> wrapped;

        private AnyPropertyAccessor(final PropertyAccessorImpl<T> wrapped) {
            this.wrapped = wrapped;
            try {
                wrapped.getPropertyMetadata().getField().setAccessible(true);
            } catch (Exception e) {
                throw new CodecConfigurationException(format("Unable to make field accessible '%s' in %s",
                        wrapped.getPropertyMetadata().getName(), wrapped.getPropertyMetadata().getDeclaringClassName()), e);
            }
        }

        @Override
        public <S> T get(final S instance) {
            return wrapped.get(instance);
        }

        @Override
        public <S> void set(final S instance, final T value) {
            try {
                wrapped.getPropertyMetadata().getField().set(instance, value);
            } catch (Exception e) {
                throw new CodecConfigurationException(format("Unable to set value for property '%s' in %s",
                        wrapped.getPropertyMetadata().getName(), wrapped.getPropertyMetadata().getDeclaringClassName()), e);
            }
        }
    }
}
