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

package com.mongodb.observability;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.Reason;
import com.mongodb.annotations.Sealed;
import com.mongodb.observability.micrometer.MicrometerObservabilitySettings;

/**
 * Observability settings for the driver.
 *
 * @since 5.7
 */
@Alpha(Reason.CLIENT)
@Sealed
@Immutable
public abstract class ObservabilitySettings {
    /**
     * A builder for {@link MicrometerObservabilitySettings}.
     *
     * @return a builder for {@link MicrometerObservabilitySettings}
     */
    public static MicrometerObservabilitySettings.Builder micrometerBuilder() {
        return MicrometerObservabilitySettings.builder();
    }
}
