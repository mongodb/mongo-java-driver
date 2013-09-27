/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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


package org.mongodb;


public class AggregationOptions {
    public enum OutputMode {
        /**
         * The output of the aggregate operation is returned inline.
         */
        INLINE,
        /**
         * The output of the aggregate operation is returned using a cursor.
         */
        CURSOR
    }

    private final Integer batchSize;
    private final Boolean allowDiskUsage;
    private final OutputMode outputMode;

    public AggregationOptions(final Integer batchSize, final Boolean allowDiskUsage, final OutputMode outputMode) {
        this.batchSize = batchSize;
        this.allowDiskUsage = allowDiskUsage;
        this.outputMode = outputMode;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getBatchSize() {
        return batchSize;
    }

    public OutputMode getOutputMode() {
        return outputMode;
    }

    public Boolean getAllowDiskUsage() {
        return allowDiskUsage;
    }

    @Override
    public String toString() {
        return "AggregationOptions{" + "batchSize=" + batchSize + ", allowDiskUsage=" + allowDiskUsage + ", outputMode=" + outputMode + '}';
    }

    public Document toDocument() {
        Document document = new Document();
        putIfNotNull(document, "batchSize", batchSize);
        putIfNotNull(document, "allowDiskUsage", allowDiskUsage);
        return document;
    }

    private void putIfNotNull(final Document document, final String name, final Object value) {
        if (value != null) {
            document.put(name, value);
        }
    }

    public static class Builder {

        private Integer batchSize;
        private Boolean allowDiskUsage;
        private OutputMode outputMode = OutputMode.INLINE;
        private ReadPreference readPreference = ReadPreference.primary();

        protected Builder() {
        }

        public Builder batchSize(final Integer size) {
            batchSize = size;
            return this;
        }

        public Builder allowDiskUsage(final Boolean allow) {
            allowDiskUsage = allow;
            return this;
        }

        public Builder outputMode(final OutputMode mode) {
            outputMode = mode;
            return this;
        }

        public Builder readPreference(final ReadPreference preference) {
            readPreference = preference;
            return this;
        }

        public Boolean getAllowDiskUsage() {
            return allowDiskUsage;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public OutputMode getOutputMode() {
            return outputMode;
        }

        public ReadPreference getReadPreference() {
            return readPreference;
        }

        public AggregationOptions build() {
            return new AggregationOptions(batchSize, allowDiskUsage, outputMode);
        }
    }
}
