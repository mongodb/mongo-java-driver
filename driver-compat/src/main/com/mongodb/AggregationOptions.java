package com.mongodb;


public class AggregationOptions {
    private final Integer batchSize;
    private final Boolean allowDiskUsage;
    private final OutputMode outputMode;

    public enum OutputMode {
        /**
         * The output of the aggregate operation is returned inline.
         */
        INLINE {
            @Override
            public org.mongodb.AggregationOptions.OutputMode toNew() {
                return org.mongodb.AggregationOptions.OutputMode.INLINE;
            }
        },
        /**
         * The output of the aggregate operation is returned using a cursor.
         */
        CURSOR {
            @Override
            public org.mongodb.AggregationOptions.OutputMode toNew() {
                return org.mongodb.AggregationOptions.OutputMode.CURSOR;
            }
        };

        public abstract org.mongodb.AggregationOptions.OutputMode toNew();
    }

    AggregationOptions(final Integer batchSize, final Boolean allowDiskUsage, final OutputMode outputMode) {
        this.batchSize = batchSize;
        this.allowDiskUsage = allowDiskUsage;
        this.outputMode = outputMode;
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

    public org.mongodb.AggregationOptions toNew() {
        return org.mongodb.AggregationOptions.builder()
                          .batchSize(getBatchSize())
                          .allowDiskUsage(getAllowDiskUsage())
                          .outputMode(getOutputMode().toNew())
                          .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Integer batchSize;
        private Boolean allowDiskUsage;
        private OutputMode outputMode = OutputMode.INLINE;

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

        public Boolean getAllowDiskUsage() {
            return allowDiskUsage;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public OutputMode getOutputMode() {
            return outputMode;
        }

        public AggregationOptions build() {
            return new AggregationOptions(batchSize, allowDiskUsage, outputMode);
        }
    }
}
