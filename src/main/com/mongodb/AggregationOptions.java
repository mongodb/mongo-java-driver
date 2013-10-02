package com.mongodb;


public class AggregationOptions {
    private final Integer batchSize;
    private final Boolean allowDiskUsage;
    private final OutputMode outputMode;

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

    public Object toDBObject() {
        DBObject document = new BasicDBObject();
        putIfNotNull(document, "batchSize", batchSize);
        putIfNotNull(document, "allowDiskUsage", allowDiskUsage);
        return document;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AggregationOptions{");
        sb.append("allowDiskUsage=").append(allowDiskUsage);
        sb.append(", batchSize=").append(batchSize);
        sb.append(", outputMode=").append(outputMode);
        sb.append('}');
        return sb.toString();
    }

    private void putIfNotNull(final DBObject document, final String name, final Object value) {
        if (value != null) {
            document.put(name, value);
        }
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
