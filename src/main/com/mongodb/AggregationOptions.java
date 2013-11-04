package com.mongodb;


/**
 * @since 2.12
 */
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

    AggregationOptions(final Builder builder) {
        this.batchSize = builder.getBatchSize();
        this.allowDiskUsage = builder.getAllowDiskUsage();
        this.outputMode = builder.getOutputMode();
    }

    /**
     * If true, this enables external sort capabilities otherwise $sort produces an error if the operation consumes 10 percent or more of 
     * RAM.
     */
    public Boolean getAllowDiskUsage() {
        return allowDiskUsage;
    }

    /**
     * The size of batches to use when iterating over results.
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * The mode of output for this configuration.  
     * @see OutputMode
     */
    public OutputMode getOutputMode() {
        return outputMode;
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
            return new AggregationOptions(this);
        }
    }
}
