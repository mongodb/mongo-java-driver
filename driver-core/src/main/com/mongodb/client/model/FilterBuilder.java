package com.mongodb.client.model;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonValue;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * A builder for {@link Filter} instances
 *
 * @since 3.0
 */
public final class FilterBuilder {

    private FilterBuilder() {
    }

    /**
     * Creates a filter that ands together the provided list of filters.  Note that this will only generate a "$and" operator if absolutely
     * necessary, as the query language implicity ands together all the keys.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Filter and(final Iterable<Filter> filters) {
        return new AndFilter(filters);
    }

    /**
     * Creates a filter that ands together the provided list of filters.  Note that this will only generate a "$and" operator if absolutely
     * necessary, as the query language implicity ands together all the keys.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Filter and(final Filter... filters) {
        return and(asList(filters));
    }

    /**
     * Creates a filter that ands together the provided list of filters.  Note that this will only generate a "$and" operator if absolutely
     * necessary, as the query language implicity ands together all the keys.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Filter or(final Iterable<Filter> filters) {
        return new OrFilter(filters);
    }

    /**
     * Creates a filter that ands together the provided list of filters.  Note that this will only generate a "$and" operator if absolutely
     * necessary, as the query language implicity ands together all the keys.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Filter or(final Filter... filters) {
        return or(asList(filters));
    }

    /**
     * Creates a filter that matches all documents where the value of the field name equals the specified value. Note that this does
     * actually generate a $eq operator, as the query language doesn't require it.
     *
     * @param fieldName the field name
     * @param value     the value
     * @param <TField>  the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/eq $eq
     */
    public static <TField> Filter eq(final String fieldName, final TField value) {
        return new Filter() {
            @Override
            @SuppressWarnings("unchecked")
            public <TDocument> BsonDocument render(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

                writer.writeStartDocument();
                writer.writeName(fieldName);
                ((Encoder) codecRegistry.get(value.getClass())).encode(writer, value, EncoderContext.builder().build());
                writer.writeEndDocument();

                return writer.getDocument();
            }
        };
    }


    /**
     * Creates a filter that matches all documents that contain the given field.
     *
     * @param fieldName the field name
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/exists $exists
     */
    public static Filter exists(final String fieldName) {
        return exists(fieldName, true);
    }

    /**
     * Creates a filter that matches all documents that either contain or do not contain the given field, depending on the value of the
     * exists parameter.
     *
     * @param fieldName the field name
     * @param exists true to check for existence, false to check for absence
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/exists $exists
     */

    public static Filter exists(final String fieldName, final boolean exists) {
        return new OperatorFilter<BsonBoolean>("$exists", fieldName, BsonBoolean.valueOf(exists));
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is greater than the specified value.
     *
     * @param fieldName the field name
     * @param value the value
     * @param <TField> the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/gt $gt
     */
    public static <TField> Filter gt(final String fieldName, final TField value) {

        return new OperatorFilter<TField>("$gt", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is less than the specified value.
     *
     * @param fieldName the field name
     * @param value the value
     * @param <TField> the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/gt $gt
     */
    public static <TField> Filter lt(final String fieldName, final TField value) {
        return new OperatorFilter<TField>("$lt", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is greater than or equal to the specified value.
     *
     * @param fieldName the field name
     * @param value the value
     * @param <TField> the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/gt $gt
     */
    public static <TField> Filter gte(final String fieldName, final TField value) {
        return new OperatorFilter<TField>("$gte", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the given field is less than or equal to the specified value.
     *
     * @param fieldName the field name
     * @param value the value
     * @param <TField> the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/gt $gt
     */
    public static <TField> Filter lte(final String fieldName, final TField value) {
        return new OperatorFilter<TField>("$lte", fieldName, value);
    }

    private static final class OperatorFilter<TField> extends Filter {
        private final String operatorName;
        private final String fieldName;
        private final TField value;

        OperatorFilter(final String operatorName, final String fieldName, final TField value) {
            this.operatorName = notNull("operatorName", operatorName);
            this.fieldName = notNull("fieldName", fieldName);
            this.value = value;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <TDocument> BsonDocument render(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(fieldName);
            writer.writeStartDocument();
            writer.writeName(operatorName);
            ((Encoder) codecRegistry.get(value.getClass())).encode(writer, value, EncoderContext.builder().build());
            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }
    }

    private static class AndFilter extends Filter {
        private final Iterable<Filter> filters;

        public AndFilter(final Iterable<Filter> filters) {
            this.filters = filters;
        }

        @Override
        public <TDocument> BsonDocument render(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument andFilter = new BsonDocument();

            for (Filter filter : filters) {
                BsonDocument renderedFilter = filter.render(documentClass, codecRegistry);
                for (Map.Entry<String, BsonValue> element : renderedFilter.entrySet()) {
                    addClause(andFilter, element);
                }
            }

            return andFilter;
        }

        private void addClause(final BsonDocument document, final Map.Entry<String, BsonValue> clause) {
            if (clause.getKey().equals("$and")) {
                for (BsonValue value : clause.getValue().asArray()) {
                    for (Map.Entry<String, BsonValue> element : value.asDocument().entrySet()) {
                        addClause(document, element);
                    }
                }
            } else if (document.size() == 1 && document.keySet().iterator().next().equals("$and")) {
                document.get("$and").asArray().add(new BsonDocument(clause.getKey(), clause.getValue()));
            } else if (document.containsKey(clause.getKey())) {
                if (document.get(clause.getKey()).isDocument() && clause.getValue().isDocument()) {
                    BsonDocument existingClauseValue = document.get(clause.getKey()).asDocument();
                    BsonDocument clauseValue = clause.getValue().asDocument();
                    if (keysIntersect(clauseValue, existingClauseValue)) {
                        promoteFilterToDollarForm(document, clause);
                    } else {
                        existingClauseValue.putAll(clauseValue);
                    }
                } else {
                    promoteFilterToDollarForm(document, clause);
                }
            } else {
                document.append(clause.getKey(), clause.getValue());
            }
        }

        private boolean keysIntersect(final BsonDocument first, final BsonDocument second) {
            for (String name : first.keySet()) {
                if (second.containsKey(name)) {
                    return true;
                }
            }
            return false;
        }

        private void promoteFilterToDollarForm(final BsonDocument document, final Map.Entry<String, BsonValue> clause) {
            BsonArray clauses = new BsonArray();
            for (Map.Entry<String, BsonValue> queryElement : document.entrySet()) {
                clauses.add(new BsonDocument(queryElement.getKey(), queryElement.getValue()));
            }
            clauses.add(new BsonDocument(clause.getKey(), clause.getValue()));
            document.clear();
            document.put("$and", clauses);
        }
    }

    private static class OrFilter extends Filter {
        private final Iterable<Filter> filters;

        public OrFilter(final Iterable<Filter> filters) {
            this.filters = filters;
        }

        @Override
        public <TDocument> BsonDocument render(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument orFilter = new BsonDocument();

            BsonArray filtersArray = new BsonArray();
            for (Filter filter : filters) {
                filtersArray.add(filter.render(documentClass, codecRegistry));
            }

            orFilter.put("$or", filtersArray);

            return orFilter;
        }
    }
}
