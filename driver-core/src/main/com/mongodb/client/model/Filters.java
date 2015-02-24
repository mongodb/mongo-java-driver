package com.mongodb.client.model;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonValue;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * A factory for query filters. A convenient way to use this class is to statically import all of its methods, which allows usage like:
 * <p><blockquote><pre>
 *    collection.find(and(eq("x", 1), lt("y", 3)));
 * </p></pre></blockquote>
 * @since 3.0
 */
public final class Filters {

    private Filters() {
    }

    /**
     * Creates a filter that ands together the provided list of filters.  Note that this will only generate a "$and" operator if absolutely
     * necessary, as the query language implicity ands together all the keys.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Bson and(final Iterable<Bson> filters) {
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
    public static Bson and(final Bson... filters) {
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
    public static Bson or(final Iterable<Bson> filters) {
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
    public static Bson or(final Bson... filters) {
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
    public static <TField> Bson eq(final String fieldName, final TField value) {
        return new Bson() {
            @Override
            @SuppressWarnings("unchecked")
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
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
    public static Bson exists(final String fieldName) {
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

    public static Bson exists(final String fieldName, final boolean exists) {
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
    public static <TField> Bson gt(final String fieldName, final TField value) {

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
    public static <TField> Bson lt(final String fieldName, final TField value) {
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
    public static <TField> Bson gte(final String fieldName, final TField value) {
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
    public static <TField> Bson lte(final String fieldName, final TField value) {
        return new OperatorFilter<TField>("$lte", fieldName, value);
    }

    private static final class OperatorFilter<TField> implements Bson {
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
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
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

    private static class AndFilter implements Bson {
        private final Iterable<Bson> filters;

        public AndFilter(final Iterable<Bson> filters) {
            this.filters = filters;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument andRenderable = new BsonDocument();

            for (Bson filter : filters) {
                BsonDocument renderedRenderable = filter.toBsonDocument(documentClass, codecRegistry);
                for (Map.Entry<String, BsonValue> element : renderedRenderable.entrySet()) {
                    addClause(andRenderable, element);
                }
            }

            return andRenderable;
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
                        promoteRenderableToDollarForm(document, clause);
                    } else {
                        existingClauseValue.putAll(clauseValue);
                    }
                } else {
                    promoteRenderableToDollarForm(document, clause);
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

        private void promoteRenderableToDollarForm(final BsonDocument document, final Map.Entry<String, BsonValue> clause) {
            BsonArray clauses = new BsonArray();
            for (Map.Entry<String, BsonValue> queryElement : document.entrySet()) {
                clauses.add(new BsonDocument(queryElement.getKey(), queryElement.getValue()));
            }
            clauses.add(new BsonDocument(clause.getKey(), clause.getValue()));
            document.clear();
            document.put("$and", clauses);
        }
    }

    private static class OrFilter implements Bson {
        private final Iterable<Bson> filters;

        public OrFilter(final Iterable<Bson> filters) {
            this.filters = filters;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument orRenderable = new BsonDocument();

            BsonArray filtersArray = new BsonArray();
            for (Bson filter : filters) {
                filtersArray.add(filter.toBsonDocument(documentClass, codecRegistry));
            }

            orRenderable.put("$or", filtersArray);

            return orRenderable;
        }
    }
}
