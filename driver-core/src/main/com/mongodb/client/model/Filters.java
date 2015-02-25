package com.mongodb.client.model;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.Map;
import java.util.regex.Pattern;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;

/**
 * A factory for query filters. A convenient way to use this class is to statically import all of its methods, which allows usage like:
 * <blockquote><pre>
 *    collection.find(and(eq("x", 1), lt("y", 3)));
 * </pre></blockquote>
 * @since 3.0
 */
public final class Filters {

    private Filters() {
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
        return new SimpleEncodingFilter<TField>(fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of the field name does not equal the specified value.
     *
     * @param fieldName the field name
     * @param value     the value
     * @param <TField>  the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/ne $ne
     */
    public static <TField> Bson ne(final String fieldName, final TField value) {
        return new OperatorFilter<TField>("$ne", fieldName, value);
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
     * @mongodb.driver.manual reference/operator/query/lt $lt
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
     * @mongodb.driver.manual reference/operator/query/gte $gte
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
     * @mongodb.driver.manual reference/operator/query/lte $lte
     */
    public static <TField> Bson lte(final String fieldName, final TField value) {
        return new OperatorFilter<TField>("$lte", fieldName, value);
    }

    /**
     * Creates a filter that matches all documents where the value of a field equals any value in the list of specified values.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/in $in
     */
    public static <TItem> Bson in(final String fieldName, final TItem... values) {
        return in(fieldName, asList(values));
    }

    /**
     * Creates a filter that matches all documents where the value of a field equals any value in the list of specified values.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/in $in
     */
    public static <TItem> Bson in(final String fieldName, final Iterable<TItem> values) {
        return new ArrayOperatorFilter<TItem>("$in", fieldName, values);
    }

    /**
     * Creates a filter that matches all documents where the value of a field does not equal any of the specified values or does not exist.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/nin $nin
     */
    public static <TItem> Bson nin(final String fieldName, final TItem... values) {
        return nin(fieldName, asList(values));
    }

    /**
     * Creates a filter that matches all documents where the value of a field does not equal any of the specified values or does not exist.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/nin $nin
     */
    public static <TItem> Bson nin(final String fieldName, final Iterable<TItem> values) {
        return new ArrayOperatorFilter<TItem>("$nin", fieldName, values);
    }

    /**
     * Creates a filter that performs a logical AND of the provided list of filters.  Note that this will only generate a "$and"
     * operator if absolutely necessary, as the query language implicity ands together all the keys.  In other words, a query expression
     * like:
     *
     * <blockquote><pre>
     *    and(eq("x", 1), lt("y", 3))
     * </pre></blockquote>
     *
     * will generate a MongoDB query like:
     * <blockquote><pre>
     *    {x : 1, y : {$lt : 3}}
     * </pre></blockquote>
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Bson and(final Iterable<Bson> filters) {
        return new AndFilter(filters);
    }

    /**
     * Creates a filter that performs a logical AND of the provided list of filters.  Note that this will only generate a "$and"
     * operator if absolutely necessary, as the query language implicity ands together all the keys.  In other words, a query expression
     * like:
     *
     * <blockquote><pre>
     *    and(eq("x", 1), lt("y", 3))
     * </pre></blockquote>
     *
     * will generate a MongoDB query like:
     *
     * <blockquote><pre>
     *    {x : 1, y : {$lt : 3}}
     * </pre></blockquote>
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/and $and
     */
    public static Bson and(final Bson... filters) {
        return and(asList(filters));
    }

    /**
     * Creates a filter that preforms a logical OR of the provided list of filters.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/or $or
     */
    public static Bson or(final Iterable<Bson> filters) {
        return new OrFilter(filters);
    }

    /**
     * Creates a filter that preforms a logical OR of the provided list of filters.
     *
     * @param filters the list of filters to and together
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/or $or
     */
    public static Bson or(final Bson... filters) {
        return or(asList(filters));
    }

    // TODO: $not
    // TODO: $nor

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
     * @param exists    true to check for existence, false to check for absence
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/exists $exists
     */

    public static Bson exists(final String fieldName, final boolean exists) {
        return new OperatorFilter<BsonBoolean>("$exists", fieldName, BsonBoolean.valueOf(exists));
    }

    /**
     * Creates a filter that matches all documents where the value of the field is of the specified BSON type.
     *
     * @param fieldName the field name
     * @param type      the BSON type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/type $type
     */
    public static Bson type(final String fieldName, final BsonType type) {
        return new OperatorFilter<BsonInt32>("$type", fieldName, new BsonInt32(type.getValue()));
    }

    /**
     * Creates a filter that matches all documents where the value of a field divided by a divisor has the specified remainder (i.e. perform
     * a modulo operation to select documents).
     *
     * @param fieldName the field name
     * @param divisor   the modulus
     * @param remainder the remainder
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/mod $mod
     */
    public static Bson mod(final String fieldName, final long divisor, final long remainder) {
        return new OperatorFilter<BsonArray>("$mod", fieldName, new BsonArray(asList(new BsonInt64(divisor), new BsonInt64(remainder))));
    }

    /**
     * Creates a filter that matches all documents where the value of the field matches the given regular expression pattern with the given
     * options applied.
     *
     * @param fieldName the field name
     * @param pattern   the pattern
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/regex $regex
     */
    public static Bson regex(final String fieldName, final String pattern) {
        return regex(fieldName, pattern, null);
    }

    /**
     * Creates a filter that matches all documents where the value of the field matches the given regular expression pattern with the given
     * options applied.
     *
     * @param fieldName the field name
     * @param pattern   the pattern
     * @param options   the options
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/regex $regex
     */
    public static Bson regex(final String fieldName, final String pattern, final String options) {
        notNull("pattern", pattern);
        return new SimpleFilter(fieldName, new BsonRegularExpression(pattern, options));
    }

    /**
     * Creates a filter that matches all documents where the value of the field matches the given regular expression pattern with the given
     * options applied.
     *
     * @param fieldName the field name
     * @param pattern   the pattern
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/regex $regex
     */
    public static Bson regex(final String fieldName, final Pattern pattern) {
        notNull("pattern", pattern);
        return new SimpleEncodingFilter<Pattern>(fieldName, pattern);
    }

    /**
     * Creates a filter that matches all documents matching the given search term.
     *
     * @param search the search term
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/text $text
     */
    public static Bson text(final String search) {
        notNull("search", search);
        return text(search, null);
    }

    /**
     * Creates a filter that matches all documents matching the given search term using the given language.
     *
     * @param search   the search term
     * @param language the language to use for stop words
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/text $text
     */
    public static Bson text(final String search, final String language) {
        notNull("search", search);
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                BsonDocument searchDocument = new BsonDocument("$search", new BsonString(search));
                if (language != null) {
                    searchDocument.put("$language", new BsonString(language));
                }
                return new BsonDocument("$text", searchDocument);
            }
        };
    }

    /**
     * Creates a filter that matches all documents for which the given expression is true.
     *
     * @param javaScriptExpression the JavaScript expression
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/where $where
     */
    public static Bson where(final String javaScriptExpression) {
        notNull("javaScriptExpression", javaScriptExpression);
        return new BsonDocument("$where", new BsonString(javaScriptExpression));
    }

    /**
     * Creates a filter that matches all documents where the value of a field is an array that contains all the specified values.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/all $all
     */
    public static <TItem> Bson all(final String fieldName, final TItem... values) {
        return all(fieldName, asList(values));
    }

    /**
     * Creates a filter that matches all documents where the value of a field is an array that contains all the specified values.
     *
     * @param fieldName the field name
     * @param values    the list of values
     * @param <TItem>   the value type
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/all $all
     */
    public static <TItem> Bson all(final String fieldName, final Iterable<TItem> values) {
        return new ArrayOperatorFilter<TItem>("$all", fieldName, values);
    }

    /**
     * Creates a filter that matches all documents containing a field that is an array where at least one member of the array matches the
     * given filter.
     *
     * @param fieldName the field name
     * @param filter    the filter to apply to each element
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/elemMatch $elemMatch
     */
    public static Bson elemMatch(final String fieldName, final Bson filter) {
        return new Bson() {
            @Override
            public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
                return new BsonDocument(fieldName, new BsonDocument("$elemMatch", filter.toBsonDocument(documentClass, codecRegistry)));
            }
        };
    }

    /**
     * Creates a filter that matches all documents where the value of a field is an array of the specified size.
     *
     * @param fieldName the field name
     * @param size      the size of the array
     * @return the filter
     * @mongodb.driver.manual reference/operator/query/size $size
     */
    public static Bson size(final String fieldName, final int size) {
        return new OperatorFilter<Integer>("$size", fieldName, size);
    }

    // TODO: $geoWithin
    // TODO: $geoIntersects
    // TODO: $near
    // TODO: $nearSphere

    private static final class SimpleFilter implements Bson {
        private final String fieldName;
        private final BsonValue value;

        private SimpleFilter(final String fieldName, final BsonValue value) {
            this.fieldName = notNull("fieldName", fieldName);
            this.value = notNull("value", value);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            return new BsonDocument(fieldName, value);
        }
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
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(fieldName);
            writer.writeStartDocument();
            writer.writeName(operatorName);
            encodeValue(writer, value, codecRegistry);
            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }
    }

    private static class AndFilter implements Bson {
        private final Iterable<Bson> filters;

        public AndFilter(final Iterable<Bson> filters) {
            this.filters = notNull("filters", filters);
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
            this.filters = notNull("filters", filters);
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

    private static class ArrayOperatorFilter<TItem> implements Bson {
        private final String operatorName;
        private final String fieldName;
        private final Iterable<TItem> values;

        ArrayOperatorFilter(final String operatorName, final String fieldName, final Iterable<TItem> values) {
            this.operatorName = notNull("operatorName", operatorName);
            this.fieldName = notNull("fieldName", fieldName);
            this.values = notNull("values", values);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            writer.writeStartDocument();
            writer.writeName(fieldName);
            writer.writeStartDocument();
            writer.writeName(operatorName);
            writer.writeStartArray();
            for (TItem value : values) {
                encodeValue(writer, value, codecRegistry);
            }
            writer.writeEndArray();
            writer.writeEndDocument();
            writer.writeEndDocument();

            return writer.getDocument();
        }
    }

    private static class SimpleEncodingFilter<TItem> implements Bson {
        private final String fieldName;
        private final TItem value;

        public SimpleEncodingFilter(final String fieldName, final TItem value) {
            this.fieldName = notNull("fieldName", fieldName);
            this.value = value;
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

            writer.writeStartDocument();
            writer.writeName(fieldName);
            encodeValue(writer, value, codecRegistry);
            writer.writeEndDocument();

            return writer.getDocument();
        }
    }

    @SuppressWarnings("unchecked")
    private static <TItem> void encodeValue(final BsonDocumentWriter writer, final TItem value, final CodecRegistry codecRegistry) {
        if (value == null) {
            writer.writeNull();
        } else {
            ((Encoder) codecRegistry.get(value.getClass())).encode(writer, value, EncoderContext.builder().build());
        }
    }

    // WIP, to be used later by not/nor methods
    private static class NotFilter implements Bson {
        private final Bson filter;

        public NotFilter(final Bson filter) {
            this.filter = notNull("filter", filter);
        }

        @Override
        public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry) {
            BsonDocument filterDocument = filter.toBsonDocument(documentClass, codecRegistry);
            if (filterDocument.size() == 1) {
                return negateSingleElementFilter(filterDocument);
            } else {
                return negateArbitraryFilter(filterDocument);
            }
        }

        private BsonDocument negateSingleElementFilter(final BsonDocument filterDocument) {
            String name = filterDocument.keySet().iterator().next();
            BsonValue value = filterDocument.get(name);

            if (name.equals("$")) {
                return negateSingleElementTopLevelOperatorFilter(filterDocument, name, value);
            }

            if (value.isDocument()) {
                // TODO: figure out what to do here
            }

            if (value.isRegularExpression()) {
                return new BsonDocument(name, new BsonDocument("$not", value));
            }

            return new BsonDocument(name, new BsonDocument("$ne", value));
        }

        private BsonDocument negateArbitraryFilter(final BsonDocument filterDocument) {
            // $not only works as a meta operator on a single operator so simulate using $nor
            return new BsonDocument("$nor", new BsonArray(asList(filterDocument)));
        }

        BsonDocument negateSingleElementTopLevelOperatorFilter(final BsonDocument filterDocument, final String name,
                                                               final BsonValue value) {
            if (name.equals("$or")) {
                return new BsonDocument("$nor", value);
            } else if (name.equals("$nor")) {
                return new BsonDocument("$or", value);
            } else {
                return negateArbitraryFilter(filterDocument);
            }
        }

    }
}
