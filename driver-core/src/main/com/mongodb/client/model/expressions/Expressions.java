package com.mongodb.client.model.expressions;

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.Decimal128;

import java.util.Date;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Builders for aggregate expressions.
 * 
 * @mongodb.driver.manual reference/operator/aggregation/
 */
public final class Expressions {

    // TODO: using lambda for simple Expression types is convenient, but it means there is no good toString or equals/hashCode support

    //
    // Accumulator expressions
    //

    /**
     * An expression that computes the average value of the numeric values. $avg ignores non-numeric values.
     *
     * @param value an expression that evaluates to an array of numerical values
     * @return a $avg expression
     * @mongodb.driver.manual reference/operator/aggregation/#accumulators-in-other-stages
     * @mongodb.driver.manual reference/operator/aggregation/avg/
     */
    public static Expression avg(final Expression value) {
        return new SingleValueExpression("$avg", value);
    }

    // TODO: alternatively, avg(List<Expression> values).  But there has to be a way to disambiguate a single value avg
    public static Expression avgOfList(final Expression... values) {
        return new ArrayValueExpression("$avg", asList(values));
    }

    //
    // Arithmetic expressions
    //

    /**
     * An expression that computes the absolute value of a number.
     *
     * @param value an expression that evaluates to an array of numerical values
     * @return a $avg expression
     * @mongodb.driver.manual reference/operator/aggregation/#arithmetic-expression-operators
     * @mongodb.driver.manual reference/operator/aggregation/abs/
     */
    public static Expression abs(Expression value) {
        return new SingleValueExpression("$abs", value);
    }

    public static Expression add(Expression...values) {
        return new ArrayValueExpression("$add", asList(values));
    }

    public static Expression multiply(Expression...values) {
        return new ArrayValueExpression("$multiply", asList(values));
    }

    //
    //  Array expressions
    //

    public static Expression arrayElementAt(final Expression array, final Expression idx) {
        return new ArrayValueExpression("$arrayElemAt", asList(array, idx));
    }

    public static Expression in(final Expression value, final Expression array) {
        return new ArrayValueExpression("$in", asList(value, array));
    }

    //
    //  Boolean expressions
    //

    public static Expression and(final Expression... values) {
        return new ArrayValueExpression("$and", asList(values));
    }

    public static Expression or(final Expression... values) {
        return new ArrayValueExpression("$or", asList(values));
    }

    public static Expression not(final Expression values) {
        return new ArrayValueExpression("$not", singletonList(values));
    }

    //
    //  Comparison expressions
    //

    public static Expression gte(final Expression first, final Expression second) {
        return new ArrayValueExpression("$gte", asList(first, second));
    }

    //
    //  Conditional expressions
    //

    public static Expression cond(final Expression condition, final Expression trueCase, final Expression falseCase) {
        return new ArrayValueExpression("$cond", asList(condition, trueCase, falseCase));
    }

    //
    //  Literal expressions
    //  TODO: should there be a way to create a literal expression that is _not_ wrapped in $literal?
    //

    public static Expression literal(final boolean value) {
        return codecRegistry -> new BsonDocument("$literal", BsonBoolean.valueOf(value));
    }

    public static Expression literal(final int value) {
        return codecRegistry -> new BsonDocument("$literal", new BsonInt32(value));
    }

    public static Expression literal(final long value) {
        return codecRegistry -> new BsonDocument("$literal", new BsonInt64(value));
    }

    public static Expression literal(final Date value) {
        return codecRegistry -> new BsonDocument("$literal", new BsonDateTime(value.getTime()));
    }

    public static Expression literal(final String value) {
        return codecRegistry -> new BsonDocument("$literal", new BsonString(value));
    }

    public static Expression literal(final double value) {
        return codecRegistry -> new BsonDocument("$literal", new BsonDouble(value));
    }

    public static Expression literal(final Decimal128 value) {
        return codecRegistry -> new BsonDocument("$literal", new BsonDecimal128(value));
    }

    public static Expression literal(final BsonValue value) {
        return codecRegistry -> new BsonDocument("$literal", value);
    }

    // TODO: is this a good escape hatch to offer?  I don't see how we can do without this.  Otherwise, there is no way to encode a literal
    // that is not an already-supported type
    @SuppressWarnings("unchecked")
    public static <T> Expression literal(final T value) {
        return codecRegistry -> {
            BsonDocument wrapper = new BsonDocument();
            BsonDocumentWriter writer = new BsonDocumentWriter(wrapper);
            writer.writeStartDocument();
            writer.writeName("$literal");
            if (value == null) {
                writer.writeNull();
            } else {
                Codec<T> codec = (Codec<T>) codecRegistry.get(value.getClass());
                codec.encode(writer, value, EncoderContext.builder().build());
            }
            return wrapper;
        };
    }

    //
    //  Path expressions
    //

    public static Expression field(final String path) {
        return (CodecRegistry codecRegistry) -> new BsonString("$" + path);
    }

    public static Expression currentRef() {
        return ref("CURRENT");
    }

    public static Expression currentRef(final String path) {
        return ref("CURRENT", path);
    }

    public static Expression rootRef() {
        return ref("ROOT");
    }

    public static Expression rootRef(final String path) {
        return ref("ROOT", path);
    }

    public static Expression thisRef() {
        return ref("this");
    }

    public static Expression thisRef(final String path) {
        return ref("this", path);
    }

    public static Expression ref(final String name) {
        return codecRegistry -> new BsonString("$$" + name);
    }

    public static Expression ref(final String name, final String path) {
        return codecRegistry -> new BsonString("$$" + name + "." + path);
    }

    //
    //  Type expressions
    //

    public static Expression toBool(final Expression input) {
        return new SingleValueExpression("$toBool", input);
    }

    public static ConvertExpression convert(final Expression input, final Expression to) {
        return new ConvertExpressionImpl(input, to, null, null);
    }

    //
    //  Variable expressions
    //

    public static Expression let(final Map<String, Expression> variables, final Expression in) {
        return codecRegistry -> {
            BsonDocument variablesDocument = new BsonDocument();
            variables.forEach((key, value) -> variablesDocument.append(key, value.toBsonValue(codecRegistry)));
            return new BsonDocument("$let",
                    new BsonDocument("vars", variablesDocument).append("in", in.toBsonValue(codecRegistry)));
        };
    }
}
