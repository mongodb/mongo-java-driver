package com.mongodb.client.model.expressions;

// TODO: should this be an interface at all?
// TODO: Is it weird to chain required arguments? Is it weird to chain optional arguments?
// TODO: Easier to read, but easier to forget as well
//   convert(path("x.y").to("string").onError(literal(null))
//   convert(path("x.y"), "string").onError(literal(null))
//   convert(path("x.y"), "string", literal(null))
public interface ConvertExpression extends Expression {

    ConvertExpression onError(Expression value);

    ConvertExpression onNull(Expression value);
}
