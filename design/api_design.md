
# Introduction

Users experience a product through its API. The API informs a user’s mental model, determines legibility of code, and ease of writing. It serves as an external contract, so it is difficult to change.

# Principles

An API should be:

* User-centred. Consider novice and experienced users. The API should be used, in tests and elsewhere, in the manner we expect the user to use it.  
* Consistent. At a macro level, represent the same things in the same way across the API. At a micro level, syntax should match semantics: use the same constructs to represent the same concepts. At a meta level, apply a small and clear set of rules confidently and consistently, even if this leads to suboptimal results in some cases.
* Boring. Idiomatic. Avoid novelty, focus on established structures and patterns, common to the language, and to languages in general. Tricks and complexity must be well-justified.
* Extensible. It must allow for future changes and additions.
* Safe. It should prevent user error, through its wording, structure, or the use of types.

We spend more time reading than writing, but we spend more time thinking than reading. Problems in a conceptual model are substantially worse than a readability issue.

* Comprehensibility above readability. Consider the number and complexity of distinct concepts, or types of thing, and how the API can be used to distinguish them. Choose structural or conceptual clarity over an API that reads like natural language. 
* Readability over writability. Keep it terse, and avoid boilerplate. This is increasingly important as our need to review AI generated code increases.
* Do not neglect writability, including tool use like autocomplete, and also AI tools.

Specific guidance:

* Show sequenced operations in a natural reading order. Prefer chaining to nesting.
* Show sub-structured operations or declarations through nesting (including lambda-initiated chains). 
* Avoid state. Prefer immutability. Make procedural code look data-driven and declarative, without destroying procedural reading order where present. 
* Be explicit. Avoid implicit operations or defaults.
* Do not rely on documentation. Ensure that the API is clear and usable, as much as possible, without docs. Then write great docs.
* When tracking another API, adhere to its functionality and concepts, but do not repeat its defects and inconsistencies.
* Prefer simplicity in the API. Move complexity, including configuration, to a layer encapsulated by the API.

# Terms and Distinctions

The following sections introduce concepts and terminology.

## Syntactic Flow

Syntactic flow refers to the syntactic structures of a language or an API used to express operations or relations in a way that matches the way we see or think about things. Examples:

```
(a + 1) * 2          -- infix operator
* + a 1 2            -- prefix operator
(mult (add a 1) 2)   -- lisp-like functional
mult(add(a, 1), 2)   -- nested functional
a.plus(1).times(2)   -- chaining (fluent, DSL)
var n = a.plus(1);
var r = n.times(2);  -- imperative
a.pipe([
  add(1),
  mult(2)  ]);       -- list-like
```

Some of these styles are preferable for expressing particular constructs, since they better-match how we think about them. This is a matter of:

* Brevity. Some things are much easier to understand at a glance. Infix operators demonstrate this, versus approaches that require multiple lines.
* Consistency and focus. If `a` and `1` are equally important and functionally identical, there is no reason to express one differently from the other, as is done in the chaining and especially pipeline examples. However, if adding and multiplying are something "done to" `a`, then we should prefer to highlight this by using chaining.
* Visual closeness. We prefer to have the `*` close to the `2` that it applies to. This is part of why the infix style is less readable.
* Sequential order. Nested approaches often violate this: `add` is a verb, so there is an implied order of action. However, we must deduce this order by glancing from place to place rather than having it naturally presented to us.
* Structural coherence. The simplest structure is a sequence, and it can be used to express the ordering of action just mentioned. It can express other things, like importance (in fields or parameters), data flow, and so on. We can also express tree-like structures: simple trees using infix operators, and complex trees using multiple lines and indentation. Directed acyclic graphs tend to require the imperative style for expressing data and procedural flow. Unconstrained graphs are more difficult to think about and express, and we tend to avoid them.

The above example code is simple. Some constructs that apply in simple cases break down in complex cases, while others improve. Note, for example, that chaining is not limited to linear sequences, and works well for structured DSLs.

```
form.validator()
    .section("reservation", s -> s
        .stringField("name", f -> f.length(2, 20))
        .dateField("date", f -> f.future())
        .stringField("comment", f -> f.optional().length(0, 100)));
```

In cases where there might be excessive or careful boilerplate in a nested functional or imperative approach, chaining is often a better fit.

In practice, we focus on the **imperative** and **chaining** styles. Operators are terse, but very limited. Functional styles violate order and introduce substantial nesting. The imperative style matches the sequential way we tend to think about things, and chaining narrows the imperative style, often making its structure more clear.

### Initiating and terminating flow

There are patterns for initiating flow:

```
new WriteConcern(2)            -- constructor
ReadPreference.secondary()     -- static factory method
MongoClientSettings.builder()  -- static factory method
myList.stream()...             -- factory method
WriteConcern.MAJORITY          -- immutable static constant
myEntity.operation(b -> b...)  -- lambda (provides b, from myEntiy)
```

Patterns for terminating flow:

```
MongoClients.create(settings)  -- create method taking a settings object
settingsBuilder.build()        -- builder build method; terminal operations
```

In many cases, flow does not explicitly terminate.

```
a.plus(1)
collection.map(a -> a + 1)
something.withValue(2)
something.value(2)
```

Configuration examples:

```
Client c = createClient().withTimeout(x);
Client c = createClient( ClientSettings.create().withTimeout(x) );
Client c = createClient( ClientSettings.builder().timeout(x).build() );
```

The first has no builder or settings, the second has ClientSettings, while the third has a ClientSettingsBuilder. The builder pattern is not strictly necessary (even validation can be on-use, rather than on-build).

## Operations

An **entity** is a thing, and an **operation** is something that can be done with a thing. A value is an entity that never changes. Some operations, like addition, are simple. They require only two values as inputs, and yield a third value. Other operations require many more inputs.

Operations are considered generally, and do not correspond to methods. An object could represent an operation, and, less frequently, multiple objects could represent a single conceptual entity.

### Subjects

The **subject** of an operation is the entity we do something to.

```
substring("hence", 0, 3)
"hence".substring(0, 3)
```

The string is the subject. The numbers are what we will call **directives**, they direct how the operation is to be performed. The object-oriented approach adheres to the above principle of "consistency and focus" by using syntax to distinguish the subject. This syntactic style strongly encourages a single subject, which is the object that a method is called on. However, there can be multiple subjects. For example:

```
"hen".concat("ce")
```

We concatenate two items, and there is no particular reason (except English reading order) to treat one as the proper subject - both strings are the subjects of the operation. Most operations that have more than two subjects can be reduced to binary operations (add, multiply, and, or, and other monoids), or equivalently to an operations taking a list. But not all. For example:

```
cond(test, thenValue, elseValue)
blendColors(r, g, b)
```

Such operations are rare, but are difficult to model consistently in an object-oriented style, and tend to appear unintuitive. In some cases, it may be possible to re-formulate the entities to allow for a binary operation - for example, ColorChannel instances might blend via binary operations, rather than the three-subject operation above. Some form of symmetry is usually a good indication that this is possible

### Domain values

Some values form natural groupings, and can be represented together as a **domain value**. For example, if an operation specifies a first and last position, these might form a Range. Three integers representing a red, green, and blue could form a Color.

Nominal wrappers can be used to wrap even a single primitive value. This practice makes code safer, but it comes with overhead, and is typically reserved for types that cross boundaries. For example, if a particular concept is used only within a simple method, it is obviously not worthwhile to create a wrapper for that type.

While such types are useful internally, they can complicate an API. If the nominal type or domain value is not used extensively outside the API, then we must create those values at the API later. Compare:

```
"hence".substring(0, 3)
"hence".substring(range(0, 3))
"hence".substring(range(0, 3))
```

Although `range` is a coherent domain value, it is not used outside the API, so we should prefer to represent it using non-domain parameters, often primitives. If a type is expected to already be created before it is provided as an argument, then we should directly use that type in the API. For example:

```
diagram.shift(x, y); // inconvenient, if using Vec2 outside of the API
diagram.shift(delta); // delta is Vec2
```


### Configuration

When discussing the subjects of operations, we have considered simple operations on values. As we turn to complex operations, we find that inputs become more numerous and complex. Suppose that we need to send a message. This operation may need to specify the message itself, the details of the destination, the format, how it should be sent, timeouts, and so on. The inputs are numerous.

There are 3 sources for these inputs: locations in code, application settings, and user data. These correspond to ownership and control: code is owned by developers, settings by the system operator, and user data by users. This control flows down: the developer can exercise control over the scope of user data, but the opposite must not be the case. There are also hard boundaries on making changes to those scopes: recompilation, and application restarts.

Inputs tend to form natural groupings. These groupings might suggest certain entities or composite domain values. Nested entities might have different values and overrides for particular inputs.

**Configuration** encompasses those inputs that govern how an operation is done. Configuration is often intentionally concealed in various entities supplied to an operation. These inputs are, in a sense, often irrelevant to the semantics of the operation itself. For example, whether a list has been configured to be an array or a linked list is often irrelevant.

### Summary

* **Operation** - what is done
* **Subject** - what an operation is done to
* **Directive** - what is done, more exactly
* **Configuration** - how it is done

The subject and directives are the **parameterization** of an operation. For example, in `"hence".substring(0, 3)`, `"hence"` is the subject and part of what we are calling the parameterization.

In an object-oriented language, in the simple ideal case, configuration is provided through the constructor, the subject is the object entity, the operation is the method, and the parameters/arguments (depending on if you are declaring or calling) are the directives.

## Guidance

1. Strongly prefer chaining to nested static functions. Nesting is difficult to read, and static functions can have conflicts and often introduce substantial boilerplate.
1. Classify the inputs as Subject, Directive, or Configuration. In some cases, this is obvious, but in others, alternatives should be considered or left open. 
2. Group inputs together. This might suggest composite entities, new subjects, composite directives. For example: range, coordinate, name, address. However, prefer primitives over domain values unless the domain value is already used extensively outside the API (see Domain values above).
3. Consider moving values into configuration, and carefully consider the structure and hierarchy of the entities involved.
4. Choose a good default, instead of letting the user configure it. (For example, the Java Streams API uses ArrayList by default.)
5. Eliminate optional parameters.

### Eliminating Optional Parameters

Optional parameters can often be removed, and very few are truly optional. They often arise when defaults or implicit values are used, when different operations are conflated into a single construct, or when higher-level configuration "leaks" into the operation.

* **Set defaults explicitly**. Implicit behaviour should be avoided, since it leads to confusion and errors. Consider: any required parameter can be made optional by giving it a convenient default. Defaults should be used rarely, such as when they save substantial boilerplate in very common operations.
* **Beware of booleans**. Booleans are sometimes used to represent alternate behaviours of a method. It may be better to split the method into two methods, especially when the alternate behaviour is rare. For example, `substr(useBytes, ...)` can be split into `substr` and `substrBytes`. The same applies to enums. Be especially wary when different values imply different sets of required parameters.
* **Omit flags**. If an optional is intended to enable functionality, consider removing the choice and always defaulting to some value. If it is truly unavoidable, consider placing this as a configuration flag on some higher level entity, or even at the level of application configuration.
* **Split out convenience methods and composite operations**. Optional parameters sometimes represent a secondary operation. Attempt to represent these instead using the same programming construct as an ensuing or preceding operation, or if there is an existing operation that performs the same task, just use it instead. An example might be a pipeline stage that has a "thenFilter" option, which should be removed, if an ensuing "filter" stage can be used instead.
* **Extract configuration**. Some entities need to be configured in some way. Entity configuration often requires sane defaults, flags, and so on. Certain operation-level options can be moved into entity configuration. If an optional does not naturally move into a higher-level entity, consider whether it belongs in a different entity. Remove optional parameters by having operations accept other entities as parameters.

The above suggestions might not eliminate all optional parameters, or there might be other obstacles to removal.

### Representing Optional Parameters

Directive parameters might be optional, sometimes based on the values of other parameters. There are various ways to deal with optional inputs, when they must be included.

Below, the items prefixed 0 are basic, generally built into the language. The items prefixed 1 offer some form of builder for all parameters. The items prefixed 2 place required fields at the method level, and use some form of builder for optional parameters.

**0.0 Flat methods and overloads**: overloads for all variations. While viable in simple cases, this is generally unsustainable as it results in an overload explosion when new options are added.
```java
densify("field", 10)
densify("field", 0, 3, 1, "p")
```

**0.1 Nested values (domain values)**: some coherent grouping moves to a parameter, which can be a polymorphic domain value. It is important to ensure that the domain value is itself a coherent type of thing, and independent of the particular operation. They should not be used as mere labels. Prefer primitives unless the domain value is already used extensively outside the API (see Domain values above).
```java
densify("field", step(10))
densify("field", rangeStep(0,3,1), "p")
move(xyDelta(1,20))
```

**1.0 Chained Options, initiated on required parameters**:

All required parameters are included in the initiation:
```java
densify(  densifySpec("field", ...)  ); -- no optionals
densify(  densifySpec("field", ...).partitionByFields("q")  );
```

**1.1 Chained, initiated on first required**:

First parameter starts the chain, required may be chained:
```java
densify(densifyField("field").step(10));
densify(densifyField("field").rangeStep(0,3,1).partitionByFields("q"));
```

Note that nested values can be combined with this approach:
```java
densify(densifyField("field").range(step(10)));
densify(densifyField("field").range(rangeStep(0,3,1)).partitionByFields("q"));
```

**1.2 Fluent Operation**: the operation starts the chain
```java
densify().field("field").step(10);
densify().field("field").rangeStep(0,3,1).partitionByFields("q");
```
In the above example, the operation either cannot be chained, or must have a mandatory terminal method.

**1.3 Builders**: All parameters are chained off of a provider method:
```java
densify(densifySpec().field("field").range(step(10)));
densify(densifySpec().field("field").range(rangeStep(0,3,1)).partitionByFields("q"));
```
The builder may use an explicit build method.

**2.0 Fluent Optionals**: the chain begins at optional parameters:
```java
densify("field", step(10))
densify("field", rangeStep(0,3,1), Densify.options().partitionByFields("q") )
densify("field", rangeStep(0,3,1), opts -> opts.partitionByFields("q") )
densify("field", rangeStep(0,3,1), Densify.OPTIONS.partitionByFields("q") )
densify("field", rangeStep(0,3,1), ... .partitionByFields("q").build() )
```
There are multiple variations. There could be overloads on just the options resulting in a more-localized "explosion".

**2.1 Varargs**: use varargs to handle the optional (variable) arguments
```java
densify("field", step(10))
densify("field", rangeStep(0,3,1), DensifyOption.partitionByFields("q"))
Signature:
densify(..., DensifyOption... options)
```

## Guidance for represeting Optional parameters

1. Use 1.3 Builders for entity configuration, where optionals are unavoidable
2. If optionals cannot be eliminated at the operation level, use 2.1 Varargs. 
3. Options and builders should never be shared across operations. Either the operations are not distinct, or they are, and their options will diverge.
3. Lambdas are a familiar and powerful pattern. A lambda will provide the type for auto-complete. In some cases, a lambda is required for type safety. However, lambdas tend to increase complexity.
4. Place subjects on the left side, whenever possible, with directive parameters to the right, and required directive parameters on the left, and optional directives on the right.

# Practical examples

We consider and evaluate concrete examples.

## Java SE: Streams

Java's Stream API is a sound and canonical design example. It uses initiating and terminal operations. However, consider the following:

```java
var out = items.stream()  
   .filter(this::keep)  
   .parallel()           // can apply to whole stream, earlier and later
   .map(this::stage2)  
   .toList();
```

Concerns:
1. Configuration is mixed into the stream of operations. A user might expect that only ensuing operations are parallel.

As a positive, it nicely uses a `stream` and `parallelStream` for initiation, rather than a boolean configuration argument `parallel` (eliminates boolean options).

## Driver: find

```java
FindIterable<Document> results = collection
   .find(Filters.eq("status", "active"))
   .projection(Projections.include("name"))
   .sort(Sorts.descending("age"))
   .limit(10)
   .collation(Collation.builder().locale("en").build()) // configuration — how strings are compared
   .maxTime(5, SECONDS);                                // configuration — execution time limit
```

Concerns:
1. Configuration is mixed into the pipeline
2. The initiating "find" takes in the filter parameter, rather than configuration. Consider why Java SE does not accept a filter in its initiating method `stream(a -> a.isEven())`.
3. The methods limit and sort ignore order. No distinction is made between configuration and the declaration of operations (and their parameterizations).

This could be improved by introducing a find that returns an iterable, placing configuration into an immutable findSettings accepted by this creation method (or, elsewhere), introducing a filter stage, and using types or exceptions to block the ordering issue. This would make the find method look much more like an aggregation pipeline.

Worth doing? No.

## Java SE: HttpClient

```java
HttpClient client = HttpClient.newBuilder()  
   .version(HTTP_2)  
   .connectTimeout(Duration.ofSeconds(5))  
   .executor(exec)  
   .followRedirects(Redirect.NORMAL)  
   .build();

var req1 = HttpRequest.newBuilder()  
   .uri(URI.create("https://api.example.com/users"))  
   .header("Authorization", "Bearer " + token)  
   .header("Accept", "application/json")  
   .GET()                                            // directive
   .build();

var req2 = HttpRequest.newBuilder()  
   .uri(URI.create("https://api.example.com/users"))  
   .header("Authorization", "Bearer " + token)  
   .POST(BodyPublishers.ofString(json))              // directive  
   .build();

client.sendAsync(req1, BodyHandlers.ofString());  
client.sendAsync(req2, BodyHandlers.ofString());
```
The client is created via a builder, and so are the requests. The client's `sendAsync` method uses the client, and the request entity. The request entity mixes configuration with directives, but is an example of moving options that could have existed in the `sendAsync` method into an entity. The `BodyHandler` was not moved.

Concerns:
1. The builder has optional boilerplate, with varying "directives".
2. This API closely tracks the REST API, without adjustment. (It may be a design goal to exactly track the original API.)

Note the similarity to `MongoClient`, though here, the "Collection" (target resource) is specified as a URI rather than as a separate entity. Perhaps the host and authentication could be specified at the client level. Specific requests could be initiated from resources.


## Driver: Mql Expressions API 

Compare with Stream:
```java
numList.stream()
       .filter(n -> n.mod(valueOf(2)).equals(ZERO))
       .map(n -> n.multiply(TEN))
       .reduce(ZERO, (a, b) -> b.add(a))
```

Equivalent code in the MQL Expressions API:

```java
fieldArrInt("numList")
       .filter(n -> n.mod(2).eq(0))
       .map(n -> n.multiply(10))
       .reduce(of(0), (a, b) -> b.add(a))
```

The MQL expressions API design uses chaining, including chaining of subjects. Reduce operations are carried out on typed lists, for the same reason, and each one is a named method (sum, concat, and so on). Only initial values use static methods, and there, they are limited to `of`. It uses lambdas for certain stages, like the Stream API.

Concerns:
1. We can include overrides for methods that can take `of` in certain locations where this has not already been done - though not all.
2. The API is not properly integrated into the driver's aggregate API.



