+++
date = "2015-03-19T14:27:51-04:00"
title = "Codec and CodecRegistry"
[menu.main]
  parent = "BSON"
  weight = 40
  pre = "<i class='fa'></i>"
+++

## Codec and CodecRegistry

In the last section we saw how to use the [`BsonReader`]({{< apiref "org/bson/BsonReader" >}}) and 
[`BsonWriter`]({{< apiref "org/bson/BsonWriter" >}}) API to read and write BSON documents.  But writing code at that 
low a level is tedious and error-prone, so in practice these algorithms are packaged in implementations of the 
[`Codec`]({{< apiref "org/bson/codecs/Codec" >}}) interface.

### Codec

The `Codec` interface abstracts the processes of decoding a BSON value into a Java object using a `BsonReader` and encoding a Java object
 into a BSON value using a `BsonWriter`.  The BSON value can be as simple as a boolean or as complex as a document or array.  
 
Consider the document that we saw in the last section:

```javascript
{
  "_id" : ObjectId("..."),
  "name": "Steve",
  "jobTitle": "Electrician",
  "dateStarted": "5/19/2016",
  "numberOfJobs": 5
}
```

Now consider the following Java class. We would like to decode BSON representation of above document into a Java object of 
the `Worker` class and vice versa.

```java
public class Worker {
    private ObjectId id;
    private String name;
    private String jobTitle;
    private Date dateStarted;
    private int numberOfJobs;

    public Worker(String name, String jobTitle, Date dateStarted, int numberOfJobs) {
        this(new ObjectId(), name, jobTitle, dateStarted, numberOfJobs);
    }

    public Worker(ObjectId id, String name, String jobTitle, Date dateStarted, int numberOfJobs) {
        this.id = id;
        this.name = name;
        this.jobTitle = jobTitle;
        this.dateStarted = dateStarted;
        this.numberOfJobs = numberOfJobs;
    }

    //
    // getters and setters
    //
}
```

Following `Codec` implementation encodes a `Worker` object into a BSON document, and vice versa:

```java
public class WorkerCodec implements Codec<Worker> {

    @Override
    public void encode(BsonWriter writer, Worker value, EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeObjectId("_id", value.getId());
        writer.writeString("name", value.getName());
        writer.writeString("jobTitle", value.getJobTitle());
        writer.writeDateTime("dateStarted", value.getDateStarted().getTime());
        writer.writeInt32("numberOfJobs", value.getNumberOfJobs());
        writer.writeEndDocument();
    }

    @Override
    public Worker decode(BsonReader reader, DecoderContext decoderContext) {
        reader.readStartDocument();
        ObjectId id = reader.readObjectId("_id");
        String name = reader.readString("name");
        String jobTitle = reader.readString("jobTitle");
        Date dateStarted = new Date(reader.readDateTime("dateStarted"));
        int numberOfJobs = reader.readInt32("numberOfJobs");
        reader.readEndDocument();
        return new Worker(id, name, jobTitle, dateStarted, numberOfJobs);
    }

    @Override
    public Class<Worker> getEncoderClass() {
        return Worker.class;
    }
}
```

### CodecRegistry

Now consider the following document that includes an array of testimonials for each `Worker`. We also saw this document in the last section. 
Each item in the testimonial array needs be decoded into a `Testimonial` object and vice versa. The `Testimonial` class and modified 
`Worker` class are shown below:

```java
public class Testimonial {

    private String authorEmail;
    private String comment;

    public Testimonial(String authorEmail, String  comment) {
        this.authorEmail = authorEmail;
        this.comment = comment;
    }

    ...

}
```

```java
public class Worker {
    private ObjectId id;
    private String name;
    private String jobTitle;
    private Date dateStarted;
    private int numberOfJobs;
    private List<Testimonial> testimonials;

    ...

}
```

`Codec` for encoding from and decoding to a `Testimonial` object would look like: 

```java
public class TestimonialCodec implements Codec<Testimonial> {

    @Override
    public void encode(BsonWriter writer, Testimonial value, EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeString("authorEmail", value.getAuthorEmail());
        writer.writeString("comment", value.getComment());
        writer.writeEndDocument();
    }

    @Override
    public Testimonial decode(BsonReader reader, DecoderContext decoderContext) {
        reader.readStartDocument();
        String authorEmail = reader.readString("authorEmail");
        String comment     = reader.readString("comment");
        reader.readEndDocument();
        return new Testimonial(authorEmail, comment);
    }

    @Override
    public Class<Testimonial> getEncoderClass() {
        return Testimonial.class;
    }
}
```

As we see, a `Codec` implementation that encodes from and decodes to the more complex `Worker` object is more complicated. Typically, more
complex objects rely on a set of `Codec` implementations for the embedded subtypes. For this, we can rely on a `CodecRegistry`.

A [`CodecRegistry`]({{< apiref "org/bson/codecs/configuration/CodecRegistry" >}}) contains a set of `Codec` instances that are accessed 
according to the Java classes that they encode from and decode to. Instances of `CodecRegistry` are generally created via static factory 
methods on the [`CodecRegistries`]({{< apiref "org/bson/codecs/configuration/CodecRegistries" >}}) class.  Consider the simplest of these 
methods, one that takes a list of `Codec`s:

```java
CodecRegistry registry = CodecRegistries.fromCodecs(new TestimonialCodec(), ...);
```

This returns an immutable `CodecRegistry` instance containing all the `Codec` instances passed to the `fromCodecs` method.  They can be
accessed within `WorkerCodec` like this:

```java
Codec<Testimonial> testimonialCodec = codecRegistry.get(Testimonial.class);
```

Since we are passing a `CodecRegistry` as a parameter,  we need an instance variable to hold `CodecRegistry`. We create a new constructor
that accepts a `CodecRegistry` parameter and sets the instance variable to this parameter's value.

Putting this all together, the modified `WorkerCodec` looks like:

```java
public class WorkerCodec implements Codec<Worker> {
    private final CodecRegistry codecRegistry;

    public WorkerCodec(CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    @Override
    public void encode(BsonWriter writer, Worker value, EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeObjectId("_id", value.getId());
        writer.writeString("name", value.getName());
        writer.writeString("jobTitle", value.getJobTitle());
        writer.writeDateTime("dateStarted", value.getDateStarted().getTime());
        writer.writeInt32("numberOfJobs", value.getNumberOfJobs());
        writer.writeName("testimonials");
        writer.writeStartArray();
        Codec<Testimonial> testimonialCodec = codecRegistry.get(Testimonial.class);
        for (Testimonial testimonial : value.getTestimonials()) {
            testimonialCodec.encode(writer, testimonial, encoderContext);
        }
        writer.writeEndArray();
        writer.writeEndDocument();
    }

    @Override
    public Worker decode(BsonReader reader, DecoderContext decoderContext) {
        reader.readStartDocument();
        ObjectId id = reader.readObjectId("_id");
        String name = reader.readString("name");
        String jobTitle = reader.readString("jobTitle");
        Date dateStarted = new Date(reader.readDateTime("dateStarted"));
        int numberOfJobs = reader.readInt32("numberOfJobs");

        reader.readName();
        Codec<Testimonial> testimonialCodec = codecRegistry.get(Testimonial.class);
        List<Testimonial> testimonials = new ArrayList<Testimonial>();
        reader.readStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            Testimonial testimonial = testimonialCodec.decode(reader, decoderContext);
            testimonials.add(testimonial);
        }
        reader.readEndArray();
        reader.readEndDocument();
        return new Worker(id, name, jobTitle, dateStarted, numberOfJobs, testimonials);
    }

    @Override
    public Class<Worker> getEncoderClass() {
        return Worker.class;
    }
}
```

The implementation of `WorkerCodec` now includes an instance variable `codecRegistry`. The constructor is also modified to accept a
parameter of type `CodecRegistry` that contains all `Codec`s needed by `WorkerCodec`. An application that is reading or writing `Worker`
objects to a collection will create an instance of `WorkerCodec` as follows:

```java
CodecRegistry testimonialCodecRegistry = CodecRegistries.fromCodecs(new TestimonialCodec());
CodecRegistry workerCodecRegistry = CodecRegistries.fromCodecs(new WorkerCodec(testimonialCodecRegistry));
```

The default `CodecRegistry` can be obtained using a static method on `MongoClient` class. The `workerCodecRegistry` together with
`defaultCodecRegistry` is used to create an instance of `CodecRegistry` that will be used to create `MongoClientOptions`:

```java
CodecRegistry defaultCodecRegistry = MongoClient.getDefaultCodecRegistry();
CodecRegistry codecRegistry = CodecRegistries.fromRegistries(workerCodecRegistry, defaultCodecRegistry);
MongoClientOptions options = MongoClientOptions.builder().codecRegistry(codecRegistry).build();
```

The `workerCodecRegistry` above needed a `CodecRegistry` containing an instance of `TestimonialCodec`. While creating the
`workerCodecRegistry` like this works fine, for more complex objects, creation of `CodecRegistry`s can get tedious. We can avoid creating
complex hierarchy of `CodecRegistry`s by using `CodecProvider`.

### CodecProvider
 
A [`CodecProvider`]({{< apiref "org/bson/codecs/configuration/CodecProvider" >}}) is a factory for `Codec` instances.  Unlike 
`CodecRegistry`, its `get` method takes not only a Class, but also a `CodecRegistry`, allowing a `CodecProvider` implementation to 
construct `Codec` instances that require a `CodecRegistry` to look up `Codec` instances for the values contained within it.  Consider a 
`CodecProvider` for the `Worker` class:

```java
public class WorkerCodecProvider implements CodecProvider {

    @Override
    public <T> Codec<T> get(Class<T> clazz,  CodecRegistry codecRegistry) {
        if (clazz == Worker.class) {
            // construct WorkerCodec with a CodecRegistry
            return (Codec<T>) new WorkerCodec(codecRegistry);
        }

        // CodecProvider returns null if it's not a provider for the requested Class 
        return  null;
    }
}
```

The `WorkerCodec`, because it is constructed with a `CodecRegistry`, can now use that registry to look up `Codec` instances for the 
values contained in each `Worker` that it encodes. An application that uses `CodecProvider` to construct `Codec` instances, benefits
from not having to create specific `CodecRegistry`s for each type of object that the application wants to encode from and decode to. It
can just create a `CodecRegistry` with a flat list of `Codec`s. The `BSON` engine creates a `CodecRegistry` containing all `Codec`s in this
list, and the created `CodecRegistry` is provided to any `Codec` constructed via `CodecProvider`.

One more problem remains, however.  Consider the problem of encoding values to a BSON DateTime.  An application may want to 
encode to a BSON DateTime instances of both the original Java `Date` class as well as the Java 8 `Instant` class.  It's easy to create 
implemenations of `Codec<Date>` and `Codec<Instant>`, and either one can be used for encoding.  But when decoding, a `WorkerCodec` 
also has to choose which Java type to decode a BSON DateTime to.  Rather than hard-coding it in the `WorkerCodec`, the decision is 
abstracted via the `BsonTypeClassMap` class.
    
### BsonTypeClassMap
    
The [`BsonTypeClassMap`]({{< apiref "org/bson/codecs/BsonTypeClassMap" >}}) class simply maps each value in the `BsonType` 
enumeration to a Java class.  It contains a sensible set of default mappings that can easily be changed by passing an `Map<BsonType, 
Class<?>>` instance to the constructor with any replacement mappings to apply.  Consider the case where an application wants to decode 
all BSON DateTime values to a Java 8 `Instant` instead of the default `Date`:

```java
Map<BsonType, Class<?>> replacements = new HashMap<BsonType, Class<?>>();
replacements.put(BsonType.DATE_TIME, Instant.class);
BsonTypeClassMap bsonTypeClassMap = new BsonTypeClassMap(replacements);
```

This will replace the default mapping of BSON DateTime to `Date` to one from BSON DateTime to `Instant`.

A `Codec` to encode from and decode to `Instant` objects would look like:

```java
public class InstantCodec implements Codec<Instant> {

    @Override
    public void encode(BsonWriter writer, Instant value, EncoderContext encoderContext) {
        writer.writeDateTime(value.toEpochMilli());
    }

    @Override
    public Instant decode(BsonReader reader, DecoderContext decoderContext) {
        return Instant.ofEpochMilli(reader.readDateTime());
    }

    @Override
    public Class<Instant> getEncoderClass() {
        return Instant.class;
    }
}
```

The `Worker` class needs a few modifications:

 * Since we want to use `Instant` instead of `Date`, the instance variable `dateStarted` is now declared to be of `Instant` type.
 * We would like `WorkerCodec` to handle both `Date` and `Instant` types for `dateStarted` field, without us having to modify it everytime
we switch between `Date` and `Instant`. For this, we create a new constructor with type of `dateStarted` parameter modified to `Object` type.
 * In the constuctor, `dateStarted` parameter is typecast to `Instant` type before assigning to the instance variable.
 * The return value of getter `getDateStarted` is now of `Instant` type.

With these changes, the modified `Worker` class looks like:

```java
public class Worker {
    private ObjectId id;
    private String name;
    private String jobTitle;
    private Instant dateStarted;
    private int numberOfJobs;
    private List<Testimonial> testimonials;

    public Worker(String name, String jobTitle, Object dateStarted,
                        int numberOfJobs, List<Testimonial> testimonials) {
        this(new ObjectId(), name, jobTitle, dateStarted, numberOfJobs, testimonials);
    }

    public Worker(ObjectId id, String name, String jobTitle, Object dateStarted,
                        int numberOfJobs, List<Testimonial> testimonials) {
        this.id = id;
        this.name = name;
        this.jobTitle = jobTitle;
        this.dateStarted = (Instant) dateStarted;
        this.numberOfJobs = numberOfJobs;
        this.testimonials = testimonials;
    }

    ...

    public Instant getDateStarted() {
        return dateStarted;
    }

    ...

}
```

To handle `BsonTypeClassMap`, `WorkerCodec` needs following modifications:

 * A new constructor that accepts an additional parameter of `BsonTypeClassMap` type. The constructor assigns the parameter to the instance
variable.
 * In `encode` and `decode` methods, the `get` method of `BsonTypeClassMap` is used to obtain the `Class` that will be used to encode
and decode a `BSON` `DATE_TIME` value. This `Class` is used to lookup `CodecRegistry` to obtain the `Codec` we need.
 * Because we want `WorkerCodec` to handle both `Date` and `Instant` types, in `WorkerCodec`'s `decode` method, the local variable
`dateStarted` is declared to be of type `Object`. When this variable is passed to `new Worker(...)`, the additional constructor that we
created in `Worker` class will be used to instantiate a `Worker` object.

`WorkerCodec` can still be use without passing it a `BsonTypeClassMap`, but the constructor needs a modification so that `bsonTypeClassMap`
instance variable gets properly initialized. We can use `BsonTypeClassMap`'s no-parameter constructor to initialize `bsonTypeClassMap`
instance variable to a map with default mappings.

With these changes, the modified `WorkerCodec` class looks like:

```java
public class WorkerCodec implements Codec<Worker> {
    private CodecRegistry codecRegistry;
    private BsonTypeClassMap bsonTypeClassMap;

    public WorkerCodec(CodecRegistry codecRegistry) {
        this.(codecRegistry, new BsonTypeClassMap());
    }

    public WorkerCodec(CodecRegistry codecRegistry, BsonTypeClassMap bsonTypeClassMap) {
        this.codecRegistry = codecRegistry;
        this.bsonTypeClassMap = bsonTypeClassMap;
    }

    @Override
    public void encode(BsonWriter writer, Worker value, EncoderContext encoderContext) {

        ...

        writer.writeName("dateStarted");
        Class dateClass = bsonTypeClassMap.get(BsonType.DATE_TIME);
        Codec dateTypeCodec = codecRegistry.get(dateClass);
        dateTypeCodec.encode(writer, value.getDateStarted(), encoderContext);

        ...

    }

    @Override
    public Worker decode(BsonReader reader, DecoderContext decoderContext) {

        ...

        reader.readName();
        Class dateClass = bsonTypeClassMap.get(BsonType.DATE_TIME);
        Codec dateTypeCodec = codecRegistry.get(dateClass);
        Object dateStarted = dateTypeCodec.decode(reader, decoderContext);

        ...

    }

    ...

    }
```

The `WorkerCodec`, because it is constructed with both a `BsonTypeClassMap` and a `CodecRegistry`, can first use the `BsonTypeClassMap`
to determine which type to decode each BSON value to, then use the `CodecRegistry` to look up the `Codec` for that Java type.

Now we need to modify `WorkerCodecProvider` so it can use the modified `WorkerCodec`. To keep `WorkerCodecProvider` usable
both with and without `BsonTypeClassMap`, we add a new constructor and modify the existing no-parameter constructor.

With these changes the, `WorkerCodecProvider` looks as follows:
 
```java
public class WorkerCodecProvider implements CodecProvider {
    private final BsonTypeClassMap bsonTypeClassMap;

    public WorkerCodecProvider() {
        this(new BsonTypeClassMap());
    }

    public WorkerCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
        this.bsonTypeClassMap = bsonTypeClassMap;
    }

    @Override
    public <T> Codec<T> get(Class<T> clazz,  CodecRegistry codecRegistry) {
        if (clazz == Worker.class) {
            // construct WorkerCodec with a CodecRegistry and a BsonTypeClassMap
            return (Codec<T>) new WorkerCodec(codecRegistry, bsonTypeClassMap);
        }
        return  null;
    }
}
``` 

Finally, we create a `CodecRegistry` instance that uses `workerCodecProvider` which was instantiatied with `BsonTypeClassMap`
parameter:

```bash
WorkerCodecProvider workerCodecProvider = new WorkerCodecProvider(bsonTypeClassMap);
codecRegistry = CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new InstantCodec()),
                                               CodecRegistries.fromProviders(workerCodecProvider),
                                               defaultCodecRegistry);
```

using an additional static factory methods from the `CodecRegistries` class: one that takes a list of `CodecProvider`s.

### Transformer

Suppose the marketing team has analyzed and determined that all workers in our collection are highly skilled and experienced. Marketing
feels that prefixing "Master" to job title of each worker would increase traffic to company's website (which is a website that provides
reviews on workers). Marketing wants to test their idea for a couple of weeks and would like to revert back to original job titles if they
consider the test to be not successful. Since, we the application developers, need an ability to revert back quickly, we can use
`Transformer` to achieve this.

We will implement a class of type `Transformer` that needs a method `transform`. The `WorkerTransformer` implementation is as follows:

```java
public class WorkerTransformer implements Transformer {

    @Override
    public Object transform(Object o) {
        String jobTitle = ((Worker) o).getJobTitle();
        ((Worker) o).setJobTitle("Master " + jobTitle);
        return o;
    }
}
```

The `WorkerCodec` needs following modifications to use `Transformer`:

 * A new instance variable of type `Transformer` is needed.
 * A new constructor is created which accepts an additional `Transformer` parameter.
 * Modify existing constructor, so it calls the new constructor with `null` value for the `Transformer` parameter. Handling of a `null`
transformer value is shown later in this document.
 * `decode` method modifies the `Worker` object by calling the `transform` method of `WorkerTransformer` object.

With these changes, the modified `WorkerCodec` looks as follows:

```java
public class WorkerCodec implements Codec<Worker> {
    private CodecRegistry codecRegistry;
    private BsonTypeClassMap bsonTypeClassMap;
    private Transformer      transformer;

    public WorkerCodec(CodecRegistry codecRegistry) {
        this(codecRegistry, new BsonTypeClassMap());
    }

    public WorkerCodec(CodecRegistry codecRegistry, BsonTypeClassMap bsonTypeClassMap) {
        this(codecRegistry, bsonTypeClassMap, null);
    }

    public WorkerCodec(CodecRegistry codecRegistry, BsonTypeClassMap bsonTypeClassMap,
                                                            Transformer transformer) {
        this.codecRegistry = codecRegistry;
        this.bsonTypeClassMap = bsonTypeClassMap;
        this.transformer = transformer;
    }

    ...

    @Override
    public Worker decode(BsonReader reader, DecoderContext decoderContext) {

        ...

        return (Worker) transformer.transform(new Worker(id, name, jobTitle, dateStarted,
                                                                 numberOfJobs, testimonials));
    }

    ...

}

```

`WorkerCodecProvider` needs a new instance variable to hold `Transformer`. The constructor accepts a `Transformer` parameter.
In the `get` method `WorkerCodec` is contructed with the additional `Transformer` parameter. The modified `WorkerCodecProvider` looks like:


```java
public class WorkerCodecProvider implements CodecProvider {
    private final BsonTypeClassMap bsonTypeClassMap;
    private final Transformer transformer;

    public WorkerCodecProvider() {
        this(new BsonTypeClassMap());
    }

    public WorkerCodecProvider(BsonTypeClassMap bsonTypeClassMap) {
        this(new BsonTypeClassMap(), null);
    }

    public WorkerCodecProvider(final BsonTypeClassMap bsonTypeClassMap, final Transformer transformer) {
        this.bsonTypeClassMap = bsonTypeClassMap;
        this.transformer = transformer;
    }

    @Override
    public <T> Codec<T> get(Class<T> clazz,  CodecRegistry codecRegistry) {
        if (clazz == Worker.class) {
            // construct WorkerCodec with a CodecRegistry, a BsonTypeClassMap and a Transformer
            return (Codec<T>) new WorkerCodec(codecRegistry, bsonTypeClassMap, transformer);
        }
        return  null;
    }
}
```

Finally, we create a `CodecRegistry` instance that uses `workerCodecProvider` which was instantiatied with `BsonTypeClassMap`
and `Transformer` parameters:

```bash
Transformer transformer = new WorkerTransformer();
WorkerCodecProvider workerCodecProvider = new WorkerCodecProvider(bsonTypeClassMap, transformer);
codecRegistry = CodecRegistries.fromRegistries(CodecRegistries.fromCodecs(new InstantCodec()),
                                               CodecRegistries.fromProviders(workerCodecProvider),
                                               defaultCodecRegistry);
```

Since we want our application to transform the result when we need to and revert back to original behavior easily, we have to develop the
application in such a way that we can switch between two behaviors quickly and with minimal code changes. We can revert to original behavior
(no transformation) by using the constructor that accepts just `BsonTypeClassMap` object. As shown earlier, this constructor in turn makes a
call to the constructor with `BsonTypeClassMap` and `Transformer` parameters by passing a `null` as `Transformer` parameter.

In `WorkerCodec` constructor, if transformer parameter is null, we create an instance of a new `Transformer` class whose `transform`
method just returns the same object that it received at parameter. Let's call the new class `PassThroughTransformer` which is implemented as:


```java
public class PassThroughTransformer implements Transformer {

    @Override
    public Object transform(Object o) {
        return o;
    }
}
```

The modified `WorkerCodec` that uses `PassThroughTransformer` looks as follows:
    
```java
public class WorkerCodec implements Codec<Worker> {
    private CodecRegistry codecRegistry;
    private BsonTypeClassMap bsonTypeClassMap;
    private Transformer      transformer;

    public WorkerCodec(CodecRegistry codecRegistry) {
        this(codecRegistry, new BsonTypeClassMap());
    }

    public WorkerCodec(CodecRegistry codecRegistry, BsonTypeClassMap bsonTypeClassMap) {
        this(codecRegistry, bsonTypeClassMap, null);
    }

    public WorkerCodec(CodecRegistry codecRegistry, BsonTypeClassMap bsonTypeClassMap,
                                                            Transformer transformer) {
        this.codecRegistry = codecRegistry;
        this.bsonTypeClassMap = bsonTypeClassMap;
        this.transformer = transformer != null ? transformer : new PassThroughTransformer();
    }

    ...

}
```

With these modifications, we can quickly revert back to not using `Transformer` by using the one-parameter constructor of
`WorkerCodecProvider`.  Thus you can switch on/off the `Transformer` functionality by modifying just one line of application code which
is shown below:


```java
WorkerCodecProvider workerCodecProvider = new WorkerCodecProvider(bsonTypeClassMap, transformer);
```

In this document, the example's final variation shows using `BsonTypeClassMap` and `Transformer` together. These features do not need
each other to work. An application that does not use `BsonTypeClassMap` but uses `Tranformer` needs to add additional constructors to
`WorkerCodec` and `WorkerCodecProvider` implementations.
