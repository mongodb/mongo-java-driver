+++
date = "2017-04-10T14:27:51-04:00"
title = "POJOs"
[menu.main]
  parent = "BSON"
  weight = 45
  pre = "<i class='fa fa-star'></i>"
+++

## POJOs - Plain Old Java Objects

The 3.5 release of the driver adds POJO support via the [`PojoCodec`]({{<apiref "org/bson/codecs/pojo/PojoCodec.html">}}), which allows for 
direct serialization of POJOs and Java Beans to and from BSON. Internally, each `PojoCodec` utilizes a
[`ClassModel`]({{<apiref "org/bson/codecs/pojo/ClassModel.html">}}) instance to store metadata about how the POJO should be serialized.

A `ClassModel` for a POJO includes:

  * The class of the POJO.
  * A new instance factory. Handling the creation of new instances of the POJO. By default it requires the POJO to have an empty constructor.
  * Property information, a list of [`PropertyModel`]({{<apiref "org/bson/codecs/pojo/PropertyModel.html">}}) instances that contain all the 
    property metadata. By default this includes; any public getter methods with any corresponding setter methods and any public fields.
  * An optional IdProperty. By default the `_id` or `id` property in the POJO.
  * Type data for the POJO and its fields to work around type erasure.
  * An optional discriminator value. The discriminator is the value used to represent the POJO class being stored.
  * An optional discriminator key. The document key name for the discriminator.
  * The use discriminator flag. This determines if the discriminator should be serialized. By default it is off.
  
Each `PropertyModel` includes:

  * The property name.
  * The read name, the name of the property to use as the key when serializing into BSON.
  * The write name, the name of the property to use as the key when deserializing from BSON.
  * Type data, to work around type erasure.
  * An optional `Codec` for the property. The codec allows for fine grained control over how the property is encoded and decoded.
  * A serialization checker. This checks if the value should be serialized. By default, `null` values are not serialized.
  * A property accessor. Used to access the property values from the POJO instance.
  * Use discriminator flag, only used when serializing other POJOs. By default it is off. When on the `PojoCodecProvider` copies the 
    `ClassModel` for the field's type and turns on the use discriminator flag. The corresponding `ClassModel` must be configured with a 
    discriminator key and value.

ClassModels are built using the [`ClassModelBuilder`]({{<apiref "org/bson/codecs/pojo/ClassModelBuilder.html">}}) which can be accessed via
 the [`ClassModel.builder(clazz)`]({{<apiref "org/bson/codecs/pojo/ClassModel.html#builder-java.lang.Class-">}}) method. The builder 
 initially uses reflection to create the required metadata.

`PojoCodec` instances are created by the [`PojoCodecProvider`]({{<apiref "org/bson/codecs/pojo/PojoCodecProvider.html">}}) which is a
`CodecProvider`. CodecProviders are used by the `CodecRegistry` to find the correct `Codec` for any given class.

{{% note class="important" %}}
By default all POJOs **must** include a public or protected, empty, no arguments, constructor. The 
[`BsonCreator`]({{<apiref "org/bson/codecs/pojo/annotations/BsonCreator.html">}}) annotation can be used to support constructors or 
public static methods to create new instances of a POJO.

All properties in a POJO must have a [`Codec`]({{< relref "codecs.md" >}}) registered in the `CodecRegistry` so that their values can be 
encoded and decoded.
{{% /note %}}

## POJO support

The entry point for POJO support is the `PojoCodecProvider`. New instances can be created via the
[`PojoCodecProvider.builder()`]({{<apiref "org/bson/codecs/pojo/PojoCodecProvider.html#builder">}}) method. The `builder` allows users to 
register any combination of:

  * Individual POJO classes.
  * Package names containing POJO classes.
  * `ClassModel` instances which allow fine grained control over how a POJO is encoded and decoded.
  
The `builder` also allows the user to register default [Conventions](#conventions) for any POJOs that are automatically mapped, either 
the individual POJO classes or POJOs found from registered packages. The `PojoCodecProvider` will lookup PojoCodecs and return the first 
that matches the POJO class:
  
  * Registered ClassModels
  * Registered POJO classes
  * Registered POJO classes contained in one of the registered packages

Once the `PojoCodecProvider` has been built, by calling `builder.build()`, it can be combined with an existing `CodecRegistry` to create a 
new registry that will also support the registered POJOs. The following example registers the package `org.example.pojos` and creates a new 
`CodecRegistry`.

```java
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

// Create a CodecRegistry containing the PojoCodecProvider instance.
CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register("org.example.pojos").build();
CodecRegistry pojoCodecRegistry = fromRegistries(fromProviders(pojoCodecProvider), defaultCodecRegistry);
```

{{% note class="tip" %}}
In general only one instance of a `PojoCodecProvider` should be created. 

This is because each `PojoCodecProvider` instance contains a look up table for discriminator names. If multiple PojoCodecProviders are 
used, care should be taken to ensure that each provider contains a holistic view of POJO classes, otherwise discriminator lookups can fail. 
Alternatively, using the full class name as the discriminator value will ensure successful POJO lookups. 
{{% /note %}}

## Default configuration

By default the `PojoCodec` will not store `null` values or a discriminator when converting a POJO to BSON. 

Take the following `Person` class:

```java
public class Person {
    private String firstName;
    private String lastName;
    private Address address = null;
 
    public Person() { }
    
    public Person(final String firstName, final String lastName) {  /* Set values... */ }

    public String getFirstName() {
        return firstName;
    }
    
    public void setFirstName(final String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }
    
    public void setLastName(final String lastName) {
        this.lastName = lastName;
    }

    public String getAddress() {
        return address;
    }
    
    public void setAddress(final Address address) {
        this.address = address;
    }
    
}
```

The instance of `new Person("Ada", "Lovelace");` would be serialized to the equivalent of `{ firstName: "Ada", lastName: "Lovelace"}`. 

Notice the `address` property is omitted because it hasn't been set and has a `null` value. If the person instance contained an address, it 
would be stored as a sub document and use the `CodecRegistry` to look up the `Codec` for the `Address` class and use that to 
encode and decode the address value.

{{% note class="note" %}}
#### POJO Properties

Properties are identified by **public** getter methods, **public** setter methods and **public** fields. 

Any properties with an underlying field that is transient or static will be ignored and not serialized or deserialized.

**Serializing to BSON**

If a public getter methods exists, it is used to obtain the value for the property, otherwise the field is used directly.

**Deserializing from BSON**

If a public setter method exists it is used to set the value, otherwise the field is set directly.

{{% /note %}}

### Generics support

Generics are fully supported. During the creation of a `ClassModelBuilder` type parameters are inspected and saved to work around type 
erasure. The only requirement is the top level POJO **cannot** contain any type parameters. 

Take the following classes:

```java
public class GenericClass<T> {
    T genericField;
    // Rest of implementation
}

public class GenericTree<A, B> {
    GenericTree<A, B> left;
    GenericTree<A, B> right;
    // Rest of implementation
}

public final class Tree extends GenericTree<Integer, String> {
    GenericClass<Long> genericClass;
    // Rest of implementation
}
```

The `Tree` POJO is serializable because it doesn't have any unknown type parameters. The `left`, `right` and `genericClass` properties are 
all serializable because they are bound to the concrete types `Integer`, `String` and `Long`. 

On their own, instances of `GenericTree` or `GenericClass` are not serializable by the `PojoCodec`. This is because the runtime type parameter
information is erased by the JVM, and the type parameters cannot be specialized accurately.

### Enum support

Enums are fully supported. The `PojoCodec` uses the name of the enum constant as the property value. This is then converted back into an Enum 
value by the codec using the static `Enum.valueOf` method.

Take the following example:

```Java

public enum Membership {
    UNREGISTERED,
    SUBSCRIBER,
    PREMIUM
}

public class Person {
    private String firstName;
    private String lastName;
    private Member membership = Member.UNREGISTERED;

    public Person() { }

    public Person(final String firstName, final String lastName, final Membership membership) { }

    // Rest of implementation
}
```

The instance of `new Person("Bryan", "May", SUBSCRIBER);` would be serialized to the equivalent of 
`{ firstName: "Bryan", lastName: "May", membership: "SUBSCRIBER"}`. 

If you require an alternative representation of the Enum, you can override how a Enum is stored by registering a custom `Codec` for the 
Enum in the `CodecRegistry`.

### Conventions

The [`Convention`]({{<apiref "org/bson/codecs/pojo/Convention.html">}}) interface provides a mechanism for `ClassModelBuilder`
instances to be configured during the build stage and the creation of the `ClassModel`.

The following Conventions are available from the [`Conventions`]({{<apiref "org/bson/codecs/pojo/Conventions.html">}}) class:

  * The [`ANNOTATION_CONVENTION`]({{<apiref "org/bson/codecs/pojo/Conventions.html#ANNOTATION_CONVENTION">}}). Applies all the 
    default annotations.
  * The [`CLASS_AND_PROPERTY_CONVENTION`]({{<apiref "org/bson/codecs/pojo/Conventions.html#CLASS_AND_PROPERTY_CONVENTION">}}). Sets the 
        discriminator key if not set to `_t` and the discriminator value if not set to the ClassModels simple type name. Also, configures 
        the PropertyModels. If the `idProperty` isn't set and there is a property named `_id` or `id` then it will be marked as the `idProperty`.
  * The [`DEFAULT_CONVENTIONS`]({{<apiref "org/bson/codecs/pojo/Conventions.html#DEFAULT_CONVENTIONS">}}), a list containing the 
    `ANNOTATION_CONVENTION` and the `CLASS_AND_PROPERTY_CONVENTION`.
  * The [`NO_CONVENTIONS`]({{<apiref "org/bson/codecs/pojo/Conventions.html#NO_CONVENTIONS">}}) an empty list.

Custom Conventions can either be set globally via the 
[`PojoCodecProvider.Builder.conventions(conventions)`]({{<apiref "org/bson/codecs/pojo/PojoCodecProvider.Builder.html#conventions-java.util.List-">}}) 
method, or via the [`ClassModelBuilder.conventions(conventions)`]({{<apiref "org/bson/codecs/pojo/ClassModelBuilder.html#conventions-java.util.List-">}}) 
method.

{{% note class="note" %}}
Conventions are applied in order during the build stage when creating a `ClassModel`. 

Each `Convention` can mutate the underlying `ClassModelBuilder`, so care should be taken that Conventions do not conflict with each other 
in their intent. 
{{% /note %}}


### Annotations

Annotations require the `ANNOTATION_CONVENTION` and provide an easy way to configure how POJOs are serialized. 

The following annotations are available from the 
[`org.bson.codecs.pojo.annotations`]({{<apiref "org/bson/codecs/pojo/annotations/package-summary.html">}}) package:

  * [`BsonCreator`]({{<apiref "org/bson/codecs/pojo/annotations/BsonCreator.html">}}), marks a public constructor or a public static method as
    the creator for new instances of the class. Can be combined with the `BsonProperty` annotation to link the parameters with properties.
  * [`BsonDiscriminator`]({{<apiref "org/bson/codecs/pojo/annotations/BsonDiscriminator.html">}}), enables using a discriminator. 
    Also allows for setting a custom discriminator key and value.
  * [`BsonId`]({{<apiref "org/bson/codecs/pojo/annotations/BsonId.html">}}), marks a property to be serialized as the `_id` property.
  * [`BsonIgnore`]({{<apiref "org/bson/codecs/pojo/annotations/BsonIgnore.html">}}), marks a property to be ignored. Can be used to 
    configure if a property is serialized and / or deserialized.
  * [`BsonProperty`]({{<apiref "org/bson/codecs/pojo/annotations/BsonProperty.html">}}). Allows for an alternative document key 
    name when converting the POJO field to BSON. Also, allows a field to turn on using a discriminator when storing a POJO value.

Annotations can be applied to read and / or write contexts by configuring the getter / setter methods. Any annotations applied to a field 
are applied to both read and write contexts.

Take the following `Person` class:

```java
import org.bson.codecs.pojo.annotations.*;

@BsonDiscriminator
public final class Person {
    public String personId;
    public String firstName;
    public String lastName;
    
    @BsonProperty(useDiscriminator = true)
    public Address addr;
    
    public Person(){
    }

}
```

The `Person` POJO Will produce BSON similar to:

```json
{ "_id": "1234567890", "_t": "Person", "firstName": "Alan", "lastName": "Turing",
  "address": { "_t": "Address", "address": "The Mansion", "street": "Sherwood Drive", 
               "town": "Bletchley", "postcode": "MK3 6EB" } }
```

The getter methods are used to configure the document keys used when storing the data in MongoDB. The `_id` document key maps to the POJO's 
`personId` property. The `_t` field contains the discriminator and the `address` field also contains a discriminator.


## Advanced configuration

For most scenarios there is no need for further configuration. However, there are some scenarios where custom configuration is required.

### Properties with abstract or interface types.

If a POJO contains a field that has an abstract type or has an interface as its type, then a discriminator is required. The type and all 
subtypes / implementations need to be registered with the `PojoCodecProvider` so that values can be encoded and decoded correctly.

The easiest way to enable a discriminator is to annotate the abstract class with the `Discriminator` annotation. Alternatively, the 
[`ClassModelBuilder.enableDiscriminator(true)`]({{<apiref "org/bson/codecs/pojo/ClassModelBuilder.html#enableDiscriminator-boolean-">}}) 
method can be used to enable the use of a discriminator. 

The following example creates a `CodecRegistry` with discriminators enabled for a `User` interface and its concrete `FreeUser` and 
`SubscriberUser` implementations:

```java
ClassModel<User> userModel = ClassModel.builder(User.class).enableDiscriminator(true).build();
ClassModel<FreeUser> freeUserModel = ClassModel.builder(FreeUser.class).enableDiscriminator(true).build();
ClassModel<SubscriberUser> subscriberUserModel = ClassModel.builder(SubscriberUser.class).enableDiscriminator(true).build();

PojoCodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(userModel, freeUserModel, subscriberUserModel).build();

CodecRegistry pojoCodecRegistry = fromRegistries(fromProviders(pojoCodecProvider), defaultCodecRegistry);
```

### Supporting POJOs without no args constructors

By default PojoCodecs work with POJOs that have an empty, no arguments, constructor. POJOs with alternative constructors are supported 
via the `ANNOTATION_CONVENTION` and the `@BsonCreator` annotation. Any parameters for the creator constructor should be annotated with the 
`@BsonProperty` annotation. Below is an example of a `Person` POJO that contains final fields that are set via the annotated constructor:

```java
import org.bson.codecs.pojo.annotations.*;

@BsonDiscriminator
public final class Person {
    private final String pid;
    private final String fName;
    private final String lName;
    
    @BsonProperty(useDiscriminator = true)
    private final Address addr;
    
    @BsonCreator
    public Person(@BsonProperty("personId") final String personId, @BsonProperty("firstName") final String firstName,
                  @BsonProperty("lastName") final String lastName, @BsonProperty("address") final Address address) {
        this.pid = personId;
        this.fName = firstName;
        this.lName = lastName;
        this.addr = address;
    }

    @Id
    public String getPersonId() {
        return pid;
    }

    public String getFirstName() {
        return fName;
    }

    public String getLastName() {
        return lName;
    }

    public String getAddress() {
        return addr;
    }
}
```

### Changing what is serialized

By default `null` values aren't serialized. This is controlled by the default implementation of the 
[`FieldSerialization`]({{<apiref "org/bson/codecs/pojo/FieldSerialization.html">}}) interface. Custom implementations can be set on 
the `PropertyModelBuilder` which is available from the `ClassModelBuilder`.

The 
