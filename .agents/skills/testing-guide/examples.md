# Test Skeletons

Match these structural conventions when writing new tests.

## Java — JUnit 5

```java
public class FeatureUnderTestTest extends OperationTest {

    private ResourceType resource;

    @BeforeEach
    void setup() {
        // Insert test data, acquire resources
    }

    @AfterEach
    void cleanup() {
        // Release resources to prevent leaks
    }

    @Test
    @DisplayName("should do expected behavior")
    void shouldDoExpectedBehavior() {
        // given
        // when
        // then
    }

    @ParameterizedTest(name = "{index} => input={0}, expected={1}")
    @MethodSource
    @DisplayName("should handle varied inputs")
    void shouldHandleVariedInputs(final int input, final int expected) {
        assertEquals(expected, methodUnderTest(input));
    }

    private static Stream<Arguments> shouldHandleVariedInputs() {
        return Stream.of(arguments(1, 1), arguments(2, 4));
    }
}
```

## Scala — ScalaTest with Mockito

```scala
class FeatureUnderTestSpec extends BaseSpec with MockitoSugar {

  val wrapped = mock[JWrappedType]
  val underTest = new ScalaWrapper(wrapped)

  "ScalaWrapper" should "have the same methods as the wrapped type" in {
    val wrappedMethods = classOf[JWrappedType].getMethods.map(_.getName).toSet
    val localMethods = classOf[ScalaWrapper].getMethods.map(_.getName)
    // Assert parity
  }

  it should "delegate to underlying method" in {
    underTest.someMethod("arg")
    verify(wrapped).someMethod("arg")
  }
}
```

## Kotlin — kotlin.test with Mockito-Kotlin

```kotlin
class FeatureUnderTestTest {

    companion object {
        @Mock internal val wrapped: com.mongodb.client.MongoCollection<MyType> = mock()
        lateinit var collection: MongoCollection<MyType>

        @JvmStatic
        @BeforeAll
        internal fun setUpMocks() {
            collection = MongoCollection(wrapped)
            whenever(wrapped.namespace).doReturn(MongoNamespace("db", "coll"))
        }
    }

    @Test
    fun shouldProduceCorrectBson() {
        assertEquals(BsonDocument.parse("""{"expected": 1}"""), resultUnderTest.toBsonDocument())
    }

    data class MyType(val id: String, val name: String)
}
```
