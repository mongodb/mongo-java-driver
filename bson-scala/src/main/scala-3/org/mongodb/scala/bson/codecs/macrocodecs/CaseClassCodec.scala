/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala.bson.codecs.macrocodecs


import scala.quoted.*
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.codecs.macrocodecs.MacroCodec
import org.mongodb.scala.bson.annotations.{BsonIgnore, BsonProperty}


/**
 * Compile-time macro that generates BSON codecs for Scala 3 case classes.
 *
 * At compile time, this macro inspects type `T` using the Scala 3 reflection API (`quotes.reflect`)
 * and generates a concrete [[MacroCodec]][T] implementation. The generated codec handles:
 *   - Encoding case class fields to BSON documents
 *   - Decoding BSON documents back to case class instances
 *   - Sealed class/trait hierarchies via a `_t` discriminator field
 *   - `@BsonProperty` for field name customization
 *   - `@BsonIgnore` for excluding fields (requires a default value)
 *   - `Option[T]` fields with configurable None handling (encode as null vs omit)
 *
 * The generated [[MacroCodec]] carries three runtime maps that describe the type structure:
 *   - `caseClassesMap`: maps class names to their `Class[?]` objects (for sealed hierarchies)
 *   - `classToCaseClassMap`: maps every encountered `Class[?]` to whether it's a case class/sealed type
 *   - `classFieldTypeArgsMap`: maps class name -> field name -> list of type argument classes
 *
 * All code generation logic lives within [[createCodec]] because Scala 3 macros require a `Quotes`
 * context and `TypeRepr.of[T]` in scope; helper functions that reference these cannot be extracted
 * outside the method.
 *
 * @see [[MacroCodec]] for the runtime base trait that the generated codec extends.
 */
private[bson] object CaseClassCodec {

  /**
   * Prefix for compiler-generated default value methods on companion objects.
   * For `case class Foo(x: Int = 42)`, the compiler generates `Foo.apply$default$1`.
   */
  private val APPLY_DEFAULT_PREFIX = "apply$default$"

  /**
   * Alternative prefix for default value methods, used when the primary constructor
   * is represented as `<init>` rather than `apply` (encoded as `$lessinit$greater$`).
   */
  private val INIT_DEFAULT_PREFIX = "$lessinit$greater$default$"

  def createCodecBasicCodecRegistryEncodeNone[T: Type](using Quotes): Expr[Codec[T]] =
    createCodecBasicCodecRegistry[T](Expr(true))

  def createCodecEncodeNone[T: Type](codecRegistry: Expr[CodecRegistry])(using Quotes): Expr[Codec[T]] =
    createCodec[T](codecRegistry, Expr(true))

  def createCodecBasicCodecRegistryIgnoreNone[T: Type](using Quotes): Expr[Codec[T]] =
    createCodecBasicCodecRegistry[T](Expr(false))

  def createCodecIgnoreNone[T: Type](codecRegistry: Expr[CodecRegistry])(using Quotes): Expr[Codec[T]] =
    createCodec[T](codecRegistry, Expr(false))

  def createCodecBasicCodecRegistry[T: Type](encodeNone: Expr[Boolean])(using Quotes): Expr[Codec[T]] =
    createCodec[T](
      '{
        import org.bson.codecs.{BsonValueCodecProvider, ValueCodecProvider}
        import org.bson.codecs.configuration.CodecRegistries.fromProviders
        import org.mongodb.scala.bson.codecs.{DocumentCodecProvider, IterableCodecProvider}
        fromProviders(
          DocumentCodecProvider(),
          IterableCodecProvider(),
          new ValueCodecProvider(),
          new BsonValueCodecProvider()
        )
      },
      encodeNone
    )

  // scalastyle:off method.length
  def createCodec[T: Type](codecRegistry: Expr[CodecRegistry], encodeNone: Expr[Boolean])(using Quotes): Expr[Codec[T]] = {
    import quotes.reflect.*

    val mainTypeRepr = TypeRepr.of[T]
    val mainSymbol = mainTypeRepr.typeSymbol

    // ============================================================
    // Type Classification Helpers
    // ============================================================
    // These functions classify types by their Scala nature (case class,
    // case object, sealed trait, etc.). Used throughout to determine how
    // to handle each type: case classes need constructor calls, case
    // objects need singleton references, sealed types need discriminators.

    val stringTypeRepr = TypeRepr.of[String]
    val mapTypeSymbol = TypeRepr.of[collection.Map[?, ?]].typeSymbol
    val optionSymbol = TypeRepr.of[Option[?]].typeSymbol

    def isCaseClass(sym: Symbol): Boolean =
      sym.flags.is(Flags.Case) && !sym.flags.is(Flags.Module) && sym.isClassDef

    def isCaseObject(sym: Symbol): Boolean =
      sym.flags.is(Flags.Case) && sym.flags.is(Flags.Module)

    def isSealed(sym: Symbol): Boolean =
      sym.flags.is(Flags.Sealed)

    def isAbstractSealed(sym: Symbol): Boolean =
      isSealed(sym) && (sym.flags.is(Flags.Abstract) || sym.flags.is(Flags.Trait))

    def isOption(tpe: TypeRepr): Boolean =
      tpe.dealias.typeSymbol == optionSymbol

    def isMap(tpe: TypeRepr): Boolean =
      tpe.baseClasses.contains(mapTypeSymbol)

    def isTuple(tpe: TypeRepr): Boolean =
      tpe.typeSymbol.fullName.startsWith("scala.Tuple")

    /** Strips intersection types (A & B) down to the left-most type. Needed for tagged types. */
    def stripAndType(tpe: TypeRepr): TypeRepr = tpe match {
      case AndType(left, _) => stripAndType(left)
      case other => other
    }

    def isCaseClassType(tpe: TypeRepr): Boolean =
      isCaseClass(stripAndType(tpe.dealias).typeSymbol)

    def isCaseObjectType(tpe: TypeRepr): Boolean =
      isCaseObject(stripAndType(tpe.dealias).typeSymbol)

    def isSealedType(tpe: TypeRepr): Boolean =
      isSealed(stripAndType(tpe.dealias).typeSymbol)

    // ============================================================
    // Subclass Discovery
    // ============================================================
    // For sealed traits/classes, recursively discover all concrete
    // case class and case object subclasses. These become the set
    // of types the codec can encode/decode.

    def allSubclasses(sym: Symbol): Set[Symbol] = {
      val direct = sym.children.toSet
      direct ++ direct.flatMap(allSubclasses)
    }

    val subClassSymbols = allSubclasses(mainSymbol).filter(s => isCaseClass(s) || isCaseObject(s)).toList

    if (isSealed(mainSymbol) && subClassSymbols.isEmpty) {
      val kind = if (mainSymbol.flags.is(Flags.Trait)) "trait" else "class"
      report.errorAndAbort(
        s"No known subclasses of sealed $kind '${mainSymbol.name}'. " +
          "Sealed types must have at least one case class or case object subclass."
      )
    }

    val knownTypeSymbols: List[Symbol] = {
      val main = if (!mainSymbol.flags.is(Flags.Abstract) && !mainSymbol.flags.is(Flags.Trait)) List(mainSymbol) else Nil
      (main ++ subClassSymbols).distinct.reverse
    }

    // ============================================================
    // Field Extraction
    // ============================================================
    // Extract constructor parameter information for each concrete type.
    // Each FieldInfo captures the parameter name, its TypeRepr, and its Symbol.

    case class FieldInfo(name: String, tpe: TypeRepr, paramSymbol: Symbol)

    def getFields(sym: Symbol): List[FieldInfo] = {
      if (isAbstractSealed(sym)) Nil
      else {
        val termParams = sym.primaryConstructor.paramSymss.filter(_.headOption.exists(!_.isType)).headOption.getOrElse(Nil)
        termParams.map { p =>
          FieldInfo(p.name, sym.typeRef.memberType(p), p)
        }
      }
    }

    val fieldsMap: Map[Symbol, List[FieldInfo]] = knownTypeSymbols.map(s => s -> getFields(s)).toMap

    val allParamSymbols: List[Symbol] = knownTypeSymbols.flatMap { sym =>
      if (isAbstractSealed(sym)) Nil
      else sym.primaryConstructor.paramSymss.filter(_.headOption.exists(!_.isType)).headOption.getOrElse(Nil)
    }

    // ============================================================
    // Annotation Extraction
    // ============================================================
    // Process @BsonProperty (field name override) and @BsonIgnore
    // (field exclusion) annotations from constructor parameters.

    val annotatedFieldsMap: Map[String, String] = allParamSymbols.flatMap { p =>
      p.annotations.collectFirst {
        case annot if annot.tpe =:= TypeRepr.of[BsonProperty] =>
          annot match {
            case Apply(_, List(Literal(StringConstant(key)))) => p.name -> key
            case _ => report.errorAndAbort(
              s"@BsonProperty on '${p.name}' in '${mainSymbol.name}' must have a string literal argument."
            )
          }
      }
    }.toMap

    case class IgnoredField(name: String, paramIndex: Int)

    val ignoredFieldsMap: Map[Symbol, List[IgnoredField]] = knownTypeSymbols.map { sym =>
      if (!isCaseClass(sym)) sym -> Nil
      else {
        val termParams = sym.primaryConstructor.paramSymss
          .filter(_.headOption.exists(!_.isType)).headOption.getOrElse(Nil)
        val ignored = termParams.zipWithIndex.flatMap { case (p, i) =>
          if (p.annotations.exists(a => a.tpe =:= TypeRepr.of[BsonIgnore])) {
            val companion = sym.companionModule
            val hasDefault = companion.methodMember(s"$APPLY_DEFAULT_PREFIX${i + 1}").nonEmpty ||
              companion.methodMember(s"$INIT_DEFAULT_PREFIX${i + 1}").nonEmpty
            if (!hasDefault) report.errorAndAbort(
              s"@BsonIgnore field '${p.name}' in '${sym.name}' must have a default value, " +
                "since the codec needs a value to use when the field is absent."
            )
            Some(IgnoredField(p.name, i))
          } else None
        }
        sym -> ignored
      }
    }.toMap

    // ============================================================
    // Name Resolution Helpers
    // ============================================================

    /** Resolves the BSON discriminator name for a class symbol (strips trailing `$` from case objects). */
    def resolveClassName(sym: Symbol): String = {
      val n = sym.name
      if (n.endsWith("$")) n.dropRight(1) else n
    }

    /** Resolves the BSON field name, applying @BsonProperty override if present. */
    def resolveFieldName(fieldName: String): String =
      annotatedFieldsMap.getOrElse(fieldName, fieldName)

    // ============================================================
    // Primitive Types Map
    // ============================================================
    // Maps Scala primitive type symbols to their boxed Java equivalents.
    // Needed because BSON codecs work with boxed types at runtime.

    val primitiveTypesMap: Map[Symbol, TypeRepr] = Map(
      TypeRepr.of[Boolean].typeSymbol -> TypeRepr.of[java.lang.Boolean],
      TypeRepr.of[Byte].typeSymbol -> TypeRepr.of[java.lang.Byte],
      TypeRepr.of[Char].typeSymbol -> TypeRepr.of[java.lang.Character],
      TypeRepr.of[Double].typeSymbol -> TypeRepr.of[java.lang.Double],
      TypeRepr.of[Float].typeSymbol -> TypeRepr.of[java.lang.Float],
      TypeRepr.of[Int].typeSymbol -> TypeRepr.of[java.lang.Integer],
      TypeRepr.of[Long].typeSymbol -> TypeRepr.of[java.lang.Long],
      TypeRepr.of[Short].typeSymbol -> TypeRepr.of[java.lang.Short]
    )

    // ============================================================
    // Type Flattening
    // ============================================================
    // Flattens a type and its type arguments into a list of Class[?]
    // objects for runtime use. For example, `Seq[Person]` flattens to
    // `[Seq, Person]`. Map keys (which must be String) are excluded.
    // Option wrappers are stripped. Primitives are boxed.

    def flattenTypeArgs(at: TypeRepr): List[TypeRepr] = {
      val t = stripAndType(at.dealias)
      val typeArgs: List[TypeRepr] = t match {
        case AppliedType(_, args) if isMap(t) =>
          args match {
            case head :: _ if !(stripAndType(head.dealias).dealias.typeSymbol == stringTypeRepr.dealias.typeSymbol) =>
              report.errorAndAbort(
                s"Unsupported Map key type in '${mainSymbol.name}': BSON documents only support String keys."
              )
            case _ :: tail => tail
            case _ => Nil
          }
        case AppliedType(_, args) => args
        case _ => Nil
      }
      val types = t +: typeArgs.flatMap(flattenTypeArgs)
      if (types.exists(isTuple)) report.errorAndAbort(
        s"Unsupported Tuple type in '${mainSymbol.name}'. Consider using a case class instead."
      )
      types.filterNot(isOption).map { x =>
        val stripped = stripAndType(x.dealias)
        primitiveTypesMap.getOrElse(stripped.typeSymbol, stripped)
      }
    }

    def classOfExpr(tpe: TypeRepr): Expr[Class[?]] = {
      Literal(ClassOfConstant(tpe)).asExprOf[Class[?]]
    }

    // ============================================================
    // Runtime Map Generation
    // ============================================================
    // These methods generate Expr values that produce the three runtime
    // maps needed by MacroCodec at runtime.

    /** Builds the map from class name -> Class[?] for all known concrete types. */
    def buildCaseClassNameToTypeMap: Expr[Map[String, Class[?]]] = {
      val entries = knownTypeSymbols.map { sym =>
        val name = Expr(resolveClassName(sym))
        val clazz = classOfExpr(sym.typeRef)
        '{ ($name, $clazz) }
      }
      val seq = Expr.ofList(entries)
      '{ $seq.toMap }
    }

    /** Builds the map from Class[?] -> Boolean indicating if the type is a case class/sealed type. */
    def buildTypeToCaseClassFlagMap: Expr[Map[Class[?], Boolean]] = {
      val allTypes = fieldsMap.toList.flatMap { case (sym, fields) =>
        fields.map(_.tpe) :+ sym.typeRef
      }
      val entries = allTypes.flatMap { t =>
        flattenTypeArgs(t).map { ft =>
          val clazz = classOfExpr(ft)
          val isCc = Expr(isCaseClassType(ft) || isCaseObjectType(ft) || isSealedType(ft))
          '{ ($clazz, $isCc) }
        }
      }
      val seq = Expr.ofList(entries)
      '{ $seq.toMap }
    }

    /** Builds the nested map: class name -> (field name -> list of type arg classes). */
    def buildFieldTypeArgsMap: Expr[Map[String, Map[String, List[Class[?]]]]] = {
      val outerEntries = fieldsMap.toList.map { case (sym, fields) =>
        val className = Expr(resolveClassName(sym))
        val innerEntries = fields
          .filterNot(field => ignoredFieldsMap.getOrElse(sym, Nil).exists(_.name == field.name))
          .map { field =>
            val fieldKey = Expr(resolveFieldName(field.name))
            val typeArgClasses = flattenTypeArgs(field.tpe).map(classOfExpr)
            val classList = Expr.ofList(typeArgClasses)
            '{ ($fieldKey, $classList) }
          }
        val innerMap = Expr.ofList(innerEntries)
        '{ ($className, $innerMap.toMap) }
      }
      val outerSeq = Expr.ofList(outerEntries)
      '{ $outerSeq.toMap }
    }

    // ============================================================
    // Write Code Generation
    // ============================================================
    // Generates the body of `writeCaseClassData`. For each known type,
    // generates an if/else chain that matches on className, then writes
    // each field using the BsonWriter.

    def accessFieldOnValueTerm(valueTerm: Term, classSym: Symbol, fieldName: String): Term = {
      classSym.typeRef.asType match {
        case '[ct] =>
          val castTerm = TypeApply(Select.unique(valueTerm, "asInstanceOf"), List(TypeTree.of[ct]))
          Select.unique(castTerm, fieldName)
      }
    }

    /** Generates write statements for a regular (non-Optional) field: writeName + writeFieldValue. */
    def buildWriteStatementsForField(
      codecTerm: Term, writerTerm: Term, encoderContextTerm: Term, keyLit: Term, fieldValueTerm: Term
    ): List[Term] = {
      val writeNameCall = Apply(Select.unique(writerTerm, "writeName"), List(keyLit))
      val writeFieldCall = Select.overloaded(codecTerm, "writeFieldValue", List(TypeRepr.of[Any]), List(keyLit, writerTerm, fieldValueTerm, encoderContextTerm))
      List(writeNameCall, writeFieldCall)
    }

    /**
     * Tries to generate a specialized write call for Scala value-type primitives (Int, Long, Double, Boolean).
     * Uses the two-arg BsonWriter convenience methods (e.g., `writeInt32(name, value)`) which combine
     * writeName + writeValue into a single call, avoiding the codec registry lookup entirely.
     *
     * String is intentionally excluded: it's a reference type that can be null, and the generic
     * writeFieldValue path provides a proper null check with a BsonInvalidOperationException.
     *
     * Returns None for non-primitive types, which should fall back to the generic writeFieldValue path.
     */
    def tryBuildPrimitiveWriteStatements(
      writerTerm: Term, keyLit: Term, fieldAccessTerm: Term, fieldType: TypeRepr
    ): Option[List[Term]] = {
      val stripped = stripAndType(fieldType.dealias)
      val writeMethodName: Option[String] = stripped.typeSymbol match {
        case sym if sym == TypeRepr.of[Int].typeSymbol => Some("writeInt32")
        case sym if sym == TypeRepr.of[Long].typeSymbol => Some("writeInt64")
        case sym if sym == TypeRepr.of[Double].typeSymbol => Some("writeDouble")
        case sym if sym == TypeRepr.of[Boolean].typeSymbol => Some("writeBoolean")
        case _ => None
      }
      writeMethodName.map { methodName =>
        val writeCall = Select.overloaded(writerTerm, methodName, Nil, List(keyLit, fieldAccessTerm))
        List(writeCall)
      }
    }

    /**
     * Generates write statements for an Optional field:
     * {{{
     *   val optVal = value.asInstanceOf[CT].fieldName
     *   if (optVal.isDefined) { writeName(key); writeFieldValue(key, writer, optVal.get, ctx) }
     *   else if (encodeNone)  { writeName(key); writeFieldValue(key, writer, BsonNull(), ctx) }
     *   else ()
     * }}}
     */
    def buildWriteStatementsForOptionalField(
      codecTerm: Term, writerTerm: Term, encoderContextTerm: Term, encodeNoneTerm: Term,
      keyLit: Term, fieldAccessTerm: Term, field: FieldInfo
    ): List[Statement] = {
      val unitLit = Literal(UnitConstant())
      val localValSym = Symbol.newVal(Symbol.spliceOwner, s"optVal_${field.name}", field.tpe.widen, Flags.EmptyFlags, Symbol.noSymbol)
      val localValDef = ValDef(localValSym, Some(fieldAccessTerm))
      val localValRef = Ref(localValSym)

      val isDefinedCall = Select.unique(localValRef, "isDefined")
      val getCall = Select.unique(localValRef, "get")

      val writeSome = buildWriteStatementsForField(codecTerm, writerTerm, encoderContextTerm, keyLit, getCall)
      val someBlock = if (writeSome.size == 1) writeSome.head else Block(writeSome.init, writeSome.last)

      val bsonNullType = TypeRepr.of[org.mongodb.scala.bson.BsonNull]
      val bsonNullInstance = Apply(
        Select(New(TypeTree.of[org.mongodb.scala.bson.BsonNull]), bsonNullType.typeSymbol.primaryConstructor),
        Nil
      )
      val writeNone = buildWriteStatementsForField(codecTerm, writerTerm, encoderContextTerm, keyLit, bsonNullInstance)
      val noneBlock = if (writeNone.size == 1) writeNone.head else Block(writeNone.init, writeNone.last)

      val ifElse = If(isDefinedCall, someBlock, If(encodeNoneTerm, noneBlock, unitLit))
      List[Statement](localValDef, ifElse)
    }

    def buildWriteBody(
      codec: Expr[MacroCodec[T]],
      className: Expr[String],
      writer: Expr[org.bson.BsonWriter],
      value: Expr[T],
      encoderContext: Expr[org.bson.codecs.EncoderContext]
    ): Expr[Unit] = {
      val codecTerm = codec.asTerm
      val classNameTerm = className.asTerm
      val writerTerm = writer.asTerm
      val valueTerm = value.asTerm
      val encoderContextTerm = encoderContext.asTerm
      val encodeNoneTerm = encodeNone.asTerm

      val unitLit = Literal(UnitConstant())

      def loop(types: List[Symbol]): Term = types match {
        case Nil =>
          Apply(Select.unique(codecTerm, "throwUnexpectedClass"), List(classNameTerm))

        case sym :: rest =>
          val nameLit = Literal(StringConstant(resolveClassName(sym)))
          val condition = Apply(Select.unique(classNameTerm, "=="), List(nameLit))

          val body: Term = if (isCaseObject(sym)) {
            unitLit
          } else {
            val fields = fieldsMap(sym)
            val ignoredNames = ignoredFieldsMap.getOrElse(sym, Nil).map(_.name).toSet
            val writeStmts: List[Statement] = fields.filterNot(field => ignoredNames.contains(field.name)).flatMap { field =>
              val keyLit = Literal(StringConstant(resolveFieldName(field.name)))
              val fieldAccessTerm = accessFieldOnValueTerm(valueTerm, sym, field.name)

              field.tpe.asType match {
                case '[Option[inner]] =>
                  buildWriteStatementsForOptionalField(codecTerm, writerTerm, encoderContextTerm, encodeNoneTerm, keyLit, fieldAccessTerm, field)
                case '[fieldType] =>
                  // Try specialized primitive write (avoids codec registry lookup), fall back to generic path
                  tryBuildPrimitiveWriteStatements(writerTerm, keyLit, fieldAccessTerm, field.tpe)
                    .getOrElse(buildWriteStatementsForField(codecTerm, writerTerm, encoderContextTerm, keyLit, fieldAccessTerm))
                    .map(t => t: Statement)
              }
            }
            if (writeStmts.isEmpty) unitLit
            else {
              val lastTerm = writeStmts.last.asInstanceOf[Term]
              if (writeStmts.size == 1) lastTerm
              else Block(writeStmts.init.toList, lastTerm)
            }
          }

          val elseBody = loop(rest)
          If(condition, body, elseBody)
      }

      val writeClassFieldNameCall = Apply(
        Select.unique(codecTerm, "writeClassFieldName"),
        List(writerTerm, classNameTerm, encoderContextTerm)
      )
      val writeFieldsTerm = loop(knownTypeSymbols)
      Block(List(writeClassFieldNameCall), writeFieldsTerm).asExprOf[Unit]
    }

    // ============================================================
    // Instance Creation Code Generation
    // ============================================================
    // Generates the body of `getInstance`. For each known type,
    // generates an if/else chain that matches on className, extracts
    // field values from the data map, and calls the case class constructor.

    def getDefaultValueTerm(classSym: Symbol, paramIndex: Int): Term = {
      val companion = classSym.companionModule
      val methodSym = companion.methodMember(s"$APPLY_DEFAULT_PREFIX${paramIndex + 1}").headOption
        .orElse(companion.methodMember(s"$INIT_DEFAULT_PREFIX${paramIndex + 1}").headOption)
        .getOrElse(report.errorAndAbort(
          s"No default value for parameter ${paramIndex + 1} on '${classSym.name}'."
        ))
      Ref(companion).select(methodSym)
    }

    /** Checks at compile time whether the given constructor parameter has a default value. */
    def hasDefaultValue(classSym: Symbol, paramIndex: Int): Boolean = {
      val companion = classSym.companionModule
      companion.methodMember(s"$APPLY_DEFAULT_PREFIX${paramIndex + 1}").nonEmpty ||
        companion.methodMember(s"$INIT_DEFAULT_PREFIX${paramIndex + 1}").nonEmpty
    }

    def buildGetInstanceBody(
      codec: Expr[MacroCodec[T]],
      className: Expr[String],
      fieldData: Expr[Map[String, Any]]
    ): Expr[T] = {
      val codecTerm = codec.asTerm
      val classNameTerm = className.asTerm
      val fieldDataTerm = fieldData.asTerm

      def buildFieldExtraction(field: FieldInfo, classSym: Symbol, paramIndex: Int): Term = {
        val keyStr = resolveFieldName(field.name)
        val keyLit = Literal(StringConstant(keyStr))

        field.tpe.asType match {
          case '[Option[inner]] =>
            val containsCall = Apply(Select.unique(fieldDataTerm, "contains"), List(keyLit))
            val applyCall = Apply(Select.unique(fieldDataTerm, "apply"), List(keyLit))
            val optionModule = Ref(TypeRepr.of[Option.type].termSymbol)
            val optionCall = Select.overloaded(optionModule, "apply", List(TypeRepr.of[Any]), List(applyCall))
            // When the field is absent: use the default value if one exists, otherwise None
            val fallback = if (hasDefaultValue(classSym, paramIndex)) {
              getDefaultValueTerm(classSym, paramIndex)
            } else {
              Ref(TypeRepr.of[None.type].termSymbol)
            }
            val ifExpr = If(containsCall, optionCall, fallback)
            TypeApply(Select.unique(ifExpr, "asInstanceOf"), List(TypeTree.of[Option[inner]]))

          case '[fieldType] =>
            // When the field is absent: use the default value if one exists, otherwise throw
            val fallback = if (hasDefaultValue(classSym, paramIndex)) {
              getDefaultValueTerm(classSym, paramIndex)
            } else {
              Apply(Select.unique(codecTerm, "throwMissingField"), List(keyLit))
            }
            val getOrElseCall = Select.overloaded(
              fieldDataTerm, "getOrElse",
              List(TypeRepr.of[Any]),
              List(keyLit, fallback)
            )
            TypeApply(Select.unique(getOrElseCall, "asInstanceOf"), List(TypeTree.of[fieldType]))
        }
      }

      def loop(types: List[Symbol]): Term = types match {
        case Nil =>
          Apply(Select.unique(codecTerm, "throwUnexpectedClass"), List(classNameTerm))

        case sym :: rest =>
          val nameLit = Literal(StringConstant(resolveClassName(sym)))
          val condition = Apply(Select.unique(classNameTerm, "=="), List(nameLit))

          val body: Term = if (isCaseObject(sym)) {
            Ref(sym.companionModule)
          } else {
            val fields = fieldsMap(sym)
            val ignoredFields = ignoredFieldsMap.getOrElse(sym, Nil)
            val ignoredNames = ignoredFields.map(_.name).toSet

            val args: List[Term] = fields.zipWithIndex.map { case (field, paramIdx) =>
              if (ignoredNames.contains(field.name)) {
                val idx = ignoredFields.find(_.name == field.name).get.paramIndex
                getDefaultValueTerm(sym, idx)
              } else {
                buildFieldExtraction(field, sym, paramIdx)
              }
            }

            sym.typeRef.asType match {
              case '[ct] =>
                Apply(
                  Select(New(TypeTree.of[ct]), sym.primaryConstructor),
                  args
                )
            }
          }

          val elseBody = loop(rest)
          If(condition, body, elseBody)
      }
      loop(knownTypeSymbols).asExprOf[T]
    }

    // ============================================================
    // Final Assembly
    // ============================================================
    // Assembles the generated code into an anonymous MacroCodec[T] instance.

    '{
      val _codecRegistry = $codecRegistry

      val codec = new MacroCodec[T] {
        val encoderClass = ${ Literal(ClassOfConstant(TypeRepr.of[T])).asExprOf[Class[T]] }
        val codecRegistry: CodecRegistry = _codecRegistry
        val caseClassesMap: Map[String, Class[?]] = ${ buildCaseClassNameToTypeMap }
        val classToCaseClassMap: Map[Class[?], Boolean] = ${ buildTypeToCaseClassFlagMap }
        val classFieldTypeArgsMap: Map[String, Map[String, List[Class[?]]]] = ${ buildFieldTypeArgsMap }

        def getInstance(className: String, fieldData: Map[String, Any]): T = {
          val __codec = this
          ${ buildGetInstanceBody('__codec, 'className, 'fieldData) }
        }

        def writeCaseClassData(className: String, writer: org.bson.BsonWriter, value: T, encoderContext: org.bson.codecs.EncoderContext): Unit = {
          writer.writeStartDocument()
          val codec = this
          ${ buildWriteBody('codec, 'className, 'writer, 'value, 'encoderContext) }
          writer.writeEndDocument()
        }
      }

      codec.asInstanceOf[Codec[T]]
    }
  }
  // scalastyle:on method.length
}
