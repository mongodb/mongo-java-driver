
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

import java.io.PrintWriter
import java.util.logging.Level

import scala.concurrent.{Future, Promise}

import com.mongodb.{ConnectionString, ReadPreference => JReadPreference}

import org.mongodb.scala._


/**
 * An example program providing similar functionality as the `mongoexport` program
 *
 * As there is no core CSV library for Scala CSV export is an exercise left to the reader.
 *
 * Add mongo-scala-driver-alldep jar to your path or add to ./lib directory and then run as a shell program::
 *
 * {{{
 *  ./mongoexport.scala -u mongodb://localhost/test.testData > ./data/testData.json
 * }}}
 *
 * Alternatively, run the `main` method in an IDE and pass the arguments in.
 *
 */
object mongoexport {
  val usage = """
                |Export MongoDB data to JSON files.
                |
                |Example:
                |  ./mongoexport.scala -u mongodb://localhost/test.testData > ./data/testData.json
                |
                |Options:
                |  --help                                produce help message
                |  --quiet                               silence all non error diagnostic messages
                |  -u [ --uri ] arg                      The connection URI - must contain a collection
                |                                        mongodb://[username:password@]host1[:port1][,host2[:port2]]/database.collection[?options]
                |                                        See: http://docs.mongodb.org/manual/reference/connection-string/
                |  -f [ --fields ] arg                   comma separated list of field names e.g. -f name,age
                |  -q [ --query ] arg                    query filter, as a JSON string, e.g. '{x:{$gt:1}}'
                |  -o [ --out ] arg                      output file; if not specified, stdout is used
                |  -k [ --slaveOk ] arg (=1)             use secondaries for export if available, default true
                |  --skip arg (=0)                       documents to skip, default 0
                |  --limit arg (=0)                      limit the numbers of documents
                |                                        returned, default all
                |  --sort arg                            sort order, as a JSON string, e.g.,
                |                                        '{x:1}'
              """.stripMargin

  /**
   * The main export program
   * Outputs debug information to Console.err - as Console.out is probably redirected to a file
   *
   * @param args the commandline arguments
   */
  def main(args: Array[String]) {
    // The time when the execution of this program started, in milliseconds since 1 January 1970 UTC.
    val executionStart: Long = currentTime

    if (args.length == 0 | args.contains("--help")) {
      Console.err.println(usage)
      sys.exit(1)
    }

    // Set the debug log level
    java.util.logging.Logger.getLogger("").getHandlers.foreach(h => h.setLevel(Level.WARNING))


    val optionMap = parseArgs(Map(), args.toList)
    val options = getOptions(optionMap)

    if (options.uri.isEmpty) {
      Console.err.println("Missing URI")
      Console.err.println(usage)
      sys.exit(1)
    }

    // Get URI
    val mongoClientURI = new ConnectionString(options.uri.get)
    if (Option(mongoClientURI.getCollection).isEmpty) {
      Console.err.println(s"Missing collection name in the URI eg:  mongodb://<hostInformation>/<database>.<collection>[?options]")
      Console.err.println(s"Current URI: $mongoClientURI")
      sys.exit(1)
    }

    // Get the collection
    val mongoClient = MongoClient(mongoClientURI.getURI)
    val database = mongoClient.getDatabase(mongoClientURI.getDatabase)
    val readPreference: ReadPreference = options.slaveOK match {
      case true => JReadPreference.secondaryPreferred()
      case false => JReadPreference.primaryPreferred()
    }
    val collection = database.getCollection(mongoClientURI.getCollection).withReadPreference(readPreference)

    // output
    val output: PrintWriter = options.out match {
      case None => new PrintWriter(System.out)
      case Some(fileName) => new PrintWriter(fileName)
    }


    // Export JSON
    if (!options.quiet) Console.err.println("Exporting...")
    val exporter = exportJson(collection, output, options)
    showPinWheel(exporter)
    val total = currentTime - executionStart
    if (!options.quiet) Console.err.println(s"Finished export: $total ms")
  }

  /**
   * Exports JSON from the collection
   *
   * @param collection the collection to import into
   * @param output the data source
   * @param options the configuration options
   */
  private def exportJson(collection: MongoCollection[Document], output: PrintWriter, options: Options): Future[Completed] = {
    val promise = Promise[Completed]()
    val cursor = collection.find(options.query)
    options.skip match {
      case None => cursor
      case Some(value) => cursor.skip(value)
    }
    options.limit match {
      case None => cursor
      case Some(value) => cursor.limit(value)
    }
    options.sort match {
      case None => cursor
      case Some(value) => cursor.sort(value)
    }

    output.write("")
    cursor.subscribe(
      (doc: Document) => output.println(doc.toJson()),
      (t: Throwable) => promise.failure(t),
      () => {
        output.close()
        promise.success(Completed())
      }
    )
    promise.future
  }

  /**
   * Recursively convert the args list into a Map of options
   *
   * @param map - the initial option map
   * @param args - the args list
   * @return the parsed OptionMap
   */
  private def parseArgs(map: Map[String, Any], args: List[String]): Map[String, Any] = {
    args match {
      case Nil => map
      case "--quiet" :: tail =>
        parseArgs(map ++ Map("quiet" -> true), tail)
      case "-u" :: value :: tail =>
        parseArgs(map ++ Map("uri" -> value), tail)
      case "--uri" :: value :: tail =>
        parseArgs(map ++ Map("uri" -> value), tail)
      case "-f" :: value :: tail =>
        parseArgs(map ++ Map("fields" -> value.split(",").toList), tail)
      case "--file" :: value :: tail =>
        parseArgs(map ++ Map("fields" -> value.split(",").toList), tail)
      case "-q" :: value :: tail =>
        parseArgs(map ++ Map("query" -> value), tail)
      case "--query" :: value :: tail =>
        parseArgs(map ++ Map("query" -> value), tail)
      case "-o" :: value :: tail =>
        parseArgs(map ++ Map("out" -> value), tail)
      case "--out" :: value :: tail =>
        parseArgs(map ++ Map("out" -> value), tail)
      case "-k" :: value :: tail =>
        parseArgs(map ++ Map("slaveOk" -> value), tail)
      case "--slaveOk" :: value :: tail =>
        parseArgs(map ++ Map("slaveOk" -> value), tail)
      case "--skip" :: value :: tail =>
        parseArgs(map ++ Map("skip" -> value), tail)
      case "--limit" :: value :: tail =>
        parseArgs(map ++ Map("limit" -> value), tail)
      case "--sort" :: value :: tail =>
        parseArgs(map ++ Map("sort" -> value), tail)
      case option :: tail =>
        Console.err.println("Unknown option " + option)
        Console.err.println(usage)
        sys.exit(1)
    }
  }

  /**
   * Convert the optionMap to an Options instance
   * @param optionMap the parsed args options
   * @return Options instance
   */
  private def getOptions(optionMap: Map[String, _]): Options = {
    val default = Options()
    val options = Options(
      quiet = optionMap.getOrElse("quiet", default.quiet).asInstanceOf[Boolean],
      uri = optionMap.get("uri") match {
        case None => default.uri
        case Some(value) => Some(value.asInstanceOf[String])
      },
      out = optionMap.get("out") match {
        case None => default.out
        case Some(value) => Some(value.asInstanceOf[String])
      },
      slaveOK = optionMap.getOrElse("slaveOK", default.slaveOK).asInstanceOf[Boolean],
      fields = optionMap.get("fields") match {
        case None => default.fields
        case Some(value) => Some(value.asInstanceOf[List[String]])
      },
      query = optionMap.get("query") match {
        case None => default.query
        case Some(value) => Document(value.asInstanceOf[String])
      },
      sort = optionMap.get("sort") match {
        case None => default.sort
        case Some(value) => Some(Document(value.asInstanceOf[String]))
      },
      skip = optionMap.get("skip") match {
        case None => default.skip
        case Some(value) => Some(value.asInstanceOf[Int])
      },
      limit = optionMap.get("limit") match {
        case None => default.limit
        case Some(value) => Some(value.asInstanceOf[Int])
      }
    )
    options
  }

  case class Options(quiet: Boolean = false, uri: Option[String] = None, out: Option[String] = None,
                     slaveOK: Boolean = true, fields: Option[List[String]] = None, query: Document = Document(),
                     sort: Option[Document] = None, skip: Option[Int] = None, limit: Option[Int] = None)

  private def currentTime = System.currentTimeMillis()

  /**
   * Shows a pinWheel in the console.err
   * @param someFuture the future we are all waiting for
   */
  private def showPinWheel(someFuture: Future[_]) {
    // Let the user know something is happening until futureOutput isCompleted
    val spinChars = List("|", "/", "-", "\\")
    while (!someFuture.isCompleted) {
      spinChars.foreach({
        case char =>
          Console.err.print(char)
          Thread sleep 200
          Console.err.print("\b")
      })
    }
    Console.err.println("")
  }

}

mongoexport.main(args)
