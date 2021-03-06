// Databricks notebook source
import org.apache.spark.eventhubs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._

val readConnectionString = ConnectionStringBuilder("Endpoint=sb://marslandingdeveventhubs.servicebus.windows.net/;SharedAccessKeyName=AzureStreamingPOCApp;SharedAccessKey=W/7yAHm4LO6SNMW/dN9XUQ45X1TYclFTOTTnbuD65/k=")
  .setEventHubName("marslandingtwitterpoc")
  .build

val readEventHubsConf = EventHubsConf(readConnectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)

val incomingStream = 
  spark.readStream
    .format("eventhubs")
    .options(readEventHubsConf.toMap)
    .load()

var messages =
  incomingStream
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Body", $"body".cast(StringType))
  .select("Offset", "Time (readable)", "Timestamp", "Body")
  .withColumn("User", get_json_object(col("body"), "$.userscreenname"))
  .withColumn("Tweet", get_json_object(col("body"), "$.text"))
  .withColumn("TweetId", get_json_object(col("body"), "$.id"))
  .withColumn("UserName", get_json_object(get_json_object(col("body"), "$.user"), "$.name"))
  .withColumn("UserHandle", get_json_object(get_json_object(col("body"), "$.user"), "$.screen_name"))
  .withColumn("UserFollowers", get_json_object(get_json_object(col("body"), "$.user"), "$.followers_count"))

var formattedMessages = messages.select("TweetId","User","Tweet","UserName","UserHandle","UserFollowers","Time (readable)")

//formattedMessages.writeStream.outputMode("append").format("console").queryName("EventHubsSparkStreamingPOCQuery").option("truncate", false).start().awaitTermination()



// COMMAND ----------

import java.io._
import java.net._
import java.util._

class Document(var id: String, var text: String, var language: String = "", var sentiment: Double = 0.0) extends Serializable

class Documents(var documents: List[Document] = new ArrayList[Document]()) extends Serializable {

    def add(id: String, text: String, language: String = "") {
        documents.add (new Document(id, text, language))
    }
    def add(doc: Document) {
        documents.add (doc)
    }
}

// COMMAND ----------

class CC[T] extends Serializable { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }
object M extends CC[scala.collection.immutable.Map[String, Any]]
object L extends CC[scala.collection.immutable.List[Any]]
object S extends CC[String]
object D extends CC[Double]

// COMMAND ----------

import javax.net.ssl.HttpsURLConnection
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import scala.util.parsing.json._

object SentimentDetector extends Serializable {

  // Cognitive Services API connection settings
  val accessKey = "42a825b30927477cafb4a5c5562212da"
  val host = "https://eastus2.api.cognitive.microsoft.com"
  val languagesPath = "/text/analytics/v2.0/languages"
  val sentimentPath = "/text/analytics/v2.0/sentiment"
  val languagesUrl = new URL(host+languagesPath)
  val sentimenUrl = new URL(host+sentimentPath)

  def getConnection(path: URL): HttpsURLConnection = {
    val connection = path.openConnection().asInstanceOf[HttpsURLConnection]
    connection.setRequestMethod("POST")
    connection.setRequestProperty("Content-Type", "text/json")
    connection.setRequestProperty("Ocp-Apim-Subscription-Key", accessKey)
    connection.setDoOutput(true)
    return connection
  }

  def prettify (json_text: String): String = {
    val parser = new JsonParser()
    val json = parser.parse(json_text).getAsJsonObject()
    val gson = new GsonBuilder().setPrettyPrinting().create()
    return gson.toJson(json)
  }

  // Handles the call to Cognitive Services API.
  // Expects Documents as parameters and the address of the API to call.
  // Returns an instance of Documents in response.
  def processUsingApi(inputDocs: Documents, path: URL): String = {
    val docText = new Gson().toJson(inputDocs)
    val encoded_text = docText.getBytes("UTF-8")
    val connection = getConnection(path)
    val wr = new DataOutputStream(connection.getOutputStream())
    wr.write(encoded_text, 0, encoded_text.length)
    wr.flush()
    wr.close()

    val response = new StringBuilder()
    val in = new BufferedReader(new InputStreamReader(connection.getInputStream()))
    var line = in.readLine()
    while (line != null) {
        response.append(line)
        line = in.readLine()
    }
    in.close()
    return response.toString()
  }

  // Calls the language API for specified documents.
  // Returns a documents with language field set.
  def getLanguage (inputDocs: Documents): Documents = {
    try {
      val response = processUsingApi(inputDocs, languagesUrl)
      // In case we need to log the json response somewhere
      val niceResponse = prettify(response)
      val docs = new Documents()
      val result = for {
            // Deserializing the JSON response from the API into Scala types
            Some(M(map)) <- scala.collection.immutable.List(JSON.parseFull(niceResponse))
            L(documents) = map("documents")
            M(document) <- documents
            S(id) = document("id")
            L(detectedLanguages) = document("detectedLanguages")
            M(detectedLanguage) <- detectedLanguages
            S(language) = detectedLanguage("iso6391Name")
      } yield {
            docs.add(new Document(id = id, text = id, language = language))
      }
      return docs
    } catch {
          case e: Exception => return new Documents()
    }
  }

  // Calls the sentiment API for specified documents. Needs a language field to be set for each of them.
  // Returns documents with sentiment field set, taking a value in the range from 0 to 1.
  def getSentiment (inputDocs: Documents): Documents = {
    try {
      val response = processUsingApi(inputDocs, sentimenUrl)
      val niceResponse = prettify(response)
      val docs = new Documents()
      val result = for {
            // Deserializing the JSON response from the API into Scala types
            Some(M(map)) <- scala.collection.immutable.List(JSON.parseFull(niceResponse))
            L(documents) = map("documents")
            M(document) <- documents
            S(id) = document("id")
            D(sentiment) = document("score")
      } yield {
            docs.add(new Document(id = id, text = id, sentiment = sentiment))
      }
      return docs
    } catch {
        case e: Exception => return new Documents()
    }
  }
}

// User Defined Function for processing content of messages to return their sentiment.
val toSentiment = udf((textContent: String) => {
  val inputDocs = new Documents()
  inputDocs.add (textContent, textContent)
  val docsWithLanguage = SentimentDetector.getLanguage(inputDocs)
  val docsWithSentiment = SentimentDetector.getSentiment(docsWithLanguage)
  if (docsWithLanguage.documents.isEmpty) {
    // Placeholder value to display for no score returned by the sentiment API
    (-1).toDouble
  } else {
    docsWithSentiment.documents.get(0).sentiment.toDouble
  }
})

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

// Prepare a dataframe with Content and Sentiment columns
val streamingDataFrame = formattedMessages.select("*").withColumn("Sentiment", toSentiment($"Tweet").cast(FloatType))

val aggStreamingDataFrame = streamingDataFrame.groupBy(col("TweetId")).sum("Sentiment")

// Display the streaming data with the sentiment
//streamingDataFrame.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
//aggStreamingDataFrame.writeStream.outputMode("complete").format("memory").trigger(Trigger.ProcessingTime("10 seconds")).queryName("Tweets").start().awaitTermination()

// COMMAND ----------

val writeConnectionString = ConnectionStringBuilder("Endpoint=sb://marslandingdeveventhubs.servicebus.windows.net/;SharedAccessKeyName=SocialListeningPolicy;SharedAccessKey=ipARG359yjSaomNjQzSMn9AmBJS7cC5DdIC+mJtzkJI=")
  .setEventHubName("sociallisteningsentiment")
  .build

val writeEventHubsConf = EventHubsConf(writeConnectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)

var streamingDataFrameJson = streamingDataFrame.selectExpr("CAST(TweetId AS STRING) AS key", "to_json(struct(*)) AS body")
//streamingDataFrameJson.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

val query =
  streamingDataFrameJson
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .options(writeEventHubsConf.toMap)
    .option("checkpointLocation", "/checkpoint/")
    .start()

