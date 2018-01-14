package amps_receiver

import com.crankuptheamps.client._
import com.crankuptheamps.client.fields.CommandField
import amps_receiver.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import Message.Command._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class AMPSMessage(bookmarkSeqNo: Long,
                       data: Array[Byte],
                       headers: Map[String, String])

object AMPSSparkReceiver extends Logging {

  def main(args: Array[String]) {

    // Configure logging
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming example. To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val byteStream = ssc.receiverStream(new AMPSSparkReceiver(this.getClass.getSimpleName,
                                                              List ("tcp://34.201.116.96:9007/amps/json"),
                                                              "test",
                                                              false,  // demo server has only one node
                                                              "sow_and_subscribe"))

    // Define processing stages
    val words = byteStream.map(x => (x.data.map(_.toChar)).mkString)
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    // Start application
    ssc.start()
    ssc.awaitTermination()

  }
}


class AMPSSparkReceiver(clientName: String,
                        serverURIs: List[String],
                        topic : String,
                        useHA: Boolean = true,
                        cmdType: String = "subscribe",

                        // advanced parameters
                        cmdTypeFilter: Int = Publish | SOW | DeltaPublish | OOF,
                        evtHeaders : Set[String] = KNOWN_HEADERS,
                        storePath: Option[String] = None,
                        options : Option[String] = None,
                        filter : Option[String] = None,
                        bookmarkLog: Option[String] = None,
                        subId : Option[String] = None
                    )

  extends Receiver[AMPSMessage](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  var clientOpt = None: Option[Client]

  def onStart() {

    if (useHA) {
      clientOpt = createHAClient
    }
    else {
      clientOpt = createClient
    }

    logInfo("Created AMPS client and connected to server")

    val command = new Command(cmdType).setTopic(topic)
    filter.fold()(command.setFilter(_))
    options.fold()(command.setOptions(_))

    if (bookmarkLog.isDefined) {
      if (!subId.isDefined) {
        throw new IllegalArgumentException("Subscription ID must be set when using a bookmark subscription.")
      }
      else {
        command.setSubId(subId.get).setBookmark(Client.Bookmarks.MOST_RECENT)
      }
    }

    // check event headers for validity
    val badKeys = evtHeaders.diff(KNOWN_HEADERS)
    if(!badKeys.isEmpty)
    {
       throw new IllegalArgumentException("Invalid event header " + "key(s): " + badKeys)
    }

    clientOpt.fold()(_.executeAsync(command, AMPSMessageHandler))

  }

  def onStop() {
    logDebug("Entering onStop() -------------------------------------")
      for (client <- clientOpt) {
        try {
          client.unsubscribe()
          client.close()

          val bStore = client.getBookmarkStore
          if (bStore.isInstanceOf[LoggedBookmarkStore]) {
            bStore.asInstanceOf[LoggedBookmarkStore].close()
          }
          else if (bStore.isInstanceOf[RingBookmarkStore]) {
            bStore.asInstanceOf[RingBookmarkStore].close()
          }
        } catch {
          case e: Exception =>
            throw new Exception("AMPS Spark Source failed to stop due to: " + e, e)
        } finally {
            logDebug("Leaving onStop() ----------------------------------")
        }
      }
  }

  object AMPSMessageHandler extends MessageHandler {
    override def invoke(message: Message): Unit = {
      processMessage(message)
    }
  }

  private def processMessage(msg: Message) = {
    if ((msg.getCommand & cmdTypeFilter) == 0) {
      logDebug(s"Filtering message out of source by command type: cmdType=${msg.getCommand}, cmdTypeFilter=${cmdTypeFilter}")
    }
    else {
      val data = msg.getDataRaw
      val ba = new Array[Byte](data.length)
      System.arraycopy(data.buffer, data.position, ba, 0, data.length)
      val headers = getEventHeaders(msg)
      val m = AMPSMessage(msg.getBookmarkSeqNo, ba, headers)
      if (!isStopped) {
        store(m)
      }
    }
  }

  private def createHAClient : Option[Client]= {
    val client = new HAClient(clientName)
    val svrChooser = new DefaultServerChooser()
    serverURIs.foreach(svrChooser.add(_))
    client.setServerChooser(svrChooser)
    storePath.fold()(x => client.setBookmarkStore(new LoggedBookmarkStore(x)))
    client.connectAndLogon()
    Some(client)
  }

  private def createClient : Option[Client] = {
    val client = new Client(clientName)
    storePath.fold()(x => client.setBookmarkStore(new LoggedBookmarkStore(x)))
    client.connect(serverURIs(0))
    client.logon()
    Some(client)
  }

  private def getEventHeaders(msg: Message): Map[String, String] = {
    val hdrs = collection.mutable.Map[String, String]()

    if (evtHeaders.contains(COMMAND_HEADER))        hdrs(COMMAND_HEADER) = CommandField.encodeCommand(msg.getCommand)
    if (evtHeaders.contains(TOPIC_HEADER))          hdrs(TOPIC_HEADER) = msg.getTopic
    if (evtHeaders.contains(SOW_KEY_HEADER))        hdrs(SOW_KEY_HEADER) = msg.getSowKey
    if (evtHeaders.contains(AMPS_TIMESTAMP_HEADER)) hdrs(AMPS_TIMESTAMP_HEADER) = msg.getTimestamp
    if (evtHeaders.contains(BOOKMARK_HEADER))       hdrs(BOOKMARK_HEADER) = msg.getBookmark
    if (evtHeaders.contains(CORRELATION_ID_HEADER)) hdrs(CORRELATION_ID_HEADER) = msg.getCorrelationId
    if (evtHeaders.contains(SUB_ID_HEADER))         hdrs(SUB_ID_HEADER) = msg.getSubId
    if (evtHeaders.contains(LENGTH_HEADER))         hdrs(LENGTH_HEADER) = String.valueOf(msg.getLength)
    if (evtHeaders.contains(TIMESTAMP_HEADER))      hdrs(TIMESTAMP_HEADER) = String.valueOf(System.currentTimeMillis)

    hdrs.toMap
  }

}
// scalastyle:on println