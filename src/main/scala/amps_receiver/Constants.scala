package amps_receiver

object Constants {

  /**
    * <p>
    * Configuration key used to get a comma-separated list of headers that
    * should be added to each Flume event created from an AMPS message.
    * Possible values include:
    * </p>
    *
    * <ul>
    *
    * <li>command - The command type of the AMPS message.</li>
    *
    * <li>topic - The topic name of the AMPS message.</li>
    *
    * <li>sowKey - The SOW key of the AMPS message. Only set on the message
    * for SOW and SOW and Subscribe queries.</li>
    *
    * <li>timestamp - The ISO-8601 timestamp of when AMPS processed the
    * message. Only set on the message when the query or subscription specifies
    * the 'timestamp' option.</li>
    *
    * <li>bookmark - The unique bookmark string of the AMPS message. Only
    * set on the message for bookmark subscriptions.</li>
    *
    * <li>correlationId - The correlation Id of the AMPS message.</li>
    *
    * <li>subId - The subscription Id of the AMPS message.</li>
    *
    * <li>length - The length of the AMPS message body in bytes.</li>
    *
    * <li>currTimestamp - The current timestamp of when the AMPS Flume source
    * received the AMPS message. Its value is the number of milliseconds
    * since the system clock's epoch, represented as a string.</li>
    *
    * </ul>
    */
  val EVENT_HEADERS = "eventHeaders"
  /**
    * Event header key for the message command type.
    */
  val COMMAND_HEADER = "command"
  /**
    * Event header key for the message topic.
    */
  val TOPIC_HEADER = "topic"
  /**
    * Event header key for the message .
    */
  val SOW_KEY_HEADER = "sowKey"
  /**
    * Event header key for the message's ISO-8601 timestamp of when it was
    * processed by the AMPS server.
    */
  val AMPS_TIMESTAMP_HEADER = "ampsTimestamp"
  /**
    * Event header key for the message bookmark.
    */
  val BOOKMARK_HEADER = "bookmark"
  /**
    * Event header key for the message correlationId.
    */
  val CORRELATION_ID_HEADER = "correlationId"
  /**
    * Event header key for the message's subscription Id.
    */
  val SUB_ID_HEADER = "subId"
  /**
    * Event header key for the message body length in bytes.
    */
  val LENGTH_HEADER = "length"
  /**
    * Event header key for the message's current timestamp when it was received
    * by the AMPS Flume Source.
    */
  val TIMESTAMP_HEADER = "timestamp"


  val KNOWN_HEADERS = Set(COMMAND_HEADER,
                      TOPIC_HEADER,
                      SOW_KEY_HEADER,
                      AMPS_TIMESTAMP_HEADER,
                      BOOKMARK_HEADER,
                      CORRELATION_ID_HEADER,
                      SUB_ID_HEADER,
                      LENGTH_HEADER,
                      TIMESTAMP_HEADER)
}
