package bagel

import spark._

class SerializingCache extends Cache with Logging {
  private val cache: BoundedMemoryCache = new BoundedMemoryCache

  override def get(key: Any): Any = {
    val entry = cache.get(key)
    if (entry != null) {
      val startTime = System.currentTimeMillis
      val result = Utils.deserialize[Any](entry.asInstanceOf[Array[Byte]])
      val timeTaken = System.currentTimeMillis - startTime
      logInfo("Deserialization for key %s took %d ms".format(key, timeTaken))
      result
    }
    else
      null
  }

  override def put(key: Any, value: Any) {
    val startTime = System.currentTimeMillis
    val result = Utils.serialize(value)
    val timeTaken = System.currentTimeMillis - startTime
    logInfo("Serialization for key %s took %d ms".format(key, timeTaken))

    cache.put(key, result)
  }
}
