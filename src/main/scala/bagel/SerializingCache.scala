package bagel

import spark._

class SerializingCache extends Cache {
  private val cache: BoundedMemoryCache = new BoundedMemoryCache

  override def get(key: Any): Any = {
    val entry = cache.get(key)
    if (entry != null)
      Utils.deserialize(entry.asInstanceOf[Array[Byte]])
    else
      null
  }

  override def put(key: Any, value: Any) {
    cache.put(key, Utils.serialize(value))
  }
}
