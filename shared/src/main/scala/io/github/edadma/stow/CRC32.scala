package io.github.edadma.stow

object CRC32:
  private val table: Array[Int] =
    val t = new Array[Int](256)
    for i <- 0 until 256 do
      var crc = i
      for _ <- 0 until 8 do
        if (crc & 1) != 0 then crc = (crc >>> 1) ^ 0xedb88320
        else crc = crc >>> 1
      t(i) = crc
    t

  def compute(data: Array[Byte], offset: Int, length: Int): Int =
    var crc = 0xffffffff
    var i = offset
    val end = offset + length
    while i < end do
      crc = table((crc ^ data(i)) & 0xff) ^ (crc >>> 8)
      i += 1
    crc ^ 0xffffffff
