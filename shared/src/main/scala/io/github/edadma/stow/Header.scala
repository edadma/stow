package io.github.edadma.stow

object Header:
  // Fixed field offsets
  val MagicOffset = 0        // 4 bytes
  val VersionOffset = 4      // 4 bytes
  val PageSizeOffset = 8     // 4 bytes
  val PageCountOffset = 12   // 4 bytes
  val EpochOffset = 16       // 8 bytes
  val MetaRootOffset = 24    // 4 bytes
  val FreeListHeadOffset = 28 // 4 bytes
  val PendingCountOffset = 32 // 2 bytes
  val PendingFreeOffset = 34  // N * 4 bytes, then checksum at end

  def maxPendingFree(pageSize: Int): Int =
    // Space available after fixed fields, minus 4 bytes for checksum
    (pageSize - PendingFreeOffset - 4) / 4

  def read(data: Array[Byte]): Option[HeaderData] =
    if data.length < PendingFreeOffset + 4 then return None

    // Verify magic
    if data(0) != Magic(0) || data(1) != Magic(1) || data(2) != Magic(2) || data(3) != Magic(3) then
      return None

    val pageSize = readInt(data, PageSizeOffset)
    val pendingCount = readShort(data, PendingCountOffset)

    if pendingCount < 0 || pendingCount > maxPendingFree(pageSize) then return None

    // Verify checksum: checksum is at PendingFreeOffset + pendingCount * 4
    val checksumOffset = PendingFreeOffset + pendingCount * 4
    if checksumOffset + 4 > data.length then return None

    val storedChecksum = readInt(data, checksumOffset)
    val computedChecksum = CRC32.compute(data, 0, checksumOffset)
    if storedChecksum != computedChecksum then return None

    val pendingFree = new Array[PageId](pendingCount)
    for i <- 0 until pendingCount do
      pendingFree(i) = readInt(data, PendingFreeOffset + i * 4)

    Some(HeaderData(
      version = readInt(data, VersionOffset),
      pageSize = pageSize,
      pageCount = readInt(data, PageCountOffset),
      epoch = readLong(data, EpochOffset),
      metaRoot = readInt(data, MetaRootOffset),
      freeListHead = readInt(data, FreeListHeadOffset),
      pendingFree = pendingFree,
    ))

  def write(h: HeaderData, pageSize: Int): Array[Byte] =
    val data = new Array[Byte](pageSize)

    // Magic
    data(0) = Magic(0); data(1) = Magic(1); data(2) = Magic(2); data(3) = Magic(3)

    writeInt(data, VersionOffset, h.version)
    writeInt(data, PageSizeOffset, h.pageSize)
    writeInt(data, PageCountOffset, h.pageCount)
    writeLong(data, EpochOffset, h.epoch)
    writeInt(data, MetaRootOffset, h.metaRoot)
    writeInt(data, FreeListHeadOffset, h.freeListHead)
    writeShort(data, PendingCountOffset, h.pendingFree.length.toShort)

    for i <- h.pendingFree.indices do
      writeInt(data, PendingFreeOffset + i * 4, h.pendingFree(i))

    // Checksum covers everything up to (but not including) the checksum field
    val checksumOffset = PendingFreeOffset + h.pendingFree.length * 4
    val checksum = CRC32.compute(data, 0, checksumOffset)
    writeInt(data, checksumOffset, checksum)

    data

  // -- byte array helpers --

  private def readInt(data: Array[Byte], off: Int): Int =
    ((data(off) & 0xff) << 24) | ((data(off + 1) & 0xff) << 16) |
      ((data(off + 2) & 0xff) << 8) | (data(off + 3) & 0xff)

  private def readShort(data: Array[Byte], off: Int): Int =
    ((data(off) & 0xff) << 8) | (data(off + 1) & 0xff)

  private def readLong(data: Array[Byte], off: Int): Long =
    ((data(off).toLong & 0xff) << 56) | ((data(off + 1).toLong & 0xff) << 48) |
      ((data(off + 2).toLong & 0xff) << 40) | ((data(off + 3).toLong & 0xff) << 32) |
      ((data(off + 4).toLong & 0xff) << 24) | ((data(off + 5).toLong & 0xff) << 16) |
      ((data(off + 6).toLong & 0xff) << 8) | (data(off + 7).toLong & 0xff)

  private def writeInt(data: Array[Byte], off: Int, v: Int): Unit =
    data(off) = ((v >> 24) & 0xff).toByte
    data(off + 1) = ((v >> 16) & 0xff).toByte
    data(off + 2) = ((v >> 8) & 0xff).toByte
    data(off + 3) = (v & 0xff).toByte

  private def writeShort(data: Array[Byte], off: Int, v: Short): Unit =
    data(off) = ((v >> 8) & 0xff).toByte
    data(off + 1) = (v & 0xff).toByte

  private def writeLong(data: Array[Byte], off: Int, v: Long): Unit =
    data(off) = ((v >> 56) & 0xff).toByte
    data(off + 1) = ((v >> 48) & 0xff).toByte
    data(off + 2) = ((v >> 40) & 0xff).toByte
    data(off + 3) = ((v >> 32) & 0xff).toByte
    data(off + 4) = ((v >> 24) & 0xff).toByte
    data(off + 5) = ((v >> 16) & 0xff).toByte
    data(off + 6) = ((v >> 8) & 0xff).toByte
    data(off + 7) = (v & 0xff).toByte

case class HeaderData(
    version: Int,
    pageSize: Int,
    pageCount: Int,
    epoch: Long,
    metaRoot: PageId,
    freeListHead: PageId,
    pendingFree: Array[PageId],
)
