package io.github.edadma.stow

import io.github.edadma.cross_platform.{RandomAccessFile, openRandomAccessFile, createTempFile, deleteFile}
import scala.collection.mutable.ArrayDeque

class FilePageStore private (file: RandomAccessFile, private var header: HeaderData, private var activeSlot: Int)
    extends PageStore:
  private val _pageSize = header.pageSize
  private val freeList = ArrayDeque[PageId]()
  private var activeTransaction: Option[TransactionImpl] = None

  // Build in-memory free list by walking the on-disk linked list
  locally {
    var page = header.freeListHead
    while page != NoPage do
      freeList.append(page)
      val data = readPage(page)
      page = readPageId(data, 0)
  }

  def pageSize: Int = _pageSize

  def metaRoot: PageId = header.metaRoot

  def read(id: PageId): Array[Byte] =
    require(id >= HeaderPages && id < header.pageCount, s"Invalid page id: $id")
    readPage(id)

  def modify(fn: WriteBatch => Unit): Unit =
    require(activeTransaction.isEmpty, "cannot call modify() while a transaction is active")
    val txn = beginTransaction()
    try
      fn(txn)
      txn.commit()
    catch
      case e: Throwable =>
        if txn.isActive then txn.rollback()
        throw e

  def beginTransaction(): Transaction =
    require(activeTransaction.isEmpty, "a transaction is already active")
    linkPendingPages()
    val txn = new TransactionImpl
    activeTransaction = Some(txn)
    txn

  def close(): Unit = file.close()

  private[stow] def completePendingReclamation(): Unit =
    var currentFreeHead = header.freeListHead

    for i <- header.pendingFree.indices do
      val page = header.pendingFree(i)
      val data = new Array[Byte](_pageSize)
      val nextFree = if i + 1 < header.pendingFree.length then header.pendingFree(i + 1) else currentFreeHead
      writePageId(data, 0, nextFree)
      writePage(page, data)
    currentFreeHead = header.pendingFree(0)

    file.fsync()

    // Write updated header with empty pending list
    val staleSlot = 1 - activeSlot
    val newHeader = HeaderData(
      version = header.version,
      pageSize = header.pageSize,
      pageCount = header.pageCount,
      epoch = header.epoch + 1,
      metaRoot = header.metaRoot,
      freeListHead = currentFreeHead,
      pendingFree = Array.empty,
    )
    val headerBytes = Header.write(newHeader, _pageSize)
    writePage(staleSlot, headerBytes)
    file.fsync()

    // Rebuild free list from disk
    freeList.clear()
    var page = currentFreeHead
    while page != NoPage do
      freeList.append(page)
      val d = readPage(page)
      page = readPageId(d, 0)

    header = newHeader
    activeSlot = staleSlot

  private var pendingLinked = false

  private def linkPendingPages(): Unit =
    if header.pendingFree.isEmpty || pendingLinked then return
    var currentFreeHead = header.freeListHead

    for i <- header.pendingFree.indices do
      val page = header.pendingFree(i)
      val data = new Array[Byte](_pageSize)
      val nextFree = if i + 1 < header.pendingFree.length then header.pendingFree(i + 1) else currentFreeHead
      writePageId(data, 0, nextFree)
      writePage(page, data)
    currentFreeHead = header.pendingFree(0)
    file.fsync()

    // Update in-memory free list: previous pending pages are now allocatable
    header.pendingFree.reverseIterator.foreach(freeList.prepend)
    pendingLinked = true
    // Note: we update freeListHead in memory for the commit to use
    header = header.copy(freeListHead = currentFreeHead)

  // -- internals --

  private def readPage(id: PageId): Array[Byte] =
    val data = new Array[Byte](_pageSize)
    file.seek(id.toLong * _pageSize)
    file.readFully(data)
    data

  private def writePage(id: PageId, data: Array[Byte]): Unit =
    file.seek(id.toLong * _pageSize)
    file.write(data, 0, _pageSize)

  private def readPageId(data: Array[Byte], off: Int): PageId =
    ((data(off) & 0xff) << 24) | ((data(off + 1) & 0xff) << 16) |
      ((data(off + 2) & 0xff) << 8) | (data(off + 3) & 0xff)

  private def writePageId(data: Array[Byte], off: Int, id: PageId): Unit =
    data(off) = ((id >> 24) & 0xff).toByte
    data(off + 1) = ((id >> 16) & 0xff).toByte
    data(off + 2) = ((id >> 8) & 0xff).toByte
    data(off + 3) = (id & 0xff).toByte

  private def commit(batch: WriteBatchImpl): Unit =
    val newMetaRoot = batch.newMetaRoot.getOrElse(header.metaRoot)
    val currentFreeHead = if freeList.nonEmpty then freeList.head else NoPage

    // Write data pages from the batch
    for (id, data) <- batch.written do
      writePage(id, data)

    // fsync data pages
    if batch.written.nonEmpty then file.fsync()

    // Write new header
    val staleSlot = 1 - activeSlot
    val pendingFree = batch.freed.toArray
    val newHeader = HeaderData(
      version = FormatVersion,
      pageSize = _pageSize,
      pageCount = header.pageCount + batch.extended,
      epoch = header.epoch + 1,
      metaRoot = newMetaRoot,
      freeListHead = currentFreeHead,
      pendingFree = pendingFree,
    )
    val headerBytes = Header.write(newHeader, _pageSize)
    writePage(staleSlot, headerBytes)

    // fsync header
    file.fsync()

    // Update in-memory state
    header = newHeader
    activeSlot = staleSlot
    pendingLinked = false

  private class WriteBatchImpl extends WriteBatch:
    val written = scala.collection.mutable.Map[PageId, Array[Byte]]()
    val freed = ArrayDeque[PageId]()
    val allocated = ArrayDeque[PageId]()
    var newMetaRoot: Option[PageId] = None
    var extended: Int = 0

    def allocate(): PageId =
      val id = if freeList.nonEmpty then
        freeList.removeHead()
      else
        val newId = header.pageCount + extended
        extended += 1
        // Extend the file
        val data = new Array[Byte](_pageSize)
        file.seek(newId.toLong * _pageSize)
        file.write(data, 0, _pageSize)
        newId
      allocated.append(id)
      id

    def read(id: PageId): Array[Byte] =
      written.getOrElse(id, FilePageStore.this.readPage(id))

    def write(id: PageId, data: Array[Byte]): Unit =
      require(data.length == _pageSize, s"Data size ${data.length} does not match page size ${_pageSize}")
      written(id) = data.clone()

    def free(id: PageId): Unit =
      require(id >= HeaderPages, s"Cannot free header page: $id")
      freed.append(id)
      written.remove(id)

    def setMetaRoot(id: PageId): Unit =
      newMetaRoot = Some(id)

  private class TransactionImpl extends Transaction:
    private val batch = new WriteBatchImpl
    private var _isActive = true

    def isActive: Boolean = _isActive

    private def requireActive(): Unit =
      require(_isActive, "transaction is no longer active")

    def allocate(): PageId =
      requireActive()
      batch.allocate()

    def read(id: PageId): Array[Byte] =
      requireActive()
      batch.read(id)

    def write(id: PageId, data: Array[Byte]): Unit =
      requireActive()
      batch.write(id, data)

    def free(id: PageId): Unit =
      requireActive()
      batch.free(id)

    def setMetaRoot(id: PageId): Unit =
      requireActive()
      batch.setMetaRoot(id)

    def commit(): Unit =
      requireActive()
      _isActive = false
      activeTransaction = None
      FilePageStore.this.commit(batch)

    def rollback(): Unit =
      requireActive()
      _isActive = false
      activeTransaction = None
      batch.allocated.foreach(freeList.prepend)

object FilePageStore:
  def create(path: String, pageSize: Int): FilePageStore =
    require(pageSize >= 64, s"Page size must be at least 64 bytes, got $pageSize")
    require((pageSize & (pageSize - 1)) == 0, s"Page size must be a power of 2, got $pageSize")

    val file = openRandomAccessFile(path, "rw")
    val header = HeaderData(
      version = FormatVersion,
      pageSize = pageSize,
      pageCount = HeaderPages,
      epoch = 0,
      metaRoot = NoPage,
      freeListHead = NoPage,
      pendingFree = Array.empty,
    )
    val headerBytes = Header.write(header, pageSize)

    // Write both header slots
    file.write(headerBytes)
    file.write(headerBytes)
    file.fsync()

    new FilePageStore(file, header, activeSlot = 0)

  def open(path: String): FilePageStore =
    val file = openRandomAccessFile(path, "rw")

    // Read page size from the first header candidate (offset 8, 4 bytes)
    file.seek(8)
    val pageSizeBytes = new Array[Byte](4)
    file.readFully(pageSizeBytes)
    val pageSize = ((pageSizeBytes(0) & 0xff) << 24) | ((pageSizeBytes(1) & 0xff) << 16) |
      ((pageSizeBytes(2) & 0xff) << 8) | (pageSizeBytes(3) & 0xff)

    require(pageSize >= 64, s"Invalid page size in file: $pageSize")

    // Read both header pages
    val page0 = new Array[Byte](pageSize)
    val page1 = new Array[Byte](pageSize)
    file.seek(0)
    file.readFully(page0)
    file.readFully(page1)

    val h0 = Header.read(page0)
    val h1 = Header.read(page1)

    val (header, activeSlot) = (h0, h1) match
      case (Some(a), Some(b)) =>
        if a.epoch >= b.epoch then (a, 0) else (b, 1)
      case (Some(a), None) => (a, 0)
      case (None, Some(b)) => (b, 1)
      case (None, None) =>
        file.close()
        throw new IllegalStateException("Corrupt page store: neither header is valid")

    val store = new FilePageStore(file, header, activeSlot)

    // Complete deferred reclamation if there are pending free pages
    if header.pendingFree.nonEmpty then
      store.completePendingReclamation()

    store
