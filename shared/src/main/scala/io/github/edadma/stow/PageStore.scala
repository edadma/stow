package io.github.edadma.stow

trait PageStore:
  def pageSize: Int
  def read(id: PageId): Array[Byte]
  def modify(fn: WriteBatch => Unit): Unit
  def metaRoot: PageId
  def close(): Unit

trait WriteBatch:
  def allocate(): PageId
  def read(id: PageId): Array[Byte]
  def write(id: PageId, data: Array[Byte]): Unit
  def free(id: PageId): Unit
  def setMetaRoot(id: PageId): Unit
