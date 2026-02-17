# stow

![Maven Central](https://img.shields.io/maven-central/v/io.github.edadma/stow_sjs1_3)
[![Last Commit](https://img.shields.io/github/last-commit/edadma/stow)](https://github.com/edadma/stow/commits)
![GitHub](https://img.shields.io/github/license/edadma/stow)
![Scala Version](https://img.shields.io/badge/Scala-3.8.1-blue.svg)
![ScalaJS Version](https://img.shields.io/badge/Scala.js-1.20.2-blue.svg)
![Scala Native Version](https://img.shields.io/badge/Scala_Native-0.5.10-blue.svg)

Crash-safe atomic durable page storage for Scala 3. Manages fixed-size pages in a single file with atomic commits, crash recovery, and no write-ahead log.

## Overview

Stow provides a low-level foundation for building persistent data structures — B+ tree indexes, table storage, blob chains — without embedding any knowledge of what the pages contain. It uses copy-on-write pages and a double-buffered header to achieve atomicity and durability.

- **Copy-on-write pages** — never modify a live page; allocate, write, free
- **Atomic commits** — all writes in a batch succeed or none do
- **Crash recovery** — double-buffered headers guarantee a valid state on disk at all times
- **Persistent free list** — freed pages are tracked on disk with deferred reclamation
- **Cross-platform** — JVM, Scala.js, and Scala Native

## Installation

Add to your `build.sbt`:

```scala
libraryDependencies += "io.github.edadma" %%% "stow" % "0.0.1"
```

## Usage

### Create a page store

```scala
import io.github.edadma.stow.*

val store = FilePageStore.create("data.db", pageSize = 4096)
```

### Write pages atomically

```scala
store.modify { batch =>
  val page = batch.allocate()
  val data = new Array[Byte](store.pageSize)
  // ... fill data ...
  batch.write(page, data)
  batch.setMetaRoot(page)
}
```

### Read pages

```scala
val data = store.read(store.metaRoot)
```

### Copy-on-write updates

```scala
store.modify { batch =>
  val oldRoot = store.metaRoot
  val oldData = batch.read(oldRoot)

  // Modify in a new page
  val newRoot = batch.allocate()
  val newData = transform(oldData)
  batch.write(newRoot, newData)
  batch.free(oldRoot)
  batch.setMetaRoot(newRoot)
}
```

### Reopen after crash or restart

```scala
val store = FilePageStore.open("data.db")
// Automatically recovers to the last committed state
```

## API

### PageStore

```scala
trait PageStore:
  def pageSize: Int
  def read(id: PageId): Array[Byte]
  def modify(fn: WriteBatch => Unit): Unit
  def metaRoot: PageId
  def close(): Unit
```

### WriteBatch

```scala
trait WriteBatch:
  def allocate(): PageId
  def read(id: PageId): Array[Byte]
  def write(id: PageId, data: Array[Byte]): Unit
  def free(id: PageId): Unit
  def setMetaRoot(id: PageId): Unit
```

## How It Works

The file is an array of fixed-size pages. Pages 0 and 1 are headers; all others are data pages.

**Double-buffered headers** alternate between page 0 and page 1. Each header includes a monotonic epoch and a CRC32 checksum. On startup, the header with the higher valid epoch wins. If a crash tears a header write, the other slot is still valid.

**Deferred free list reclamation** solves the problem of freeing pages under copy-on-write. Pages freed in commit N are recorded in the header's pending list. At the start of commit N+1, they're linked into the on-disk free list. This ensures freed pages are never written to while still referenced by the current header.

## Design Tradeoffs

- Full page writes even for small changes
- Freed pages unavailable for one commit cycle
- No write batching or background flushing
- No page caching (relies on OS page cache)

These trade simplicity and correctness for raw performance, which is the right trade for the database sizes this library targets.

## License

ISC License — see [LICENSE](LICENSE) for details.
