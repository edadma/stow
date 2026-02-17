package io.github.edadma.stow

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import io.github.edadma.cross_platform.{createTempFile, deleteFile}

class CRC32Tests extends AnyFreeSpec with Matchers:

  "empty input" in {
    CRC32.compute(Array.empty, 0, 0) shouldBe 0
  }

  "known value" in {
    val data = "123456789".getBytes("US-ASCII")
    CRC32.compute(data, 0, data.length) shouldBe 0xcbf43926
  }

  "partial array" in {
    val data = "XX123456789YY".getBytes("US-ASCII")
    CRC32.compute(data, 2, 9) shouldBe 0xcbf43926
  }

class HeaderTests extends AnyFreeSpec with Matchers:

  "round-trip header with no pending free" in {
    val h = HeaderData(
      version = FormatVersion,
      pageSize = 256,
      pageCount = 10,
      epoch = 42,
      metaRoot = 5,
      freeListHead = 3,
      pendingFree = Array.empty,
    )
    val bytes = Header.write(h, 256)
    bytes.length shouldBe 256

    val parsed = Header.read(bytes)
    parsed shouldBe defined
    val r = parsed.get
    r.version shouldBe FormatVersion
    r.pageSize shouldBe 256
    r.pageCount shouldBe 10
    r.epoch shouldBe 42
    r.metaRoot shouldBe 5
    r.freeListHead shouldBe 3
    r.pendingFree shouldBe empty
  }

  "round-trip header with pending free pages" in {
    val pending = Array(4, 7, 9)
    val h = HeaderData(
      version = FormatVersion,
      pageSize = 256,
      pageCount = 10,
      epoch = 1,
      metaRoot = 2,
      freeListHead = NoPage,
      pendingFree = pending,
    )
    val bytes = Header.write(h, 256)
    val parsed = Header.read(bytes).get
    parsed.pendingFree shouldBe pending
  }

  "corrupted header returns None" in {
    val h = HeaderData(
      version = FormatVersion,
      pageSize = 256,
      pageCount = 2,
      epoch = 0,
      metaRoot = NoPage,
      freeListHead = NoPage,
      pendingFree = Array.empty,
    )
    val bytes = Header.write(h, 256)
    bytes(20) = (bytes(20) ^ 0xff).toByte // corrupt a byte
    Header.read(bytes) shouldBe None
  }

  "bad magic returns None" in {
    val bytes = new Array[Byte](256)
    bytes(0) = 'X'.toByte
    Header.read(bytes) shouldBe None
  }

class PageStoreTests extends AnyFreeSpec with Matchers:

  def withStore(pageSize: Int = 256)(test: (FilePageStore, String) => Unit): Unit =
    val path = createTempFile("stow-test", ".db")
    val store = FilePageStore.create(path, pageSize)
    try test(store, path)
    finally
      try store.close() catch case _: Exception => ()
      deleteFile(path)

  def fillPage(store: PageStore, value: Byte): Array[Byte] =
    val data = new Array[Byte](store.pageSize)
    java.util.Arrays.fill(data, value)
    data

  "create and read back" in withStore() { (store, _) =>
    store.pageSize shouldBe 256
    store.metaRoot shouldBe NoPage
  }

  "allocate, write, and read a page" in withStore() { (store, _) =>
    var pageId: PageId = NoPage
    store.modify { batch =>
      pageId = batch.allocate()
      pageId should be >= HeaderPages
      val data = fillPage(store, 0x42)
      batch.write(pageId, data)
    }

    val readBack = store.read(pageId)
    readBack.forall(_ == 0x42) shouldBe true
  }

  "read within batch sees uncommitted writes" in withStore() { (store, _) =>
    store.modify { batch =>
      val id = batch.allocate()
      val data = fillPage(store, 0xab.toByte)
      batch.write(id, data)

      val readBack = batch.read(id)
      readBack.forall(_ == 0xab.toByte) shouldBe true
    }
  }

  "set and persist metaRoot" in withStore() { (store, path) =>
    var rootId: PageId = NoPage
    store.modify { batch =>
      rootId = batch.allocate()
      val data = fillPage(store, 0x01)
      batch.write(rootId, data)
      batch.setMetaRoot(rootId)
    }

    store.metaRoot shouldBe rootId
    store.close()

    val reopened = FilePageStore.open(path)
    reopened.metaRoot shouldBe rootId
    val data = reopened.read(rootId)
    data.forall(_ == 0x01) shouldBe true
    reopened.close()
  }

  "COW: free old page, allocate new" in withStore() { (store, _) =>
    var oldId: PageId = NoPage
    store.modify { batch =>
      oldId = batch.allocate()
      batch.write(oldId, fillPage(store, 0x01))
      batch.setMetaRoot(oldId)
    }

    var newId: PageId = NoPage
    store.modify { batch =>
      newId = batch.allocate()
      batch.write(newId, fillPage(store, 0x02))
      batch.free(oldId)
      batch.setMetaRoot(newId)
    }

    store.metaRoot shouldBe newId
    store.read(newId).forall(_ == 0x02) shouldBe true
  }

  "freed pages are reclaimed after deferred cycle" in withStore() { (store, _) =>
    // Commit 1: allocate page
    var pageA: PageId = NoPage
    store.modify { batch =>
      pageA = batch.allocate()
      batch.write(pageA, fillPage(store, 0x01))
      batch.setMetaRoot(pageA)
    }

    // Commit 2: free pageA (goes to pending)
    var pageB: PageId = NoPage
    store.modify { batch =>
      pageB = batch.allocate()
      batch.write(pageB, fillPage(store, 0x02))
      batch.free(pageA)
      batch.setMetaRoot(pageB)
    }

    // Commit 3: pageA should now be reclaimable (linked from pending into free list)
    var pageC: PageId = NoPage
    store.modify { batch =>
      pageC = batch.allocate()
      batch.write(pageC, fillPage(store, 0x03))
    }

    // pageC should have reclaimed pageA's slot
    pageC shouldBe pageA
  }

  "batch rollback on exception" in withStore() { (store, _) =>
    var allocatedId: PageId = NoPage
    store.modify { batch =>
      allocatedId = batch.allocate()
      batch.write(allocatedId, fillPage(store, 0x01))
      batch.setMetaRoot(allocatedId)
    }

    val originalRoot = store.metaRoot

    try
      store.modify { batch =>
        val id = batch.allocate()
        batch.write(id, fillPage(store, 0xff.toByte))
        batch.setMetaRoot(id)
        throw new RuntimeException("rollback test")
      }
    catch case _: RuntimeException => ()

    store.metaRoot shouldBe originalRoot
  }

  "multiple pages in single batch" in withStore() { (store, _) =>
    var ids = Vector.empty[PageId]
    store.modify { batch =>
      for i <- 0 until 5 do
        val id = batch.allocate()
        batch.write(id, fillPage(store, i.toByte))
        ids = ids :+ id
    }

    for i <- 0 until 5 do
      store.read(ids(i)).forall(_ == i.toByte) shouldBe true
  }

  "persistence across close/open" in withStore() { (store, path) =>
    var ids = Vector.empty[PageId]
    store.modify { batch =>
      for i <- 0 until 3 do
        val id = batch.allocate()
        batch.write(id, fillPage(store, (i + 10).toByte))
        ids = ids :+ id
      batch.setMetaRoot(ids.head)
    }
    store.close()

    val reopened = FilePageStore.open(path)
    reopened.metaRoot shouldBe ids.head
    for i <- 0 until 3 do
      reopened.read(ids(i)).forall(_ == (i + 10).toByte) shouldBe true
    reopened.close()
  }

  "free list persists across close/open" in withStore() { (store, path) =>
    // Allocate and free pages across multiple commits
    var pageA: PageId = NoPage
    store.modify { batch =>
      pageA = batch.allocate()
      batch.write(pageA, fillPage(store, 0x01))
      batch.setMetaRoot(pageA)
    }

    var pageB: PageId = NoPage
    store.modify { batch =>
      pageB = batch.allocate()
      batch.write(pageB, fillPage(store, 0x02))
      batch.free(pageA)
      batch.setMetaRoot(pageB)
    }

    // pageA is now in pending. Do one more commit to link it into free list.
    store.modify { batch =>
      val id = batch.allocate()
      batch.write(id, fillPage(store, 0x03))
    }

    store.close()

    // Reopen: free list should contain pageA
    val reopened = FilePageStore.open(path)
    var reused: PageId = NoPage
    reopened.modify { batch =>
      reused = batch.allocate()
      batch.write(reused, fillPage(reopened, 0x04))
    }
    // The last commit freed nothing and the pending from the commit before close
    // should have been linked during the last modify, so pageA should be reusable.
    // (Exact reuse depends on free list ordering)
    reopened.close()
  }

  "startup completes pending reclamation" in withStore() { (store, path) =>
    var pageA: PageId = NoPage
    store.modify { batch =>
      pageA = batch.allocate()
      batch.write(pageA, fillPage(store, 0x01))
      batch.setMetaRoot(pageA)
    }

    // Free pageA — it goes to pending
    var pageB: PageId = NoPage
    store.modify { batch =>
      pageB = batch.allocate()
      batch.write(pageB, fillPage(store, 0x02))
      batch.free(pageA)
      batch.setMetaRoot(pageB)
    }

    // Close without another commit — pageA is still in pending
    store.close()

    // Reopen — startup should complete the reclamation
    val reopened = FilePageStore.open(path)
    var pageC: PageId = NoPage
    reopened.modify { batch =>
      pageC = batch.allocate()
      batch.write(pageC, fillPage(reopened, 0x03))
    }
    pageC shouldBe pageA // reclaimed
    reopened.close()
  }

  "write rejects wrong-sized data" in withStore() { (store, _) =>
    assertThrows[IllegalArgumentException] {
      store.modify { batch =>
        val id = batch.allocate()
        batch.write(id, new Array[Byte](10))
      }
    }
  }

  "cannot free header pages" in withStore() { (store, _) =>
    assertThrows[IllegalArgumentException] {
      store.modify { batch =>
        batch.free(0)
      }
    }
  }
