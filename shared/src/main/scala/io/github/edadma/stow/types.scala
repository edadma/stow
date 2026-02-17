package io.github.edadma.stow

type PageId = Int

val NoPage: PageId = 0

val Magic: Array[Byte] = Array('P'.toByte, 'G'.toByte, 'S'.toByte, 'T'.toByte)
val FormatVersion: Int = 1

val HeaderPages: Int = 2
