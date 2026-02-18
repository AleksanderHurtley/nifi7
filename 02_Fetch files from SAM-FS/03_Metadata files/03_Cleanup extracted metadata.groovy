import java.nio.file.*

def ff = session.get()
if (!ff) return

def extractDirStr = ff.getAttribute('metadata.extract.dir')
if (!extractDirStr) {
  ff = session.putAttribute(ff, "metadata.extract.deleted", "SKIPPED")
  ff = session.putAttribute(ff, "metadata.extract.delete.reason", "metadata.extract.dir attribute missing")
  session.transfer(ff, REL_SUCCESS)
  return
}

Path dir = Paths.get(extractDirStr)

try {
  if (!Files.exists(dir)) {
    ff = session.putAttribute(ff, "metadata.extract.deleted", "SKIPPED")
    ff = session.putAttribute(ff, "metadata.extract.delete.reason", "extract dir does not exist")
    session.transfer(ff, REL_SUCCESS)
    return
  }

  if (!Files.isDirectory(dir)) {
    ff = session.putAttribute(ff, "metadata.extract.deleted", "FAIL")
    ff = session.putAttribute(ff, "metadata.extract.delete.error", "Not a directory: ${extractDirStr}")
    session.transfer(ff, REL_FAILURE)
    return
  }

  // Safety guard: only delete if the path contains "/fc1/work/" (prevents accidents)
  def p = dir.toAbsolutePath().normalize().toString()
  if (!p.contains("/fc1/work/")) {
    ff = session.putAttribute(ff, "metadata.extract.deleted", "FAIL")
    ff = session.putAttribute(ff, "metadata.extract.delete.error", "Refusing to delete path outside /fc1/work: ${p}")
    session.transfer(ff, REL_FAILURE)
    return
  }

  // Recursive delete (files first, then dirs)
  Files.walk(dir)
    .sorted(Comparator.reverseOrder())
    .forEach { Path x ->
      try { Files.deleteIfExists(x) } catch (Exception ignore) {}
    }

  ff = session.putAttribute(ff, "metadata.extract.deleted", "true")
  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  ff = session.putAttribute(ff, "metadata.extract.deleted", "false")
  ff = session.putAttribute(ff, "metadata.extract.delete.error", e.toString())
  session.transfer(ff, REL_FAILURE)
}
