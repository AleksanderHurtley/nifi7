import java.nio.file.*

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = "fetch.metadata.cleanup"
final int ERROR_DETAILS_MAX = 2048

def capDetails = { s ->
  if (s == null) return null
  String t = s.toString()
  (t.length() > ERROR_DETAILS_MAX) ? t.substring(0, ERROR_DETAILS_MAX) : t
}

def setFailure = { flowFile, String message, String details = null ->
  def out = session.putAttribute(flowFile, "error.stage", ERROR_STAGE)
  out = session.putAttribute(out, "error.message", message ?: "Metadata cleanup failed")
  if (details != null && details.toString().trim()) {
    out = session.putAttribute(out, "error.details", capDetails(details))
  }
  return out
}

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
    def msg = "Not a directory: ${extractDirStr}"
    ff = session.putAttribute(ff, "metadata.extract.deleted", "FAIL")
    ff = session.putAttribute(ff, "metadata.extract.delete.error", msg)
    ff = setFailure(ff, msg)
    session.transfer(ff, REL_FAILURE)
    return
  }

  // Safety guard: only delete if the path contains "/fc1/work/" (prevents accidents)
  def p = dir.toAbsolutePath().normalize().toString()
  if (!p.contains("/fc1/work/")) {
    def msg = "Refusing to delete path outside /fc1/work: ${p}"
    ff = session.putAttribute(ff, "metadata.extract.deleted", "FAIL")
    ff = session.putAttribute(ff, "metadata.extract.delete.error", msg)
    ff = setFailure(ff, msg)
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
  ff = setFailure(ff, e.message ?: "Metadata cleanup failed", e.toString())
  session.transfer(ff, REL_FAILURE)
}
