import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.Comparator

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = "cleanup.package"
final int ERROR_DETAILS_MAX = 2048

def capDetails = { s ->
  if (s == null) return null
  String t = s.toString()
  (t.length() > ERROR_DETAILS_MAX) ? t.substring(0, ERROR_DETAILS_MAX) : t
}

def setFailure = { flowFile, String message, String details = null ->
  def out = session.putAttribute(flowFile, "error.stage", ERROR_STAGE)
  out = session.putAttribute(out, "error.message", message ?: "Package cleanup failed")
  if (details != null && details.toString().trim()) {
    out = session.putAttribute(out, "error.details", capDetails(details))
  }
  return out
}

def getAttr = { String key ->
  def v = ff.getAttribute(key)
  (v != null && v.toString().trim()) ? v.toString().trim() : null
}

def deleteTree = { Path root ->
  Files.walk(root).withCloseable { stream ->
    stream
      .sorted(Comparator.reverseOrder())
      .forEach { Path p -> Files.deleteIfExists(p) }
  }
}

def packageName = getAttr("package.name")
def payloadsBase = getAttr("payloads.base.dir")
def transferBase = getAttr("transfer.base.dir")
def workBase     = getAttr("work.base.dir")

def missing = []
[
  ["package.name", packageName],
  ["payloads.base.dir", payloadsBase],
  ["transfer.base.dir", transferBase],
  ["work.base.dir", workBase]
].each { kv ->
  if (!kv[1]) missing << kv[0]
}

if (!missing.isEmpty()) {
  def msg = "Missing required attributes: ${missing.join(', ')}"
  ff = session.putAttribute(ff, "cleanup.package.status", "FAIL")
  ff = setFailure(ff, msg)
  session.transfer(ff, REL_FAILURE)
  return
}

def targets = [
  [label: "payloads", base: payloadsBase],
  [label: "transfer", base: transferBase],
  [label: "work",     base: workBase]
]

try {
  boolean anyDeleted = false

  for (t in targets) {
    String label = t.label
    String baseStr = t.base

    Path basePath = Paths.get(baseStr).toAbsolutePath().normalize()
    Path target   = basePath.resolve(packageName).toAbsolutePath().normalize()

    if (!target.startsWith(basePath)) {
      throw new RuntimeException("Refusing cleanup: ${label} target escapes base dir. target=${target} base=${basePath}")
    }

    String existsKey  = "cleanup.package.exists.${label}"
    String deletedKey = "cleanup.package.deleted.${label}"
    String pathKey    = "cleanup.package.path.${label}"

    ff = session.putAttribute(ff, pathKey, target.toString())

    if (!Files.exists(target)) {
      ff = session.putAttribute(ff, existsKey, "false")
      ff = session.putAttribute(ff, deletedKey, "false")
      continue
    }

    ff = session.putAttribute(ff, existsKey, "true")

    if (!Files.isDirectory(target)) {
      throw new RuntimeException("Refusing cleanup: expected directory for ${label}, found non-directory: ${target}")
    }

    deleteTree(target)
    anyDeleted = true
    ff = session.putAttribute(ff, deletedKey, "true")
  }

  ff = session.putAttribute(ff, "cleanup.package.deleted.any", anyDeleted.toString())
  ff = session.putAttribute(ff, "cleanup.package.status", "OK")
  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  ff = session.putAttribute(ff, "cleanup.package.status", "FAIL")
  ff = setFailure(ff, e.message ?: "Package cleanup failed", e.toString())
  session.transfer(ff, REL_FAILURE)
}
