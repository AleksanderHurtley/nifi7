import java.nio.file.*
import static java.nio.file.StandardOpenOption.*

def ff = session.get()
if (!ff) return

def getAttr = { String k ->
  def v = ff.getAttribute(k)
  (v && v.trim()) ? v.trim() : null
}

// ------------------------------------------------------------
// ATTRIBUTES
// ------------------------------------------------------------
def pkg                      = getAttr("package.name")
def payloadDirStr            = getAttr("payloads.dir")
def eventsPayloadPathStr     = getAttr("events.payload.path")
def submissionPayloadPathStr = getAttr("submission.payload.path")

def workDir           = getAttr("work.dir")
def dpxUnpackDir       = getAttr("dpx.unpack.dir")
def repDir             = getAttr("rep.dir")
def repDataDir         = getAttr("rep.data.dir")
def metadataOtherDir   = getAttr("metadata.other.dir")
def metadataDescDir    = getAttr("metadata.descriptive.dir")

// ------------------------------------------------------------
// VALIDATE REQUIRED ATTRIBUTES
// ------------------------------------------------------------
def missing = []
[
  ["package.name", pkg],
  ["payloads.dir", payloadDirStr],
  ["events.payload.path", eventsPayloadPathStr],
  ["submission.payload.path", submissionPayloadPathStr],
  ["work.dir", workDir],
  ["dpx.unpack.dir", dpxUnpackDir],
  ["rep.dir", repDir],
  ["rep.data.dir", repDataDir],
  ["metadata.other.dir", metadataOtherDir],
  ["metadata.descriptive.dir", metadataDescDir],
].each { if (!it[1]) missing << it[0] }

if (!missing.isEmpty()) {
  ff = session.putAttribute(ff, "setup.status", "FAIL")
  ff = session.putAttribute(ff, "setup.error", "Missing attributes: ${missing.join(', ')}")
  session.transfer(ff, REL_FAILURE)
  return
}

try {
  // ------------------------------------------------------------
  // CREATE DIRECTORIES
  // ------------------------------------------------------------
  [
    workDir,
    dpxUnpackDir,
    repDir,
    repDataDir,
    metadataOtherDir,
    metadataDescDir,
    payloadDirStr
  ].each { p -> Files.createDirectories(Paths.get(p)) }

  // ------------------------------------------------------------
  // RESET PAYLOAD FILES
  // ------------------------------------------------------------
  def resetFileAtomic = { String pathStr ->
    Path p = Paths.get(pathStr)
    Files.createDirectories(p.getParent())
    Path tmp = p.resolveSibling(p.getFileName().toString() + ".tmp")
    Files.write(tmp, new byte[0], CREATE, TRUNCATE_EXISTING, WRITE)
    Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  }

  resetFileAtomic(eventsPayloadPathStr)
  resetFileAtomic(submissionPayloadPathStr)

  ff = session.putAttribute(ff, "setup.status", "OK")
  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  ff = session.putAttribute(ff, "setup.status", "FAIL")
  ff = session.putAttribute(ff, "setup.error", e.toString())
  session.transfer(ff, REL_FAILURE)
}
