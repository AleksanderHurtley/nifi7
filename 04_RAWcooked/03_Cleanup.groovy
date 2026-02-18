import java.nio.file.*

def ff = session.get()
if (!ff) return

def getAttr = { String k ->
  def v = ff.getAttribute(k)
  (v && v.trim()) ? v.trim() : null
}

def pkg        = getAttr("package.name")
def workDirStr = getAttr("work.dir")
def batchesStr = getAttr("batches.dir")
def dpxStr     = getAttr("dpx.unpack.dir")
def dpxPreStr  = getAttr("dpx.unpack.dir.pre")   // optional

def missing = []
[['package.name',pkg],
 ['work.dir',workDirStr],
 ['batches.dir',batchesStr],
 ['dpx.unpack.dir',dpxStr]].each { kv ->
  if (!kv[1]) missing << kv[0]
}

if (!missing.isEmpty()) {
  ff = session.putAttribute(ff, "cleanup.status", "FAIL")
  ff = session.putAttribute(ff, "cleanup.error", "Missing attributes: ${missing.join(',')}")
  session.transfer(ff, REL_FAILURE)
  return
}

Path workDir   = Paths.get(workDirStr).toAbsolutePath().normalize()
Path batches   = Paths.get(batchesStr).toAbsolutePath().normalize()
Path dpxDir    = Paths.get(dpxStr).toAbsolutePath().normalize()
Path dpxPreDir = dpxPreStr ? Paths.get(dpxPreStr).toAbsolutePath().normalize() : null

def ensureUnderWorkDir = { Path p, String label ->
  if (p == null) return
  if (!p.startsWith(workDir)) {
    throw new RuntimeException(
      "Refusing cleanup: ${label} is outside work.dir. ${label}=${p} work.dir=${workDir}"
    )
  }
}

def deleteTree = { Path root ->
  if (!root || !Files.exists(root)) return false
  Files.walk(root)
    .sorted(Comparator.reverseOrder())
    .forEach { p ->
      try { Files.deleteIfExists(p) } catch (Exception ignore) {}
    }
  return true
}

def deleteLogs = { Path root ->
  if (!Files.isDirectory(root)) return 0
  int deleted = 0
  Files.newDirectoryStream(root, "rawcooked_*.log").withCloseable { ds ->
    ds.each { Path p ->
      try { Files.deleteIfExists(p); deleted++ } catch (Exception ignore) {}
    }
  }
  return deleted
}

try {
  // Guardrails: delete only inside work.dir
  ensureUnderWorkDir(batches, "batches.dir")
  ensureUnderWorkDir(dpxDir, "dpx.unpack.dir")
  if (dpxPreDir != null) ensureUnderWorkDir(dpxPreDir, "dpx.unpack.dir.pre")

  boolean deletedBatches = deleteTree(batches)
  boolean deletedDpx     = deleteTree(dpxDir)
  boolean deletedDpxPre  = (dpxPreDir != null) ? deleteTree(dpxPreDir) : false

  int deletedLogCount = deleteLogs(workDir)

  ff = session.putAttribute(ff, "cleanup.status", "OK")
  ff = session.putAttribute(ff, "cleanup.deleted.batches", deletedBatches.toString())
  ff = session.putAttribute(ff, "cleanup.deleted.dpx", deletedDpx.toString())
  ff = session.putAttribute(ff, "cleanup.deleted.dpx_pre", deletedDpxPre.toString())
  ff = session.putAttribute(ff, "cleanup.deleted.rawcooked.logs.count", deletedLogCount.toString())

  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  ff = session.putAttribute(ff, "cleanup.status", "FAIL")
  ff = session.putAttribute(ff, "cleanup.error", e.toString())
  session.transfer(ff, REL_FAILURE)
}
