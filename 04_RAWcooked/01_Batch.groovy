import groovy.json.JsonSlurper
import java.nio.file.*
import static java.nio.file.StandardCopyOption.*

def ff = session.get()
if (!ff) return

def getAttr = { String k ->
  def v = ff.getAttribute(k)
  (v && v.trim()) ? v.trim() : null
}

try {
  def pkg           = getAttr("package.name")
  def dpxDirStr     = getAttr("dpx.unpack.dir")
  def dpxPreDirStr  = getAttr("dpx.unpack.dir.pre")   // optional
  def batchesDirStr = getAttr("batches.dir")
  def metaDirStr    = getAttr("metadata.preservation.dpx.dir")
  def batchPreId    = getAttr("batch.pre.id")

  def missing = []
  [
    ["package.name", pkg],
    ["dpx.unpack.dir", dpxDirStr],
    ["batches.dir", batchesDirStr],
    ["metadata.preservation.dpx.dir", metaDirStr],
    ["batch.pre.id", batchPreId]
  ].each { if (!it[1]) missing << it[0] }

  if (!missing.isEmpty()) {
    ff = session.putAttribute(ff, "batch.status", "FAIL")
    ff = session.putAttribute(ff, "batch.error", "Missing attributes: ${missing.join(', ')}")
    session.transfer(ff, REL_FAILURE)
    return
  }

  Path dpxDir     = Paths.get(dpxDirStr)
  Path batchesDir = Paths.get(batchesDirStr)
  Path metaDir    = Paths.get(metaDirStr)

  if (!Files.isDirectory(metaDir)) throw new RuntimeException("metadata.preservation.dpx.dir is missing or not a directory: ${metaDir}")
  if (!Files.isDirectory(dpxDir))  throw new RuntimeException("dpx.unpack.dir is missing or not a directory: ${dpxDir}")

  Path dpxPreDir = null
  boolean hasPreDir = false
  if (dpxPreDirStr) {
    dpxPreDir = Paths.get(dpxPreDirStr)
    hasPreDir = Files.isDirectory(dpxPreDir)
  }

  Files.createDirectories(batchesDir)

  // Read batches.json
  Path batchesJsonPath = metaDir.resolve("batches.json")
  if (!Files.isRegularFile(batchesJsonPath)) {
    throw new RuntimeException("batches.json not found: ${batchesJsonPath}")
  }

  def parsed = new JsonSlurper().parse(batchesJsonPath.toFile())
  def batchesMap = parsed?.batches
  if (!(batchesMap instanceof Map) || batchesMap.isEmpty()) {
    throw new RuntimeException("Invalid batches.json: missing or empty 'batches' object")
  }

  // If PRE exists in mapping, require pre dir
  boolean needsPre = batchesMap.containsKey(batchPreId)
  if (needsPre && !hasPreDir) {
    throw new RuntimeException("PRE batch present in batches.json but dpx.unpack.dir.pre is missing or not a directory: ${dpxPreDirStr ?: 'null'}")
  }

  // Process batches in stable order: PRE first, then batch0001..
  def batchKey = { String batchId ->
    if (batchId == batchPreId) return -1
    def m = (batchId =~ /batch(\d{4})/)
    return m.find() ? m.group(1).toInteger() : 999999
  }

  def batchIds = batchesMap.keySet().collect { it.toString() }.sort { a, b ->
    batchKey(a) <=> batchKey(b) ?: (a <=> b)
  }

  int batchCounter = 0
  def batchNames = new LinkedHashSet<String>()

  batchIds.each { String batchId ->
    def files = batchesMap[batchId]
    if (!(files instanceof List) || files.isEmpty()) {
      throw new RuntimeException("Batch '${batchId}' has no files in batches.json")
    }

    Path batchDir = batchesDir.resolve(batchId)
    Files.createDirectories(batchDir)

    Path srcBase = (batchId == batchPreId) ? dpxPreDir : dpxDir

    // File order is kept exactly as in batches.json
    files.each { Object o ->
      def fname = (o?.toString() ?: "").trim()
      if (!fname) return

      Path src = srcBase.resolve(fname)
      Path dst = batchDir.resolve(fname)

      if (!Files.exists(src)) {
        if (Files.exists(dst)) {
          log.warn("Source missing but destination already exists (assuming previously moved): src=${src} dst=${dst}")
          return
        }
        throw new RuntimeException("Missing DPX file listed in ${batchId}: ${src}")
      }

      Files.move(src, dst, REPLACE_EXISTING)
    }

    batchCounter++
    batchNames.add(batchId)
  }

  ff = session.putAttribute(ff, "batch.status", "OK")
  ff = session.putAttribute(ff, "batches.count", batchCounter.toString())
  ff = session.putAttribute(ff, "batches.names", batchNames.join(","))

  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  log.error("Batch folder creation error: ${e.message}", e)
  ff = session.putAttribute(ff, "batch.status", "FAIL")
  ff = session.putAttribute(ff, "batch.error", (e.message ?: e.toString()))
  session.transfer(ff, REL_FAILURE)
}
