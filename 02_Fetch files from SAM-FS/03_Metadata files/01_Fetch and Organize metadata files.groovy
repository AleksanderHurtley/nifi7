import java.nio.file.*
import java.nio.charset.StandardCharsets

def ff = session.get()
if (!ff) return

def pkg                = ff.getAttribute('package.name')
def sourceDirStr       = ff.getAttribute('source.dir')
def repDirStr          = ff.getAttribute('rep.dir')
def extractDirStr      = ff.getAttribute('metadata.extract.dir')
def workDirStr         = ff.getAttribute('work.dir')

def descriptiveDirStr  = ff.getAttribute('metadata.descriptive.dir')
def deprMetsDirStr     = ff.getAttribute('metadata.other.depr_mets.dir')
def preservationDirStr = ff.getAttribute('metadata.preservation.dir')
def unclassifiedBaseStr= ff.getAttribute('metadata.other.unclassified.dir')

def missing = []
[
  ['package.name', pkg],
  ['source.dir', sourceDirStr],
  ['rep.dir', repDirStr],
  ['metadata.extract.dir', extractDirStr],
  ['work.dir', workDirStr],
  ['metadata.descriptive.dir', descriptiveDirStr],
  ['metadata.other.depr_mets.dir', deprMetsDirStr],
  ['metadata.preservation.dir', preservationDirStr],
  ['metadata.other.unclassified.dir', unclassifiedBaseStr]
].each { kv ->
  if (!(kv[1]?.trim())) missing << kv[0]
}

if (!missing.isEmpty()) {
  ff = session.putAttribute(ff, 'metadata.org.status', 'FAIL')
  ff = session.putAttribute(ff, 'metadata.org.error', "Missing attributes: ${missing.join(',')}")
  session.transfer(ff, REL_FAILURE)
  return
}

Path sourceDir = Paths.get(sourceDirStr)
if (!Files.isDirectory(sourceDir)) {
  ff = session.putAttribute(ff, 'metadata.org.status', 'FAIL')
  ff = session.putAttribute(ff, 'metadata.org.error', "Source dir not found: ${sourceDirStr}")
  session.transfer(ff, REL_FAILURE)
  return
}

// Paths
Path repDir         = Paths.get(repDirStr)
Path extractDir     = Paths.get(extractDirStr)
Path workDir        = Paths.get(workDirStr)
Path descriptiveDir = Paths.get(descriptiveDirStr)
Path deprMetsDir    = Paths.get(deprMetsDirStr)
Path preservationDir= Paths.get(preservationDirStr)

Path unclassifiedBase         = Paths.get(unclassifiedBaseStr)
Path unclassifiedSourceDir    = unclassifiedBase.resolve("source")
Path unclassifiedExtractedDir = unclassifiedBase.resolve("extracted")

try {
  // ----------------------------------------------------------------
  // Create expected directories
  // ----------------------------------------------------------------
  [descriptiveDir, deprMetsDir, preservationDir, extractDir].each { Files.createDirectories(it) }

  // ----------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------
  def safeCopy = { Path src, Path dst ->
    Files.createDirectories(dst.getParent())
    Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)
  }

  // Lazy copy: only creates unclassified dirs when needed
  boolean wroteUnclassified = false
  def safeCopyLazy = { Path src, Path dst ->
    Files.createDirectories(dst.getParent())
    Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)
    wroteUnclassified = true
  }

  def isJhove = { String name -> name.startsWith("JHOVE_") || name.contains("JHOVE_") }

  def isIgnorableMarkerFile = { String name ->
    def n = (name ?: "").toLowerCase()
    if (n.endsWith(".done")) return true
    if (n == ".ds_store") return true
    if (n == "thumbs.db") return true
    if (n == "desktop.ini") return true
    return false
  }

  def deleteJhoveUnder = { Path root ->
    if (!Files.isDirectory(root)) return
    Files.walk(root).forEach { Path p ->
      if (!Files.isRegularFile(p)) return
      def n = p.fileName.toString()
      if (isJhove(n)) {
        try { Files.deleteIfExists(p) } catch (Exception ignore) {}
      }
    }
  }

  // ----------------------------------------------------------------
  // Check if a file is "known/handled"
  // ----------------------------------------------------------------
  def isHandledSourceFile = { String name ->
    if (isIgnorableMarkerFile(name)) return true
    if (isJhove(name)) return true
    if (name == "${pkg}.xml") return true
    if (name.toLowerCase().endsWith("_meta_xml.tar")) return true
    if (name.startsWith("MAVIS_") && name.toLowerCase().endsWith(".xml")) return true
    return false
  }

  def isHandledExtractedFile = { String name ->
    if (isIgnorableMarkerFile(name)) return true
    if (isJhove(name)) return true
    if (name == ".extract_complete") return true
    if (name.startsWith("META_") && name.endsWith(".tar.xml")) return true // intentionally kept only in extractDir
    if (name.startsWith("METS_") && name.toLowerCase().endsWith(".xml")) return true
    if (name.toLowerCase().contains("scanitytransfer") && name.toLowerCase().endsWith(".xml")) return true
    return false
  }

  // ----------------------------------------------------------------
  // 1) Extract meta/*_meta_xml.tar into extractDir if not already extracted
  // ----------------------------------------------------------------
  Path marker = extractDir.resolve(".extract_complete")
  def alreadyExtracted = Files.isRegularFile(marker)

  if (!alreadyExtracted) {
    if (Files.exists(extractDir)) {
      Files.walk(extractDir)
        .sorted(Comparator.reverseOrder())
        .forEach { Path p -> try { Files.deleteIfExists(p) } catch (Exception ignore) {} }
    }
    Files.createDirectories(extractDir)

    Path metaDir = sourceDir.resolve("meta")
    if (!Files.isDirectory(metaDir)) {
      throw new RuntimeException("meta dir not found: ${metaDir}")
    }

    def candidates = []
    Files.newDirectoryStream(metaDir, "*_meta_xml.tar").each { Path p ->
      if (Files.isRegularFile(p)) candidates << p
    }

    if (candidates.isEmpty()) {
      throw new RuntimeException("No *_meta_xml.tar found in: ${metaDir}")
    }
    if (candidates.size() != 1) {
      def names = candidates.collect { it.fileName.toString() }.sort().join(", ")
      throw new RuntimeException("Expected exactly 1 *_meta_xml.tar in ${metaDir}, found ${candidates.size()}: ${names}")
    }

    Path metaTar = candidates[0]

    def cmd = ['tar', '-xf', metaTar.toString(), '-C', extractDir.toString()]
    def pb = new ProcessBuilder(cmd)
    pb.redirectErrorStream(true)
    def proc = pb.start()
    def out = proc.inputStream.getText('UTF-8')
    def rc = proc.waitFor()

    if (rc != 0) {
      throw new RuntimeException("tar extract failed rc=${rc}. Output: " + out.take(2000))
    }

    Files.write(marker, "ok\n".getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  // ----------------------------------------------------------------
  // 2) Delete JHOVE under extractDir
  // ----------------------------------------------------------------
  deleteJhoveUnder(extractDir)

  // ----------------------------------------------------------------
  // 3) Copy deprecated mets digifilm_*.xml -> SIP metadata/other/deprecated_mets
  // ----------------------------------------------------------------
  Path topXml = sourceDir.resolve("${pkg}.xml")
  if (Files.isRegularFile(topXml)) {
    safeCopy(topXml, deprMetsDir.resolve(topXml.fileName.toString()))
  } else {
    Files.newDirectoryStream(sourceDir, "*.xml").each { Path p ->
      def n = p.fileName.toString()
      if (isJhove(n)) return
      safeCopy(p, deprMetsDir.resolve(n))
    }
  }

  // ----------------------------------------------------------------
  // 4) From extractDir:
  // ----------------------------------------------------------------
  boolean scanityCopied = false

  if (Files.isDirectory(extractDir)) {
    Files.walk(extractDir).forEach { Path p ->
      if (!Files.isRegularFile(p)) return
      def n = p.fileName.toString()
      if (isJhove(n)) return
      if (n == ".extract_complete") return

      if (!scanityCopied && n.toLowerCase().contains("scanitytransfer") && n.toLowerCase().endsWith(".xml")) {
        safeCopy(p, preservationDir.resolve("ScanityTransfer.xml"))
        scanityCopied = true
        return
      }

      if (n.startsWith("METS_") && n.toLowerCase().endsWith(".xml")) {
        safeCopy(p, deprMetsDir.resolve(n))
        return
      }
    }
  }

  // ----------------------------------------------------------------
  // 5) Unclassified capture
  // ----------------------------------------------------------------

  // 5a) Source top-level regular files
  Files.newDirectoryStream(sourceDir).each { Path p ->
    if (!Files.isRegularFile(p)) return
    def n = p.fileName.toString()
    if (isIgnorableMarkerFile(n)) return
    if (isHandledSourceFile(n)) return
    safeCopyLazy(p, unclassifiedSourceDir.resolve(n))
  }

  // 5b) Source meta/ regular files
  Path metaDir2 = sourceDir.resolve("meta")
  if (Files.isDirectory(metaDir2)) {
    Files.newDirectoryStream(metaDir2).each { Path p ->
      if (!Files.isRegularFile(p)) return
      def n = p.fileName.toString()
      if (isIgnorableMarkerFile(n)) return
      if (isHandledSourceFile(n)) return
      safeCopyLazy(p, unclassifiedSourceDir.resolve(n))
    }
  }

  // 5c) Extracted dir leftovers
  if (Files.isDirectory(extractDir)) {
    Files.walk(extractDir).forEach { Path p ->
      if (!Files.isRegularFile(p)) return
      def n = p.fileName.toString()
      if (isIgnorableMarkerFile(n)) return
      if (isHandledExtractedFile(n)) return
      Path rel = extractDir.relativize(p)
      safeCopyLazy(p, unclassifiedExtractedDir.resolve(rel))
    }
  }

  // ----------------------------------------------------------------
  // Final: set review flag
  // ----------------------------------------------------------------
  ff = session.putAttribute(ff, 'metadata.org.status', 'OK')
  ff = session.putAttribute(ff, 'metadata.org.review.required', wroteUnclassified ? 'true' : 'false')
  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  ff = session.putAttribute(ff, 'metadata.org.status', 'FAIL')
  ff = session.putAttribute(ff, 'metadata.org.error', e.toString())
  session.transfer(ff, REL_FAILURE)
}
