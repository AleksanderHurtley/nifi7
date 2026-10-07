import java.nio.file.*
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Arrays
import groovy.xml.XmlSlurper

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = "fetch.metadata.organize"
final int ERROR_DETAILS_MAX = 2048

def capDetails = { s ->
  if (s == null) return null
  String t = s.toString()
  (t.length() > ERROR_DETAILS_MAX) ? t.substring(0, ERROR_DETAILS_MAX) : t
}

def setFailure = { flowFile, String message, String details = null ->
  def out = session.putAttribute(flowFile, "error.stage", ERROR_STAGE)
  out = session.putAttribute(out, "error.message", message ?: "Metadata organization failed")
  if (details != null && details.toString().trim()) {
    out = session.putAttribute(out, "error.details", capDetails(details))
  }
  return out
}

def pkg                = ff.getAttribute('package.name')
def sourceDirStr       = ff.getAttribute('source.dir')
def repDirStr          = ff.getAttribute('rep.dir')
def extractDirStr      = ff.getAttribute('metadata.extract.dir')
def workDirStr         = ff.getAttribute('work.dir')

def descriptiveDirStr  = ff.getAttribute('metadata.descriptive.dir')
def deprMetsDirStr     = ff.getAttribute('metadata.descriptive.depr_mets.dir')
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
  ['metadata.descriptive.depr_mets.dir', deprMetsDirStr],
  ['metadata.preservation.dir', preservationDirStr],
  ['metadata.other.unclassified.dir', unclassifiedBaseStr]
].each { kv ->
  if (!(kv[1]?.trim())) missing << kv[0]
}

if (!missing.isEmpty()) {
  def msg = "Missing attributes: ${missing.join(',')}"
  ff = session.putAttribute(ff, 'metadata.org.status', 'FAIL')
  ff = session.putAttribute(ff, 'metadata.org.error', msg)
  ff = setFailure(ff, msg)
  session.transfer(ff, REL_FAILURE)
  return
}

Path sourceDir = Paths.get(sourceDirStr)
if (!Files.isDirectory(sourceDir)) {
  def msg = "Source dir not found: ${sourceDirStr}"
  ff = session.putAttribute(ff, 'metadata.org.status', 'FAIL')
  ff = session.putAttribute(ff, 'metadata.org.error', msg)
  ff = setFailure(ff, msg)
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

  final Set<String> IGNORED_LEGACY_CHECKSUM_FILES = [
    "checksum.md5",
    "images.md5"
  ] as Set

  def isIgnoredLegacyChecksum = { String name ->
    IGNORED_LEGACY_CHECKSUM_FILES.contains((name ?: "").toLowerCase())
  }

  def isScanityTransferFile = { String name ->
    def n = (name ?: "").toLowerCase()
    n.contains("scanitytransfer") && (n.endsWith(".xml") || n.endsWith(".xml~"))
  }

  def isIgnorableMarkerFile = { String name ->
    def n = (name ?: "").toLowerCase()
    if (n.endsWith(".done")) return true
    if (n == ".ds_store") return true
    if (n == "thumbs.db") return true
    if (n == "desktop.ini") return true
    if (n == "renaming.txt") return true 
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

  def filesEqual = { Path a, Path b ->
    if (!Files.isRegularFile(a) || !Files.isRegularFile(b)) return false
    if (Files.size(a) != Files.size(b)) return false
    Arrays.equals(Files.readAllBytes(a), Files.readAllBytes(b))
  }

  def sha256Prefix = { Path p ->
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(p))
    digest.collect { String.format("%02x", it) }.join().substring(0, 12)
  }

  def scanityFrameRange = { Path p ->
    def xml = new XmlSlurper(false, false).parse(p.toFile())
    def clips = xml.WorkItem.Material.Element.PullClip
    if (clips.size() != 1) {
      throw new RuntimeException("Expected exactly one Scanity PullClip in ${p}, found ${clips.size()}")
    }

    String firstText = clips[0].FirstFrame.text()?.trim()
    String lastText  = clips[0].LastFrame.text()?.trim()
    if (!(firstText ==~ /\d+/) || !(lastText ==~ /\d+/)) {
      throw new RuntimeException("Missing or invalid Scanity frame range in ${p}: first='${firstText}', last='${lastText}'")
    }

    long first = firstText.toLong()
    long last  = lastText.toLong()
    if (last < first) {
      throw new RuntimeException("Invalid Scanity frame range in ${p}: ${first}-${last}")
    }

    [first: first, last: last]
  }

  int additionalScanityCopied = 0
  int duplicateScanitySkipped = 0
  int fallbackScanityNamed = 0
  int ignoredLegacyChecksumCount = 0

  def copyAdditionalScanity = { Path src, Path canonicalDst ->
    if (Files.isRegularFile(canonicalDst) && filesEqual(src, canonicalDst)) {
      duplicateScanitySkipped++
      log.info("Skipping byte-identical additional ScanityTransfer file: ${src}")
      return
    }

    String baseName
    try {
      def range = scanityFrameRange(src)
      baseName = "ScanityTransfer-range-${range.first}-${range.last}"
    } catch (Exception rangeError) {
      fallbackScanityNamed++
      baseName = "ScanityTransfer-extra-${sha256Prefix(src)}"
      log.warn("Could not derive a Scanity frame range from ${src}; preserving it as ${baseName}.xml: ${rangeError.message}")
    }
    Path dst = preservationDir.resolve(baseName + ".xml")

    if (Files.exists(dst)) {
      if (filesEqual(src, dst)) {
        duplicateScanitySkipped++
        log.info("Skipping byte-identical additional ScanityTransfer file already staged as ${dst}: ${src}")
        return
      }
      dst = preservationDir.resolve(baseName + "-" + sha256Prefix(src) + ".xml")
    }

    if (Files.exists(dst)) {
      if (filesEqual(src, dst)) {
        duplicateScanitySkipped++
        log.info("Skipping byte-identical additional ScanityTransfer file already staged as ${dst}: ${src}")
        return
      }
      throw new RuntimeException("Refusing to overwrite different ScanityTransfer metadata: ${dst}")
    }

    Files.createDirectories(dst.getParent())
    Files.copy(src, dst, StandardCopyOption.COPY_ATTRIBUTES)
    additionalScanityCopied++
    log.info("Preserved additional ScanityTransfer metadata: ${src} -> ${dst}")
  }

  // ----------------------------------------------------------------
  // Check if a file is "known/handled"
  // ----------------------------------------------------------------
  def isHandledSourceFile = { String name ->
    if (isIgnorableMarkerFile(name)) return true
    if (isJhove(name)) return true
    if (isIgnoredLegacyChecksum(name)) return true
    if (isScanityTransferFile(name)) return true
    if (name == "${pkg}.xml") return true
    if (name.toLowerCase().endsWith("_meta_xml.tar")) return true
    if (name.startsWith("MAVIS_") && name.toLowerCase().endsWith(".xml")) return true
    return false
  }

  def isHandledExtractedFile = { String name ->
    if (isIgnorableMarkerFile(name)) return true
    if (isJhove(name)) return true
    if (isIgnoredLegacyChecksum(name)) return true
    if (name == ".extract_complete") return true
    if (name.startsWith("META_") && name.endsWith(".tar.xml")) return true // intentionally kept only in extractDir
    if (name.startsWith("METS_") && name.toLowerCase().endsWith(".xml")) return true
    if (isScanityTransferFile(name)) return true
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
  // 3) Copy the deprecated MAVIS XML(s) from sourceDir into descriptive/deprecated_mets/
  // ----------------------------------------------------------------
  Path topXml = sourceDir.resolve("${pkg}.xml")
  if (Files.isRegularFile(topXml)) {
    safeCopy(topXml, deprMetsDir.resolve(topXml.fileName.toString()))
  } else {
    Files.newDirectoryStream(sourceDir, "*.xml").each { Path p ->
      def n = p.fileName.toString()
      if (isJhove(n)) return
      if (isScanityTransferFile(n)) return
      safeCopy(p, deprMetsDir.resolve(n))
    }
  }

  // ----------------------------------------------------------------
  // 4) ScanityTransfer metadata goes to preservation. Select the canonical
  //    file deterministically and preserve distinct additional/backup files
  //    under frame-range names. Deprecated reel METS files go to
  //    descriptive/deprecated_mets/.
  // ----------------------------------------------------------------
  List<Path> extractedFiles = []
  if (Files.isDirectory(extractDir)) {
    Files.walk(extractDir).forEach { Path p ->
      if (Files.isRegularFile(p)) extractedFiles << p
    }
  }

  List<Path> canonicalCandidates = extractedFiles.findAll { Path p ->
    def n = p.fileName.toString()
    !isJhove(n) && n.toLowerCase().contains("scanitytransfer") && n.toLowerCase().endsWith(".xml")
  }.sort { a, b -> a.toString() <=> b.toString() }

  Path canonicalScanitySource = null
  Path canonicalScanityDst = preservationDir.resolve("ScanityTransfer.xml")
  if (!canonicalCandidates.isEmpty()) {
    String packageName = "${pkg}_ScanityTransfer.xml".toLowerCase()
    canonicalScanitySource = canonicalCandidates.find {
      it.fileName.toString().toLowerCase() == packageName
    } ?: canonicalCandidates.find {
      it.fileName.toString().equalsIgnoreCase("ScanityTransfer.xml")
    } ?: canonicalCandidates[0]

    safeCopy(canonicalScanitySource, canonicalScanityDst)
  }

  List<Path> additionalScanityCandidates = []
  extractedFiles.each { Path p ->
    def n = p.fileName.toString()
    if (!isJhove(n) && isScanityTransferFile(n) && p != canonicalScanitySource) {
      additionalScanityCandidates << p
    }
  }

  // Source-level Scanity files, including editor-style *.xml~ backups.
  [sourceDir, sourceDir.resolve("meta")].each { Path dir ->
    if (!Files.isDirectory(dir)) return
    Files.newDirectoryStream(dir).each { Path p ->
      if (Files.isRegularFile(p) && !isJhove(p.fileName.toString()) && isScanityTransferFile(p.fileName.toString())) {
        additionalScanityCandidates << p
      }
    }
  }

  additionalScanityCandidates
    .unique { it.toAbsolutePath().normalize().toString() }
    .sort { a, b -> a.toString() <=> b.toString() }
    .each { Path p -> copyAdditionalScanity(p, canonicalScanityDst) }

  extractedFiles.each { Path p ->
    def n = p.fileName.toString()
    if (!isJhove(n) && n.startsWith("METS_") && n.toLowerCase().endsWith(".xml")) {
      safeCopy(p, deprMetsDir.resolve(n))
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
    if (isIgnoredLegacyChecksum(n)) {
      ignoredLegacyChecksumCount++
      log.info("Ignoring legacy checksum sidecar; DPX fixity is verified from extracted META metadata: ${p}")
      return
    }
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
      if (isIgnoredLegacyChecksum(n)) {
        ignoredLegacyChecksumCount++
        log.info("Ignoring legacy checksum sidecar; DPX fixity is verified from extracted META metadata: ${p}")
        return
      }
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
      if (isIgnoredLegacyChecksum(n)) {
        ignoredLegacyChecksumCount++
        log.info("Ignoring legacy checksum sidecar; DPX fixity is verified from extracted META metadata: ${p}")
        return
      }
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
  ff = session.putAttribute(ff, 'metadata.org.scanity.additional.count', additionalScanityCopied.toString())
  ff = session.putAttribute(ff, 'metadata.org.scanity.duplicate.count', duplicateScanitySkipped.toString())
  ff = session.putAttribute(ff, 'metadata.org.scanity.fallback.count', fallbackScanityNamed.toString())
  ff = session.putAttribute(ff, 'metadata.org.legacy.checksum.ignored.count', ignoredLegacyChecksumCount.toString())
  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  ff = session.putAttribute(ff, 'metadata.org.status', 'FAIL')
  ff = session.putAttribute(ff, 'metadata.org.error', e.toString())
  ff = setFailure(ff, e.message ?: "Metadata organization failed", e.toString())
  session.transfer(ff, REL_FAILURE)
}
