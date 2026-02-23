import java.nio.charset.StandardCharsets
import java.nio.file.*
import javax.xml.parsers.DocumentBuilderFactory
import org.w3c.dom.Element
import groovy.xml.MarkupBuilder

def ff = session.get()
if (!ff) return

def getAttr = { String k ->
  def v = ff.getAttribute(k)
  (v && v.trim()) ? v.trim() : null
}

// ------------------------------------------------------------
// Required attributes
// ------------------------------------------------------------
def pkg           = getAttr('package.name')
def extractDirStr = getAttr('metadata.extract.dir')
def outDirStr     = getAttr('metadata.preservation.dpx.dir')

def missing = []
[['package.name', pkg],
 ['metadata.extract.dir', extractDirStr],
 ['metadata.preservation.dpx.dir', outDirStr]
].each { if (!it[1]) missing << it[0] }

if (!missing.isEmpty()) {
  ff = session.putAttribute(ff, 'dpxmeta.status', 'FAIL')
  ff = session.putAttribute(ff, 'dpxmeta.error', "Missing attributes: ${missing.join(',')}")
  session.transfer(ff, REL_FAILURE)
  return
}

Path extractDir = Paths.get(extractDirStr)
Path outDir     = Paths.get(outDirStr)

try {
  if (!Files.isDirectory(extractDir)) {
    throw new RuntimeException("metadata.extract.dir is not a directory: ${extractDir}")
  }
  Files.createDirectories(outDir)

  // ------------------------------------------------------------
  // XML parser setup
  // ------------------------------------------------------------
  def dbf = DocumentBuilderFactory.newInstance()
  dbf.setNamespaceAware(false)
  def builder = dbf.newDocumentBuilder()

  def textOf = { Element parent, String tag ->
    def nl = parent.getElementsByTagName(tag)
    if (!nl || nl.getLength() == 0) return null
    def v = nl.item(0)?.getTextContent()
    v != null ? v.trim() : null
  }

  def parseBatchId = { String metaFileName ->
    if (metaFileName.contains("_pre.tar.xml")) return "batchPRE"
    def m = (metaFileName =~ /_(\d{4})\.tar\.xml$/)
    if (m.find()) return "batch${m.group(1)}"
    return null
  }

  // ------------------------------------------------------------
  // Find META_*.tar.xml files
  // ------------------------------------------------------------
  def metaFiles = []
  Files.newDirectoryStream(extractDir, "META_*.tar.xml").withCloseable { ds ->
    ds.each { Path p -> if (Files.isRegularFile(p)) metaFiles << p }
  }

  if (metaFiles.isEmpty()) {
    ff = session.putAttribute(ff, 'dpxmeta.status', 'FAIL')
    ff = session.putAttribute(ff, 'dpxmeta.error', "No META_*.tar.xml found in ${extractDir}")
    session.transfer(ff, REL_FAILURE)
    return
  }

  // batchId -> ordered list of filenames
  def batchMap = new LinkedHashMap<String, List<String>>()
  // filename -> md5
  def md5ByFile = new LinkedHashMap<String, String>()
  def md5Conflicts = []

  long dpxEntriesSeen = 0

  metaFiles = metaFiles.sort { it.fileName.toString() }

  for (Path metaPath : metaFiles) {
    def metaName = metaPath.fileName.toString()
    def batchId  = parseBatchId(metaName)
    if (!batchId) continue

    def inDoc = builder.parse(metaPath.toFile())
    Element root = inDoc.getDocumentElement()
    def dpxNodes = root.getElementsByTagName("dpxMetadata")

    def fileList = (batchMap[batchId] ?: [])

    for (int i = 0; i < dpxNodes.getLength(); i++) {
      def el = (Element) dpxNodes.item(i)

      def fileName = textOf(el, "fileName")
      def md5      = textOf(el, "md5Checksum")?.toLowerCase()

      if (!fileName || !md5) continue
      if (!(md5 ==~ /[a-f0-9]{32}/)) {
        throw new RuntimeException("Invalid MD5 value for ${fileName} in ${metaName}: ${md5}")
      }

      dpxEntriesSeen++
      fileList << fileName

      def existing = md5ByFile[fileName]
      if (existing == null) {
        md5ByFile[fileName] = md5
      } else if (existing != md5) {
        md5Conflicts << [file: fileName, md5_first: existing, md5_next: md5, source: metaName]
      }
    }

    batchMap[batchId] = fileList
  }

  if (batchMap.isEmpty()) {
    throw new RuntimeException("No batch IDs could be parsed from META_*.tar.xml filenames.")
  }
  if (md5ByFile.isEmpty()) {
    throw new RuntimeException("No DPX md5 checksums found in META_*.tar.xml files.")
  }
  if (!md5Conflicts.isEmpty()) {
    throw new RuntimeException("Conflicting MD5 values for same DPX filename (sample): " +
      md5Conflicts.take(3).collect { "${it.file} ${it.md5_first} != ${it.md5_next}" }.join("; "))
  }

  // ------------------------------------------------------------
  // Write unified DPX manifest XML
  // ------------------------------------------------------------
  Path manifestPath = outDir.resolve("${pkg}_dpx_manifest.xml")
  Path manifestTmp  = manifestPath.resolveSibling(manifestPath.fileName.toString() + ".tmp")

  def batchKey = { String batchId ->
    if (batchId == "batchPRE") return -1
    def m = (batchId =~ /batch(\d{4})/)
    return m.find() ? m.group(1).toInteger() : 999999
  }

  def orderedBatchIds = batchMap.keySet().collect { it.toString() }.sort { a, b ->
    batchKey(a) <=> batchKey(b) ?: (a <=> b)
  }

  def sw = new StringWriter()
  def xml = new MarkupBuilder(sw)
  xml.setDoubleQuotes(true)
  xml.mkp.xmlDeclaration(version: "1.0", encoding: "UTF-8")
  xml.dpxManifest(packageId: pkg, checksumAlgorithm: "MD5", createdFrom: "metadata.extract.dir") {
    batches {
      orderedBatchIds.each { String batchId ->
        batch(id: batchId) {
          (batchMap[batchId] ?: []).each { String fname ->
            def md5 = md5ByFile[fname]
            if (!md5) {
              throw new RuntimeException("No checksum found for DPX file '${fname}' in batch '${batchId}'")
            }
            file(name: fname, md5: md5)
          }
        }
      }
    }
  }

  def manifestXml = sw.toString() + "\n"
  Files.write(manifestTmp, manifestXml.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
  try {
    Files.move(manifestTmp, manifestPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  } catch (Exception ignore) {
    Files.move(manifestTmp, manifestPath, StandardCopyOption.REPLACE_EXISTING)
  }

  ff = session.putAttribute(ff, 'dpxmeta.status', 'OK')
  ff = session.putAttribute(ff, 'dpxmeta.out.dir', outDir.toString())
  ff = session.putAttribute(ff, 'dpxmeta.manifest.path', manifestPath.toString())
  ff = session.putAttribute(ff, 'dpxmeta.batches.count', batchMap.keySet().size().toString())
  ff = session.putAttribute(ff, 'dpxmeta.files.count', md5ByFile.keySet().size().toString())
  ff = session.putAttribute(ff, 'dpxmeta.entries.seen', dpxEntriesSeen.toString())

  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  ff = session.putAttribute(ff, 'dpxmeta.status', 'FAIL')
  ff = session.putAttribute(ff, 'dpxmeta.error', e.toString())
  session.transfer(ff, REL_FAILURE)
}
