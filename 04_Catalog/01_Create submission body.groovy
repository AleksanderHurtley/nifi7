import java.nio.charset.StandardCharsets
import java.nio.file.*
import groovy.json.JsonOutput
import groovy.xml.XmlSlurper

def ff = session.get()
if (!ff) return

def getAttr = { String k ->
  def v = ff.getAttribute(k)
  (v && v.trim()) ? v.trim() : null
}

def pkg = getAttr("package.name")
def submissionPathStr = getAttr("submission.payload.path")
def metadataDescDirStr = getAttr("metadata.descriptive.dir")

def missing = []
[
  ["package.name", pkg],
  ["submission.payload.path", submissionPathStr],
  ["metadata.descriptive.dir", metadataDescDirStr]
].each { if (!it[1]) missing << it[0] }

if (!missing.isEmpty()) {
  ff = session.putAttribute(ff, "submission.payload.status", "FAIL")
  ff = session.putAttribute(ff, "submission.payload.error",
      "Missing attributes: ${missing.join(', ')}")
  session.transfer(ff, REL_FAILURE)
  return
}

try {
  Path submissionPath = Paths.get(submissionPathStr)
  Files.createDirectories(submissionPath.getParent())

  Path tmpPath = submissionPath.resolveSibling(
      submissionPath.getFileName().toString() + ".tmp"
  )

  // 1) Find the WORK XML file
  Path transferDir = Paths.get(getAttr("transfer.dir") ?: "/")
  Path transferDescDir = transferDir.resolve("metadata/descriptive")

  List<Path> dirsToCheck = [
    Paths.get(metadataDescDirStr),
    transferDescDir
  ]

  Path workXmlPath = null

  for (Path d : dirsToCheck) {
    if (d != null && d.toString() != "" && Files.exists(d) && Files.isDirectory(d)) {
      try {
        def files = d.toFile().listFiles()
        if (files != null) {
          def workFile = files.find { it.name.contains('_WORK_') && it.name.endsWith('.xml') }
          if (workFile != null) {
            workXmlPath = workFile.toPath()
            break
          }
        }
      } catch (Exception ignore) {}
    }
  }

  // Basic default values for required fields
  String extractedTitle = "Unknown Title"
  def identifiers = [
    [
      type: "URN",
      value: "URN:NBN:no-nb_" + pkg
    ]
  ]


  // 2) Parse XML if found to extract actual data
  if (workXmlPath != null) {
    def root = new XmlSlurper().parse(workXmlPath.toFile())
    def record = root.recordList.record[0]
    
    // Extract title (prefer originaltittel, fallback to first)
    def titleNodes = record.Title
    if (titleNodes.size() > 0) {
      def origTitle = titleNodes.find { it.'title.type'.text() == 'Originaltittel' }
      if (origTitle) {
        extractedTitle = origTitle.title.text()
      } else {
        extractedTitle = titleNodes[0].title.text()
      }
    }
    

  }

  // ------------------------------------------------------------
  // Construct submission payload
  // ------------------------------------------------------------
  def payload = [
    objectId: pkg,
    priority: 75,
    metadata: [
      type: "Film",
      identifier: identifiers,
      title: [
        value: extractedTitle
      ]
    ]
  ]



  def json = JsonOutput.prettyPrint(JsonOutput.toJson(payload))

  Files.write(tmpPath, json.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE)

  Files.move(tmpPath, submissionPath,
      StandardCopyOption.REPLACE_EXISTING,
      StandardCopyOption.ATOMIC_MOVE)

  ff = session.putAttribute(ff, "submission.payload.status", "OK")
  ff = session.putAttribute(ff, "submission.payload.path", submissionPathStr)

  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  ff = session.putAttribute(ff, "submission.payload.status", "FAIL")
  ff = session.putAttribute(ff, "submission.payload.error", e.toString())
  session.transfer(ff, REL_FAILURE)
}
