import java.nio.file.*
import groovy.xml.XmlSlurper
import groovy.json.JsonGenerator

def ff = session.get()
if (!ff) return

def getAttr = { k -> ff.getAttribute(k)?.trim() ?: null }

// ----------------------
// VALIDATE ATTRIBUTES
// ----------------------
def attrs = [
    "package.name",
    "submission.payload.path",
    "metadata.descriptive.dir"
]

def values = attrs.collectEntries { [(it): getAttr(it)] }
def missing = values.findAll { !it.value }.keySet()

if (missing) {
    ff = session.putAttribute(ff, "submission.payload.status", "FAIL")
    ff = session.putAttribute(
        ff,
        "submission.payload.error",
        "Missing attributes: ${missing.join(', ')}"
    )
    session.transfer(ff, REL_FAILURE)
    return
}

try {

    // ----------------------
    // SETUP DIRECTORIES
    // ----------------------
    def metadataDescDir = Paths.get(values["metadata.descriptive.dir"])
    def transferDir     = Paths.get(getAttr("transfer.dir") ?: "/")

    def dirsToCheck = [
        metadataDescDir,
        transferDir.resolve("metadata/descriptive")
    ]

    // ----------------------
    // FIND ALL FILES
    // ----------------------
    def analogItemPartXmlPaths = dirsToCheck.collectMany { dir ->
        if (Files.isDirectory(dir)) {
            dir.toFile().listFiles()
                ?.findAll {
                    it.name.contains('_ANALOG_ITEM_PART_') &&
                    it.name.endsWith('.xml')
                }
                ?.collect { it.toPath() } ?: []
        } else {
            []
        }
    }

    // ----------------------
    // COLLECT BARCODES
    // ----------------------
    def barcodes = []

    analogItemPartXmlPaths.each { path ->

        def record = new XmlSlurper()
            .parse(path.toFile())
            ?.recordList?.record?.getAt(0)

        if (!record) return

        def barcode = record.bar_code?.text()?.trim()

        if (barcode) {
            barcodes << barcode
        }
    }

    // ----------------------
    // BUILD JSON ATTRIBUTE
    // ----------------------
    if (barcodes) {

        def uniqueBarcodes = barcodes.unique()

        ff = session.putAttribute(
            ff,
            "metadata.barcode",
            uniqueBarcodes.join(",")
        )
    }

    // ----------------------
    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, "submission.payload.status", "FAIL")
    ff = session.putAttribute(ff, "submission.payload.error", e.toString())
    session.transfer(ff, REL_FAILURE)
}