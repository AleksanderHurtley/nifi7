import java.nio.file.*
import groovy.xml.XmlSlurper
import groovy.json.JsonBuilder
import groovy.json.JsonOutput

def ff = session.get()
if (!ff) return

def getAttr = { k -> ff.getAttribute(k)?.trim() ?: null }

// ----------------------
// VALIDATE ATTRIBUTES
// ----------------------
def requiredAttrs = [
    "package.name",
    "submission.payload.path",
    "metadata.descriptive.dir"
]

def values = requiredAttrs.collectEntries { [(it): getAttr(it)] }
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
    // FIND XML FILES
    // ----------------------
    def xmlPaths = dirsToCheck.collectMany { dir ->
        if (!Files.isDirectory(dir)) return []

        dir.toFile().listFiles()
            ?.findAll {
                it.name.contains('_DIGITAL_ITEM_') &&
                !it.name.contains('_DIGITAL_ITEM_PART_') &&
                it.name.endsWith('.xml')
            }
            ?.collect { it.toPath() } ?: []
    }

    // ----------------------
    // COLLECTIONS
    // ----------------------
    def languages = []

    // ----------------------
    // PARSE XML FILES
    // ----------------------
    xmlPaths.each { path ->

        def records = new XmlSlurper(false, false)
            .parse(path.toFile())
            ?.recordList?.record

        if (!records) return

        records.each { record ->
            // --- LANGUAGE USAGE ---
            record.'**'
                .findAll { it.name().toString().toLowerCase().endsWith('language') }
                .each { node ->

                    def languageName = node.'language'?.text()?.trim()
                    def languageType = node.'language.usage'?.text()?.trim()

                    if (!languageName) return

                    if ("Norsk".equalsIgnoreCase(languageName)) {
                       languageName = "nob"
                    }

                    // DPS requires a non-empty language type; fall back to
                    // "Ukjent" when the catalog record has no language.usage.
                    languages << [
                        type : languageType ?: "Ukjent",
                        value: languageName
                    ]
                }
        }
    }

    // ----------------------
    // SET ATTRIBUTES
    // ----------------------

    // --- LANGUAGE USAGE ---
    if (languages) {
        def languagesS = languages
            .unique { it.type + it.value }
            .collect {
                [type: it.type, value: it.value]
            }

        ff = session.putAttribute(
            ff,
            "metadata.languages",
            new JsonBuilder([language: languagesS]).toString()
        )
    }

    // ----------------------
    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, "submission.payload.status", "FAIL")
    ff = session.putAttribute(
        ff,
        "submission.payload.error",
        e.toString()
    )
    session.transfer(ff, REL_FAILURE)
}