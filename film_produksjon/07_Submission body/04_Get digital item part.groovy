import java.nio.file.*
import groovy.xml.XmlSlurper
import groovy.json.JsonBuilder

def ff = session.get()
if (!ff) return

// Rebuild this stage's outputs; missing URNs/relations must not reuse another run.
[
    'metadata.pidDataUrns',
    'metadata.digitalItemPartRelations',
    'metadata.provenance',
    'submission.payload.status',
    'submission.payload.error'
].each { key -> ff = session.removeAttribute(ff, key) }

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
    // FIND ALL XML FILES
    // ----------------------
    // Prefer this export type in work; do not mix it with an older SIP copy.
    def xmlPaths = dirsToCheck.collect { dir ->
        if (Files.isDirectory(dir)) {
            dir.toFile().listFiles()
                ?.findAll {
                    it.name.contains('_DIGITAL_ITEM_PART_') &&
                    it.name.endsWith('.xml')
                }
                ?.collect { it.toPath() } ?: []
        } else {
            []
        }
    }.find { !it.isEmpty() } ?: []

    // ----------------------
    // COLLECTIONS
    // ----------------------
    def alternativeNumbers = []
    def pidDataUrns        = []
    def relations          = []

    // ----------------------
    // PARSE XML FILES (ALL RECORDS)
    // ----------------------
    xmlPaths.each { path ->

        def records = new XmlSlurper()
            .parse(path.toFile())
            ?.recordList?.record

        if (!records) return

        records.each { record ->

            // --- ALTERNATIVE NUMBERS ---
            /*
            record.'**'
                .findAll { it.name() == 'Alternative_number' }
                .each { node ->

                    def typeVal  = node.'alternative_number.type'?.text()?.trim()
                    def valueVal = node.'alternative_number'?.text()?.trim()

                    if (typeVal && valueVal) {
                        alternativeNumbers << [type: typeVal, value: valueVal]
                    }
                }
            */
            
            // --- PID_data_URN ---
            record.PID_data_URN.each { pidNode ->
                def value = pidNode.text()?.trim()
                if (value) pidDataUrns << value
            }

            // -- RELATION copied_from ---
            // Direct links belong to this item part; nested parent records have
            // their own copy history and must not supply this carrier's sources.
            record.CopiedFrom
                .each { node ->

                    def copiedFromTitle = node.'copied_from.title'?.text()?.trim()
                    def copiedFromLref  = node.'copied_from.lref'?.text()?.trim()
                    def copiedFromLang  = ''

                    if (!copiedFromLref) return

                    relations << [
                        title: copiedFromTitle,
                        id   : copiedFromLref,
                        type : 'CopiedFrom',
                        lang : copiedFromLang
                    ]
                }

            // -- RELATION copied_to ---
            record.CopiedTo
                .each { node ->

                    def copiedToTitle = node.'copied_to.title'?.text()?.trim()
                    def copiedToLref  = node.'copied_to.lref'?.text()?.trim()
                    def copiedToLang  = ''

                    if (!copiedToLref) return

                    relations << [
                        title: copiedToTitle,
                        id   : copiedToLref,
                        type : 'CopiedTo',
                        lang : copiedToLang
                    ]
                }
        }
    }

    // ----------------------
    // SET ATTRIBUTES
    // ----------------------
    
    // --- IDENTIFIER (URN only here, barcode handled later)
    if (!pidDataUrns.isEmpty()) {
        ff = session.putAttribute(
            ff,
            "metadata.pidDataUrns",
            pidDataUrns.unique().join(",")
        )
    }

    // --- RELATION ---
    if (relations) {

        def uniqueRelations = relations
            .unique { it.title + it.id + it.type + it.lang }
            .collect {
                [
                    title: it.title,
                    id   : it.id,
                    type : it.type,
                    lang : it.lang
                ]
            }

        ff = session.putAttribute(
            ff,
            "metadata.digitalItemPartRelations",
            new JsonBuilder([relation: uniqueRelations]).toString()
        )
    }

    // -- PROVENANCE--
    def provenance = [
        provenance: [[
            value: "Digitale sikringsfiler og mezzaninfiler produsert ved NB for bevaring og formidling. Filtypene dette gjelder er digitale sikringsfiler fra film- og lydskannere (DPX og Wav) og etterarbeidede masterfiler (mezzanin/MOV). Dette er bilde- og lydfiler som er kopiert fra analogt filmmateriale.",
            lang : "nob"
        ]]
    ]

    ff = session.putAttribute(
        ff,
        "metadata.provenance",
        new JsonBuilder(provenance).toString()
    )

    // ----------------------
    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, "submission.payload.status", "FAIL")
    ff = session.putAttribute(ff, "submission.payload.error", e.toString())
    session.transfer(ff, REL_FAILURE)
}
