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
    def transferDir = Paths.get(getAttr("transfer.dir") ?: "/")

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
                it.name.contains('_WORK_') && it.name.endsWith('.xml')
            }
            ?.collect { it.toPath() } ?: []
    }

    // ----------------------
    // COLLECTIONS
    // ----------------------
    def mainTitle = null
    def alternativeTitles = []

    def creators = []
    def contributors = []
    def publishers = []
    def relations = []
    def countries = []
    def dates = []
    def descriptions = []

    // ----------------------
    // PARSE XML FILES
    // ----------------------
    xmlPaths.each { path ->

        def records = new XmlSlurper(false, false)
            .parse(path.toFile())
            ?.recordList?.record

        if (!records) return

        records.each { record ->

            // --- TITLES ---
            record.Title.each { t ->
            // record.'**'.findAll { it.name() == 'Title' }.each { t ->

                def value = t.title?.text()?.trim()
                def type = t.'title.type'?.text()?.trim()

                if (!value) return

                if (type?.equalsIgnoreCase("Originaltittel")) {
                    if (!mainTitle) mainTitle = [value: value]
                } else {
                    // DPS requires a non-empty alternative title type; fall back
                    // to "Ukjent" when the catalog record has no title.type.
                    alternativeTitles << [type: type ?: "Ukjent", value: value]
                }
            }

            // --- CREDITS ---
            record.'**'.findAll {
                it.name().toString().toLowerCase().endsWith('credits')
            }.each { node ->

                def creditName = node.'credit.name'?.text()?.trim()
                def creditType = node.'credit.type'?.text()?.trim()

                if (!creditName) return

                if ("Regissør".equalsIgnoreCase(creditType)) {
                    creators << [name: creditName,  type: "Person", role: creditType]
                }

                if ("Produksjonsselskap".equalsIgnoreCase(creditType)) {
                    contributors << [name: creditName,  type: "korporasjon", role: creditType]
                }

                if ("Produksjon/produsent".equalsIgnoreCase(creditType)) {
                    contributors << [name: creditName,  type: "Person", role: creditType]
                }

                if ("Distribusjon".equalsIgnoreCase(creditType)) {
                    // publishers << [type: creditType, value: creditName]
                    publishers << [name: creditName, type: "korporasjon"]
                }

                if ("Kringkaster".equalsIgnoreCase(creditType)) {
                    publishers << [name: creditName, type: "korporasjon"]
                }

                if ("Utgiver".equalsIgnoreCase(creditType)) {
                    publishers << [name: creditName, type: "korporasjon"]
                }
            }

            // --- DATES ---
            record.'**'.findAll {
                it.name().toString().toLowerCase().endsWith('dating')
            }.each { node ->

                def dateStart = node.'dating.date.start'?.text()?.trim()
                def dateType = node.'dating.type'?.text()?.trim()

                if (!dateStart) return

                // DPS requires a non-empty date type; fall back to "Ukjent"
                // when the catalog record has no dating.type.
                dates << [type: dateType ?: "Ukjent", value: dateStart]
            }

            // --- RELATION segment_of ---
            record.'**'.findAll {
                it.name().toString().toLowerCase().endsWith('segment_of')
            }.each { node ->

                def segmentOfTitle = node.'segment_of.title'?.text()?.trim()
                def segmentOfLref = node.'segment_of.lref'?.text()?.trim()

                if (!segmentOfLref) return

                relations << [
                    title: segmentOfTitle,
                    id: segmentOfLref,
                    type: 'SegmentOf'
                ]
            }

            // --- RELATION av_series ---
            record.'**'.findAll {
                it.name().toString().toLowerCase().endsWith('av_series')
            }.each { node ->

                def avSeriesTitle = node.'av_series.title'?.text()?.trim()
                def avSeriesLref = node.'av_series.lref'?.text()?.trim()
                def avSeriesEpisodNo = node.'av_series.episode_no'?.text()?.trim()

                if (!avSeriesLref) return

                if (avSeriesTitle) {
                    relations << [
                        title: avSeriesTitle,
                        id: avSeriesLref,
                        type: 'IsPartOfSeries'
                    ]
                }

                if (avSeriesEpisodNo) {
                    relations << [
                        title: avSeriesEpisodNo,
                        id: avSeriesLref,
                        type: 'IsPartOfSeries'
                    ]
                }
            }

            // --- RELATION related_object ---
            record.'**'.findAll {
                it.name().toString().toLowerCase().endsWith('related_object')
            }.each { node ->

                def relatedObjectTitle = node.'related_object.title'?.text()?.trim()
                def relatedObjectLref = node.'related_object.reference.lref'?.text()?.trim()

                if (!relatedObjectLref) return

                relations << [
                    title: relatedObjectTitle,
                    id: relatedObjectLref,
                    type: 'RelatedTo'
                ]
            }

            // --- COUNTRIES ---
            record.production_country.each { pidNode ->
                def value = pidNode.text()?.trim()
                if (value) countries << value
            }

            // --- DESCRIPTION ---
           record.'**'.findAll { it.name() == 'Description' }.each { node ->

                def descriptionText = node.'description'?.text()?.trim()
                def descriptionType = node.'description.type'?.text()?.trim()

                if (!descriptionText) return
 
                      if ("Norsk til publisering".equalsIgnoreCase(descriptionType)) {
                         descriptionType = "nob"
                      }
                      else if ("Engelsk til publisering".equalsIgnoreCase(descriptionType)) {
                        descriptionType = "eng"
                      } 
                     else {
                        descriptionType = "nob"
                     }
                   
                descriptions << [value: descriptionText, lang: descriptionType]
            }
        }
    }

    // ----------------------
    // SET ATTRIBUTES
    // ----------------------

    // --- TITLES MAIN ---
    if (!mainTitle && alternativeTitles) {
        mainTitle = [value: alternativeTitles[0].value]
    }

    if (mainTitle) {
        ff = session.putAttribute(
            ff,
            "metadata.mainTitle",
            new JsonBuilder([title: mainTitle]).toString()
        )
    }

    // --- TITLES ALTERNATIVE ---
    /**
    if (alternativeTitles) {
        def uniqueAlternative = alternativeTitles
            .unique { it.type + it.value }
            .collect {
                [type: it.type, value: it.value]
            }

        ff = session.putAttribute(
            ff,
            "metadata.alternativeTitles",
            new JsonBuilder([alternative: uniqueAlternative]).toString()
        )
    }
    **/
// --- TITLES ALTERNATIVE ---
if (alternativeTitles && mainTitle?.value) {

    def uniqueAlternative = alternativeTitles
        // remove ones that match mainTitle
        .findAll { it.value != mainTitle.value }
        // deduplicate
        .unique { it.type + it.value }
        // map structure
        .collect {
            [type: it.type, value: it.value]
        }

    ff = session.putAttribute(
        ff,
        "metadata.alternativeTitles",
        new JsonBuilder([alternative: uniqueAlternative]).toString()
    )
}    



    // --- DATES ---
    if (dates) {
        def uniqueDates = dates
            .unique { it.type + it.value }
            .collect {
                [type: it.type, value: it.value]
            }

        ff = session.putAttribute(
            ff,
            "metadata.dates",
            new JsonBuilder([date: uniqueDates]).toString()
        )
    }

    // --- CREATORS ---
    if (creators) {
        def uniqueCreators = creators
            .unique { it.name + it.type + it.role }
            .collect {
                [name: it.name, type: it.type, role: it.role]
            }

        ff = session.putAttribute(
            ff,
            "metadata.creators",
            new JsonBuilder([creator: uniqueCreators]).toString()
        )
    }

    // --- CONTRIBUTORS ---
    if (contributors) {
        def uniqueContributors = contributors
           .unique { it.name + it.type + it.role }
             .collect {
                [name: it.name, type: it.type, role: it.role]
            }

        ff = session.putAttribute(
            ff,
            "metadata.contributors",
            new JsonBuilder([contributor: uniqueContributors]).toString()
        )
    }

    // --- PUBLISHERS ---
    if (publishers) {
        def uniquePublishers = publishers
            .unique { it.name + it.type }
            .collect {
                [name: it.name, type: it.type]
            }

        ff = session.putAttribute(
            ff,
            "metadata.publishers",
            new JsonBuilder([publisher: uniquePublishers]).toString()
        )
    }

    // --- RELATION ---
    if (relations) {
        def uniqueRelations = relations
            .unique { it.title + it.id + it.type + it.lang }
            .collect {
                [title: it.title, id: it.id, type: it.type]
            }

        ff = session.putAttribute(
            ff,
            "metadata.workRelations",
            new JsonBuilder([relation: uniqueRelations]).toString()
        )
    }

    // --- COUNTRIES ---
    if (countries) {
        def spatial = countries.collect {
            [name: it, type: "Country"]
        }

        ff = session.putAttribute(
            ff,
            "metadata.countries",
            JsonOutput.toJson([spatial: spatial])
        )
    }

    // --- DESCRIPTION ---
    if (descriptions) {
        def uniqueDescriptions = descriptions
            .unique { it.value + it.lang }
            .collect {
                [value: it.value, lang: it.lang]
            }

        ff = session.putAttribute(
            ff,
            "metadata.descriptions",
            new JsonBuilder([description: uniqueDescriptions]).toString()
        )
    }

    // ----------------------
    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, "submission.payload.status", "FAIL")
    ff = session.putAttribute(ff, "submission.payload.error", e.toString())
    session.transfer(ff, REL_FAILURE)
}