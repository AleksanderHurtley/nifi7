import org.apache.nifi.processor.io.StreamCallback
import groovy.json.JsonSlurper
import groovy.json.JsonGenerator
import java.nio.charset.StandardCharsets

flowFile = session.get()
if (!flowFile) return

def attrs = flowFile.getAttributes()
def slurper = new JsonSlurper()

try {
   def pkg = attrs["package.name"]

    // ----------------------
    // CUSTOM PRETTY PRINTER (NO UNICODE ESCAPE)
    // ----------------------
    def prettyPrintJson = { String json ->

        def indent = 0
        def result = new StringBuilder()
        def inQuotes = false

        json.each { ch ->

            switch (ch) {

                case '"':
                    result.append(ch)
                    if (!result.toString().endsWith("\\\"")) {
                        inQuotes = !inQuotes
                    }
                    break

                case '{':
                case '[':
                    result.append(ch)
                    if (!inQuotes) {
                        result.append('\n')
                        indent++
                        result.append('  ' * indent)
                    }
                    break

                case '}':
                case ']':
                    if (!inQuotes) {
                        result.append('\n')
                        indent--
                        result.append('  ' * indent)
                    }
                    result.append(ch)
                    break

                case ',':
                    result.append(ch)
                    if (!inQuotes) {
                        result.append('\n')
                        result.append('  ' * indent)
                    }
                    break

                case ':':
                    result.append(inQuotes ? ':' : ': ')
                    break

                default:
                    result.append(ch)
            }
        }

        return result.toString()
    }

    flowFile = session.write(flowFile, { inputStream, outputStream ->

        // -------------------------------------------------------------------------------
        // Build IDENTIFIER array from URN + BARCODE + ALTERNATIVE IDENTIFIERS (MAVIS etc)
        // -------------------------------------------------------------------------------
        def identifier = []

        // --- URNs ---
        def urnsRaw = attrs["metadata.pidDataUrns"]
        if (urnsRaw) {
            urnsRaw.split(",").each { urn ->
                def cleanUrn = urn.trim()
                if (cleanUrn) {
                    identifier << [
                        type : "URN",
                        value: cleanUrn,
                        lang : "eng"
                    ]
                }
            }
        }

        // --- BARCODES ---
        def barcodesRaw = attrs["metadata.barcode"]
        if (barcodesRaw) {
            barcodesRaw.split(",").each { bc ->
                def cleanBc = bc.trim()
                if (cleanBc) {
                    identifier << [
                        type : "HYLLESIGNATUR",
                        value: cleanBc,
                        lang : "nob"
                    ]
                }
            }
        }

        // --- ALTERNATIVE IDENTIFIERS (MAVIS etc) ---
        /**
        def altJsonRaw = attrs["metadata.identifier.alternative"]
        if (altJsonRaw) {
            def altParsed = slurper.parseText(altJsonRaw)

            altParsed?.identifier?.each { item ->
                if (item?.type && item?.value) {
                    identifier << [
                        type : item.type,
                        value: item.value
                    ]
                }
            }
        }
        **/

        // -------------------------
        // Build RELATIONS array
        // ------------------------
        def relation = []

        def workRelationsRaw = attrs["metadata.workRelations"]
        if (workRelationsRaw) {
            def workRelParsed = slurper.parseText(workRelationsRaw)

            workRelParsed?.relation?.each { item ->
                if (item.id && item?.type) {
                    relation << [
                        title: item.title,
                        id   : item.id,
                        type : item.type                    ]
                }
            }
        }

        def digitalItemPartRelationsRaw = attrs["metadata.digitalItemPartRelations"]
        if (digitalItemPartRelationsRaw) {
            def digitalItemPartRelParsed = slurper.parseText(digitalItemPartRelationsRaw)

            digitalItemPartRelParsed?.relation?.each { item ->
                if (item?.id && item?.type) {
                    relation << [
                        title: item.title,
                        id   : item.id,
                        type : item.type
                    ]
                }
            }
        }

        // ----------------------
        // Parse JSON attributes
        // ----------------------
        /**
        def titles = attrs["metadata.titles"]
            ? slurper.parseText(attrs["metadata.titles"])
            : [:]
        def title = titles?.title ?: []
        def alternative = titles?.alternative ?: []
        **/ 

        def titleJson = attrs["metadata.mainTitle"]
            ? slurper.parseText(attrs["metadata.mainTitle"])
            : [:]
        def title = titleJson?.title ?: []

        def alternativesJson = attrs["metadata.alternativeTitles"]
            ? slurper.parseText(attrs["metadata.alternativeTitles"])
            : [:]
        def alternative = alternativesJson?.alternative ?: []

        def creatorsJson = attrs["metadata.creators"]
            ? slurper.parseText(attrs["metadata.creators"])
            : [:]
        def creator = creatorsJson?.creator ?: []

        def contributorsJson = attrs["metadata.contributors"]
            ? slurper.parseText(attrs["metadata.contributors"])
            : [:]
        def contributor = contributorsJson?.contributor ?: []

        def publishersJson = attrs["metadata.publishers"]
            ? slurper.parseText(attrs["metadata.publishers"])
            : [:]
        def publisher = publishersJson?.publisher ?: []

        def countriesJson = attrs["metadata.countries"]
            ? slurper.parseText(attrs["metadata.countries"])
            : [:]
        def spatial = countriesJson?.spatial ?: []

        def datesJson = attrs["metadata.dates"]
            ? slurper.parseText(attrs["metadata.dates"])
            : [:]
        def date = datesJson?.date ?: []

        def languagesJson = attrs["metadata.languages"]
            ? slurper.parseText(attrs["metadata.languages"])
            : [:]
        def language = languagesJson?.language ?: []

        def descriptionsJson = attrs["metadata.descriptions"]
            ? slurper.parseText(attrs["metadata.descriptions"])
            : [:]
        def description = descriptionsJson?.description ?: []

        def provenanceJson = attrs["metadata.provenance"]
            ? slurper.parseText(attrs["metadata.provenance"])
            : [:]
        def provenance = provenanceJson?.provenance ?: []

        // ----------------------
        // Build result
        // ----------------------
        def result = [
            objectId: attrs["objectId"] ?: "",
            objectId: pkg,
            priority: 70,
            metadata: [
                type       : "Film",
                identifier : identifier,
                title      : title,
                alternative: alternative,
                creator    : creator,
                contributor: contributor,
                publisher  : publisher,
                spatial    : spatial,
                date       : date,
                relation   : relation,
                language   : language,
                provenance : provenance,
                description: description
            ]
        ]

        // ----------------------
        // JSON without unicode escaping
        // ----------------------
        def jsonGen = new JsonGenerator.Options()
            .disableUnicodeEscaping()
            .build()

        def rawJson = jsonGen.toJson(result)
        def pretty = prettyPrintJson(rawJson)

        // ----------------------
        // Write output
        // ----------------------
        outputStream.write(
            pretty.getBytes(StandardCharsets.UTF_8)
        )

    } as StreamCallback)

    session.transfer(flowFile, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, "submission.payload.status", "FAIL")
    ff = session.putAttribute(ff, "submission.payload.error", e.toString())
    session.transfer(ff, REL_FAILURE)
}