import org.apache.nifi.processor.io.StreamCallback
import groovy.json.JsonSlurper
import groovy.json.JsonGenerator
import groovy.xml.XmlSlurper
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

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
        // Copy history stays at ITEM_PART level: one source per CopiedFrom carrier.
        def sourcesById = new LinkedHashMap()
        def sourceFormatsById = new LinkedHashMap()
        def addSource = { String sourceId, rawTitle ->
            sourceId = sourceId?.trim()
            if (!sourceId) return
            if (!(sourceId ==~ /[0-9]+/)) {
                throw new IllegalArgumentException("CopiedFrom id must be a Collections priref: ${sourceId}")
            }
            def sourceItem = sourcesById[sourceId]
            if (sourceItem == null) {
                sourceItem = [
                    identifier: [[type: 'Collections priref', value: sourceId]],
                    URI: "https://collections.nb.no/collections/link/xplus/audiovisualcatalogue/${sourceId}".toString()
                ]
                sourcesById[sourceId] = sourceItem
            }
            def sourceTitle = rawTitle?.toString()?.trim()
            if (sourceTitle && !sourceItem.description) sourceItem.description = sourceTitle
        }

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

                    if (item.type.toString().trim().equalsIgnoreCase('CopiedFrom')) {
                        addSource(item.id.toString(), item.title)
                    }
                }
            }
        }

        // Read the staged catalog exports once, preferring work over a retained SIP.
        def catalogDirs = []
        if (attrs['metadata.descriptive.dir']?.trim()) {
            catalogDirs << Paths.get(attrs['metadata.descriptive.dir'].trim())
        }
        if (attrs['transfer.dir']?.trim()) {
            catalogDirs << Paths.get(attrs['transfer.dir'].trim()).resolve('metadata/descriptive')
        }
        def recordsById = new LinkedHashMap()
        catalogDirs.unique().each { dir ->
            if (!Files.isDirectory(dir)) return
            def xmlFiles = []
            Files.newDirectoryStream(dir, '*.xml').withCloseable { entries ->
                entries.each { path -> if (Files.isRegularFile(path)) xmlFiles << path }
            }
            xmlFiles.sort { it.fileName.toString() }.each { path ->
                new XmlSlurper().parse(path.toFile()).recordList.record.each { record ->
                    def recordId = record.@priref.text()?.trim() ?: record.priref.text()?.trim()
                    if (recordId && !recordsById.containsKey(recordId)) recordsById[recordId] = record
                }
            }
        }
        def preferredTitle = { nodes ->
            def titles = nodes.findAll { it.title.text()?.trim() }
            def selected = titles.find { it.'title.main'.text()?.trim() == 'x' } ?:
                titles.find { it.'title.type'.text()?.trim()?.equalsIgnoreCase('Originaltittel') } ?:
                (titles ? titles[0] : null)
            selected?.title?.text()?.trim()
        }
        def recordTitle = { record ->
            preferredTitle(record.Title) ?:
                record.Part_of.'part_of.title'.find { it.text()?.trim() }?.text()?.trim() ?:
                preferredTitle(record.Part_of.part_of_reference.Title)
        }

        // Prefer direct CopiedFrom links on the current ITEM_PART export when
        // available. Never follow its parent ITEM or infer IDs by numeric offsets.
        def resourceUrns = (attrs['metadata.pidDataUrns'] ?: '').split(',')
            .collect { it.trim() }.findAll { it } as Set
        if (!resourceUrns && pkg) resourceUrns = ["URN:NBN:no-nb_${pkg}".toString()] as Set
        def currentParts = recordsById.values().findAll { record ->
            def recordType = record.record_type.value.find { it.@lang.text() == 'neutral' }?.text()?.trim()
            recordType == 'ITEM_PART' && record.PID_data_URN.any { resourceUrns.contains(it.text()?.trim()) }
        }
        if (currentParts) {
            sourcesById.clear()
            currentParts.each { record ->
                record.CopiedFrom.each { node ->
                    addSource(node.'copied_from.lref'.text(), node.'copied_from.title'.text())
                }
            }
        }

        // Resolve source details via an exact record priref or Parts child reference.
        def missingSourceDetails = new LinkedHashSet(sourcesById.keySet())
        if (missingSourceDetails) {
            def useSourceDetails = { String id, String sourceTitle, String fileFormat ->
                if (!missingSourceDetails.contains(id)) return
                if (sourceTitle && !sourcesById[id].description) {
                    sourcesById[id].description = sourceTitle
                }
                if (fileFormat && !sourceFormatsById[id]) {
                    sourceFormatsById[id] = fileFormat
                }
                if (sourcesById[id].description && sourceFormatsById[id]) {
                    missingSourceDetails.remove(id)
                }
            }
            recordsById.each { recordId, record ->
                if (missingSourceDetails.contains(recordId)) {
                    useSourceDetails(recordId, recordTitle(record), record.'file.format'.text()?.trim())
                }
                // A matching Parts block may inherit the containing film/component title.
                // Its format always comes from that block, never from its parent record.
                record.'**'.findAll { it.name() == 'Parts' }.each { part ->
                    def childId = part.'parts.reference.lref'.text()?.trim() ?:
                        part.parts_reference.priref.text()?.trim()
                    def childTitle = part.'parts.title'.text()?.trim() ?:
                        part.parts_reference.Title.find { it.title.text()?.trim() }?.title?.text()?.trim() ?:
                        recordTitle(record)
                    useSourceDetails(childId, childTitle, part.'parts.fileformat'.text()?.trim())
                }
            }
        }

        sourcesById.each { id, sourceItem ->
            def fileFormat = sourceFormatsById[id]
            if (fileFormat) {
                sourceItem.description = sourceItem.description ?
                    "${sourceItem.description} — ${fileFormat}".toString() : fileFormat
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
            objectId: attrs["objectId"] ?: pkg,
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

        // Source is optional. Do not use this package's URN or the aggregated
        // metadata.barcode as source identifiers without binding them to a source.
        if (sourcesById) {
            result.metadata.source = sourcesById.values().toList()
        }

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
    flowFile = session.putAttribute(flowFile, "submission.payload.status", "FAIL")
    flowFile = session.putAttribute(flowFile, "submission.payload.error", e.toString())
    session.transfer(flowFile, REL_FAILURE)
}
