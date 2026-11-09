import groovy.json.JsonGenerator
import groovy.json.JsonOutput
import groovy.xml.XmlSlurper
import org.apache.nifi.processor.io.StreamCallback
import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.util.regex.Pattern

def ff = session.get()
if (!ff) return

// Remove the former extractors' outputs, including on failure. Paths, identity,
// fixity results and unrelated stages are retained. No metadata JSON attributes
// are read or recreated: every payload is built from this catalog snapshot.
[
    'metadata.mainTitle', 'metadata.alternativeTitles', 'metadata.creators',
    'metadata.contributors', 'metadata.publishers', 'metadata.workRelations',
    'metadata.countries', 'metadata.dates', 'metadata.descriptions',
    'metadata.languages', 'metadata.barcode', 'metadata.pidDataUrns',
    'metadata.digitalItemPartRelations', 'metadata.provenance',
    'submission.payload.status', 'submission.payload.error'
].each { ff = session.removeAttribute(ff, it) }
if (ff.getAttribute('error.stage') == 'submission.body') {
    ['error.stage', 'error.message', 'error.details'].each { ff = session.removeAttribute(ff, it) }
}

try {
    def required = { String key ->
        String value = ff.getAttribute(key)?.trim()
        if (!value) throw new IllegalArgumentException("Missing ${key}")
        value
    }
    String pkg = required('package.name')
    String primaryId = required('catalog.primary.priref')
    if (!(primaryId ==~ /[0-9]+/)) throw new IllegalArgumentException('Primary priref must be numeric')
    if (required('catalog.dump.status') != 'COMPLETE') throw new IOException('Catalog dump must be COMPLETE')
    Path workMetadata = Paths.get(required('metadata.descriptive.dir'))
    Path payloads = Paths.get(required('payloads.dir'))
    if (!workMetadata.isAbsolute() || !payloads.isAbsolute()) {
        throw new IllegalArgumentException('Metadata and payload directories must be absolute')
    }
    // One snapshot only: after work cleanup, the retained SIP can supply all
    // exports. Never fill missing work records from an older retained SIP.
    Path directory = workMetadata
    if (!Files.isDirectory(directory)) {
        String transfer = ff.getAttribute('transfer.dir')?.trim()
        if (!transfer || !Paths.get(transfer).isAbsolute()) throw new IOException('Catalog directory is missing')
        directory = Paths.get(transfer).resolve('metadata/descriptive')
    }
    if (!Files.isDirectory(directory)) throw new IOException('Catalog directory is missing')

    String urn = "URN:NBN:no-nb_${pkg}"
    ['catalog.primary.urn', 'catalog.urn', 'media.urn'].each { key ->
        String value = ff.getAttribute(key)?.trim()
        if (value && value != urn) throw new IOException("${key} does not match package.name")
    }
    String objectId = ff.getAttribute('objectId')?.trim()
    if (objectId && objectId != pkg) throw new IOException('objectId does not match package.name')

    def idOf = { record -> record.@priref.text()?.trim() ?: record.priref.text()?.trim() }
    def typeOf = { record -> record.record_type.value.find { it.@lang.text() == 'neutral' }?.text()?.trim() }
    def records = new LinkedHashMap()
    def analogBarcodes = []
    def filenames = []
    String kinds = 'PACKAGE_CATALOG_RECORD|ITEM_PART|ITEM|DIGITAL_ITEM_PART|DIGITAL_ITEM|ANALOG_ITEM_PART|ANALOG_ITEM|MANIFESTATION|WORK|COPIED_FROM'
    def exportName = Pattern.compile('(?:' + Pattern.quote(pkg) + '_)?(?:' + kinds + ')_[0-9_]+\\.xml')
    Files.newDirectoryStream(directory, '*.xml').withCloseable { paths ->
        paths.each { path ->
            if (Files.isRegularFile(path) && exportName.matcher(path.fileName.toString()).matches()) filenames << path
        }
    }
    filenames.sort { it.fileName.toString() }.each { path ->
        def xml = new XmlSlurper().parse(path.toFile())
        if (xml.name() != 'adlibXML') throw new IOException("Expected catalog XML: ${path.fileName}")
        xml.recordList.record.each { record ->
            String id = idOf(record)
            if (!(id ==~ /[0-9]+/) || record.@deleted.text() == 'true') {
                throw new IOException("Invalid/deleted exported record: ${path.fileName}")
            }
            if (records.containsKey(id)) throw new IOException("Multiple exports for catalog priref ${id}")
            records[id] = record
        }
        // Preserve the former optional analog-export barcode mapping. Do not
        // use source-carrier barcodes as this package's identifiers.
        if (path.fileName.toString() ==~ /(?:.*_)?ANALOG_ITEM_PART_[0-9_]+\.xml/) {
            String barcode = xml.recordList.record[0]?.bar_code?.text()?.trim()
            if (barcode) analogBarcodes << barcode
        }
    }
    def primary = records[primaryId]
    if (primary == null || typeOf(primary) != 'ITEM_PART' || !primary.PID_data_URN.any { it.text()?.trim() == urn }) {
        throw new IOException('Selected primary ITEM_PART is absent or does not match the package URN')
    }

    // Only the selected primary's actual parent line supplies WORK/ITEM data.
    // Other same-URN candidates and unrelated WORK exports are not merged.
    def hierarchy = []
    Set<String> visited = new LinkedHashSet<>()
    Closure visit
    visit = { String id, Set<String> ancestors ->
        if (ancestors.contains(id)) throw new IOException('Cycle in catalog parent references')
        if (!visited.add(id)) return
        def record = records[id]
        if (record == null) return // Missing WORK/optional parent metadata is allowed.
        hierarchy << record
        if (typeOf(record) == 'WORK') return
        record.Part_of.each { link ->
            String parentId = link.'part_of_reference.lref'.text()?.trim() ?: link.part_of_reference.priref.text()?.trim()
            if (parentId) {
                if (!(parentId ==~ /[0-9]+/)) throw new IOException('Parent priref must be numeric')
                visit(parentId, (ancestors + id) as Set)
            }
        }
    }
    visit(primaryId, [] as Set)
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
    def metadata = [type: 'Film', identifier: [], title: null, alternative: [],
        creator: [], contributor: [], publisher: [], spatial: [], date: [],
        relation: [], language: [], provenance: [], description: []]
    primary.PID_data_URN.each { node ->
        String value = node.text()?.trim()
        if (value) metadata.identifier << [type: 'URN', value: value, lang: 'eng']
    }
    analogBarcodes.unique().each { metadata.identifier << [type: 'HYLLESIGNATUR', value: it, lang: 'nob'] }
    hierarchy.findAll { typeOf(it) == 'WORK' }.each { record ->
        record.Title.each { node ->
            String value = node.title.text()?.trim()
            String type = node.'title.type'.text()?.trim()
            if (!value) return
            if (type?.equalsIgnoreCase('Originaltittel')) {
                if (!metadata.title) metadata.title = [value: value]
            } else metadata.alternative << [type: type ?: 'Ukjent', value: value]
        }
        record.'**'.each { node ->
            String name = node.name().toString().toLowerCase()
            if (name.endsWith('credits')) {
                String value = node.'credit.name'.text()?.trim()
                String role = node.'credit.type'.text()?.trim()
                if (!value) return
                if (role?.equalsIgnoreCase('Regissør')) metadata.creator << [name: value, type: 'Person', role: role]
                if (role?.equalsIgnoreCase('Produksjonsselskap')) metadata.contributor << [name: value, type: 'korporasjon', role: role]
                if (role?.equalsIgnoreCase('Produksjon/produsent')) metadata.contributor << [name: value, type: 'Person', role: role]
                if (['Distribusjon', 'Kringkaster', 'Utgiver'].any { it.equalsIgnoreCase(role) }) {
                    metadata.publisher << [name: value, type: 'korporasjon']
                }
            }
            if (name.endsWith('dating')) {
                String value = node.'dating.date.start'.text()?.trim()
                if (value) metadata.date << [type: node.'dating.type'.text()?.trim() ?: 'Ukjent', value: value]
            }
            if (name.endsWith('segment_of')) {
                String id = node.'segment_of.lref'.text()?.trim()
                if (id) metadata.relation << [title: node.'segment_of.title'.text()?.trim(), id: id, type: 'SegmentOf']
            }
            if (name.endsWith('av_series')) {
                String id = node.'av_series.lref'.text()?.trim()
                if (id) ['av_series.title', 'av_series.episode_no'].each { key ->
                    String value = node[key].text()?.trim()
                    if (value) metadata.relation << [title: value, id: id, type: 'IsPartOfSeries']
                }
            }
            if (name.endsWith('related_object')) {
                String id = node.'related_object.reference.lref'.text()?.trim()
                if (id) metadata.relation << [title: node.'related_object.title'.text()?.trim(), id: id, type: 'RelatedTo']
            }
            if (node.name() == 'Description') {
                String value = node.description.text()?.trim()
                if (value) metadata.description << [value: value,
                    lang: node.'description.type'.text()?.trim()?.equalsIgnoreCase('Engelsk til publisering') ? 'eng' : 'nob']
            }
        }
        record.production_country.each { node ->
            String value = node.text()?.trim()
            if (value) metadata.spatial << [name: value, type: 'Country']
        }
    }
    if (!metadata.title && metadata.alternative) metadata.title = [value: metadata.alternative[0].value]
    if (!metadata.title) {
        String title = hierarchy.collect { recordTitle(it) }.find { it }
        if (title) metadata.title = [value: title]
    }
    if (!metadata.title) throw new IOException('No title found on WORK, primary record or its parent line')
    metadata.alternative = metadata.alternative.findAll { it.value != metadata.title.value }
    hierarchy.findAll { typeOf(it) == 'ITEM' }.each { record ->
        record.'**'.findAll { it.name().toString().toLowerCase().endsWith('language') }.each { node ->
            String value = node.language.text()?.trim()
            if (value) metadata.language << [type: node.'language.usage'.text()?.trim() ?: 'Ukjent',
                value: value.equalsIgnoreCase('Norsk') ? 'nob' : value]
        }
    }

    def sources = new LinkedHashMap()
    def sourceFormats = [:]
    ['CopiedFrom': 'copied_from', 'CopiedTo': 'copied_to'].each { type, field ->
        primary[type].each { link ->
            String id = link["${field}.lref"].text()?.trim()
            if (!(id ==~ /[0-9]+/)) throw new IOException("${type} must contain a numeric priref")
            String title = link["${field}.title"].text()?.trim()
            metadata.relation << [title: title, id: id, type: type]
            if (type == 'CopiedFrom') {
                if (!sources.containsKey(id)) sources[id] = [identifier: [[type: 'Collections priref', value: id]],
                    URI: "https://collections.nb.no/collections/link/xplus/audiovisualcatalogue/${id}".toString()]
                if (title && !sources[id].description) sources[id].description = title
            }
        }
    }
    def useSourceDetails = { String id, String title, String format ->
        if (!sources.containsKey(id)) return
        if (title && !sources[id].description) sources[id].description = title
        if (format && !sourceFormats[id]) sourceFormats[id] = format
    }
    sources.keySet().each { id ->
        def record = records[id]
        if (record != null) useSourceDetails(id, recordTitle(record), record.'file.format'.text()?.trim())
    }
    records.values().each { record ->
        // An explicitly linked carrier Parts block may supply details when a
        // standalone source export is sparse/missing. Never guess adjacent IDs.
        record.'**'.findAll { it.name() == 'Parts' }.each { part ->
            String childId = part.'parts.reference.lref'.text()?.trim() ?: part.parts_reference.priref.text()?.trim()
            String title = part.'parts.title'.text()?.trim() ?:
                part.parts_reference.Title.find { it.title.text()?.trim() }?.title?.text()?.trim() ?: recordTitle(record)
            useSourceDetails(childId, title, part.'parts.fileformat'.text()?.trim())
        }
    }
    metadata.relation.each { link ->
        if (link.type == 'CopiedFrom' && !link.title && sources[link.id]?.description) link.title = sources[link.id].description
    }
    sources.each { id, source ->
        if (sourceFormats[id]) source.description = source.description ?
            "${source.description} — ${sourceFormats[id]}".toString() : sourceFormats[id]
    }
    if (sources) metadata.source = sources.values().toList()
    metadata.provenance = [[
        value: 'Digitale sikringsfiler og mezzaninfiler produsert ved NB for bevaring og formidling. Filtypene dette gjelder er digitale sikringsfiler fra film- og lydskannere (DPX og Wav) og etterarbeidede masterfiler (mezzanin/MOV). Dette er bilde- og lydfiler som er kopiert fra analogt filmmateriale.',
        lang: 'nob'
    ]]
    metadata.each { key, value -> if (value instanceof List) metadata[key] = value.unique() }
    def generator = new JsonGenerator.Options().disableUnicodeEscaping().build()
    String body = JsonOutput.prettyPrint(generator.toJson([objectId: pkg, priority: 70, metadata: metadata]), true)
    ff = session.write(ff, { input, output -> output.write(body.getBytes(StandardCharsets.UTF_8)) } as StreamCallback)
    ff = session.putAllAttributes(ff, ['filename': 'submission.json', 'mime.type': 'application/json',
        'submission.payload.path': payloads.resolve('submission.json').normalize().toString(),
        'submission.payload.status': 'OK'])
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    String message = (e.message ?: e.toString()).take(2048)
    ff = session.putAllAttributes(ff, ['submission.payload.status': 'FAIL', 'submission.payload.error': message,
        'error.stage': 'submission.body', 'error.message': message])
    log.error('Building produksjon submission body failed', e)
    session.transfer(ff, REL_FAILURE)
}
