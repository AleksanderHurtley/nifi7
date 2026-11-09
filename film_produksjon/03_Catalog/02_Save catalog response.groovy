import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import groovy.xml.XmlSlurper
import java.net.URLEncoder
import java.nio.file.*
import org.apache.nifi.processor.io.StreamCallback

def ff = session.get()
if (!ff) return

def json = new JsonGenerator.Options().disableUnicodeEscaping().build()
def parse = { String value -> new JsonSlurper().parseText(value) }
def writeBytes = { Path path, byte[] bytes ->
    Path tmp = Files.createTempFile(path.parent, '.catalog-', '.tmp')
    try {
        Files.write(tmp, bytes)
        try {
            Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING)
        }
    } finally { Files.deleteIfExists(tmp) }
}
def idOf = { record -> record.@priref.text()?.trim() ?: record.priref.text()?.trim() }
def typeOf = { record -> record.record_type.value.find { it.@lang.text() == 'neutral' }?.text()?.trim() }
def parentsOf = { record ->
    Set<String> ids = new LinkedHashSet<>()
    // Direct references only, never descendant Part_of entries in expanded records.
    record.Part_of.each { link ->
        String direct = link.'part_of_reference.lref'.text()?.trim()
        String embedded = link.part_of_reference.priref.text()?.trim()
        if (direct && embedded && direct != embedded) throw new IOException('Conflicting direct parent references')
        String id = direct ?: embedded
        if (id) {
            if (!(id ==~ /[0-9]+/)) throw new IOException('Parent priref must be numeric')
            ids.add(id)
        } else if (link.part_of_reference.text()?.trim()) {
            throw new IOException('Populated parent reference has no priref')
        }
    }
    ids.toList()
}
def sourcesOf = { record ->
    record.CopiedFrom.collect { link ->
        String id = link.'copied_from.lref'.text()?.trim()
        if (!(id ==~ /[0-9]+/)) throw new IOException('CopiedFrom must contain a numeric priref')
        id
    }.unique()
}

try {
    String dir = ff.getAttribute('catalog.dump.directory')
    String runDirStr = ff.getAttribute('catalog.dump.run.dir')
    if (ff.getAttribute('catalog.dump.status') != 'FETCH' ||
        ff.getAttribute('package.name') != ff.getAttribute('catalog.dump.package') ||
        !dir || !runDirStr || !Paths.get(dir).isAbsolute() || !Paths.get(runDirStr).isAbsolute() ||
        Paths.get(ff.getAttribute('metadata.descriptive.dir')).normalize() != Paths.get(dir)) {
        throw new IllegalArgumentException('Catalog response state does not match this package')
    }
    Path descriptive = Paths.get(dir)
    Path runDir = Paths.get(runDirStr)
    Path runsBase = Paths.get(ff.getAttribute('payloads.dir')).toAbsolutePath().normalize().resolve('catalog-runs')
    if (runDir.normalize().parent != runsBase || !Files.isDirectory(runDir)) {
        throw new IllegalArgumentException('Invalid catalog evidence directory')
    }
    String phase = ff.getAttribute('catalog.dump.phase')
    if (!(phase in ['PRIMARY', 'RELATED'])) throw new IllegalArgumentException('Unknown catalog request phase')
    String code = ff.getAttribute('invokehttp.status.code') ?: ''
    if (!(code ==~ /2[0-9][0-9]/)) throw new IOException("Expected HTTP 2xx, got ${code}")
    byte[] bytes
    session.read(ff).withCloseable { bytes = it.readAllBytes() }
    def xml = new XmlSlurper().parse(new ByteArrayInputStream(bytes))
    if (xml.name() != 'adlibXML') throw new IOException('Expected adlibXML catalog response')
    def records = xml.recordList.record
    // Do not silently accept an API response truncated by its result limit.
    def diagnostic = xml.diagnostic
    String hits = diagnostic.hits.text()?.trim()
    if (hits && (!(hits ==~ /[0-9]+/) || hits.toBigInteger() != records.size())) {
        throw new IOException('Catalog response is incomplete: diagnostic.hits differs from returned records')
    }
    def queue = parse(ff.getAttribute('catalog.dump.queue'))
    def saved = parse(ff.getAttribute('catalog.dump.records'))
    if (!(queue instanceof List) || !(saved instanceof List) || saved*.id.unique().size() != saved.size()) {
        throw new IllegalArgumentException('Invalid catalog queue')
    }
    def record
    String role
    List ancestors = []
    if (phase == 'PRIMARY') {
        if (saved || queue) throw new IllegalArgumentException('Primary request must start an empty catalog run')
        // Preserve the entire lookup response, including rejected duplicate candidates.
        writeBytes(runDir.resolve('URN_RESPONSE.xml'), bytes)
        if (!records.size()) throw new IOException('URN lookup returned no records')
        String urn = ff.getAttribute('catalog.dump.urn')
        records.each { candidate ->
            if (!candidate.PID_data_URN.any { it.text()?.trim() == urn } ||
                candidate.@deleted.text() == 'true' || !(idOf(candidate) ==~ /[0-9]+/)) {
                throw new IOException('URN response contains a deleted, unrelated or unidentified record')
            }
        }
        if (records.size() > 1) {
            def sizes = records.collect { candidate ->
                String value = candidate.'file.size'.text()?.trim()
                [priref: idOf(candidate), type: typeOf(candidate), size: value,
                 sizeValue: value ==~ /[0-9]+/ ? value.toBigInteger() : null]
            }
            def positive = sizes.findAll { it.sizeValue != null && it.sizeValue > 0 }
            String suggested = ff.getAttribute('catalog.dump.duplicates.mode') == 'film_produksjon' &&
                sizes.every { it.type == 'ITEM_PART' && it.sizeValue != null } &&
                positive.size() == 1 && sizes.every { it.sizeValue == 0 || it.priref == positive[0].priref } ?
                positive[0].priref : null
            // Compare complete top-level field groups, retaining repeated-field context
            // and attributes. Empty groups may be absent in an incomplete record. A
            // report is evidence for human review, never permission to delete.
            Closure canonical
            canonical = { node ->
                def children = node.children().collect { canonical(it) }
                [name: node.name(), attributes: node.attributes().sort(),
                 value: children ? null : node.text().trim(), children: children]
            }
            def fieldMap = { candidate ->
                candidate.children().groupBy { it.name() }.collectEntries { key, nodes ->
                    [(key): nodes.collect { canonical(it) }]
                }
            }
            def comparisons = []
            if (suggested) {
                def selected = records.find { idOf(it) == suggested }
                def reference = fieldMap(selected)
                records.findAll { idOf(it) != suggested }.each { candidate ->
                    def other = fieldMap(candidate)
                    def differences = (reference.keySet() + other.keySet()).unique().sort().findAll {
                        reference[it] != other[it]
                    }.collect { key -> [field: key, proposedPrimary: reference[key], candidate: other[key]] }
                    comparisons << [priref: idOf(candidate), differingFields: differences]
                }
            }
            def report = [urn: urn, proposedPrimaryPriref: suggested,
                candidates: sizes.collect { it.findAll { k, v -> k != 'sizeValue' } },
                comparisons: comparisons, deletionApproved: false,
                note: 'Preserve identifiers, unique metadata and incoming links before any approved catalog cleanup. Rerun with catalog.expected.primary.priref after cleanup.']
            Path reportPath = runDir.resolve('DUPLICATES.json')
            writeBytes(reportPath, json.toJson(report).getBytes('UTF-8'))
            ff = session.putAttribute(ff, 'catalog.dump.report.path', reportPath.toString())
            if (suggested) ff = session.putAttribute(ff, 'catalog.dump.proposed.primary.priref', suggested)
            throw new IOException('Multiple URN matches; catalog cleanup/review is required before continuing')
        }
        record = records[0]
        String expected = ff.getAttribute('catalog.dump.expected.priref')
        if (expected && idOf(record) != expected) throw new IOException('URN lookup did not return the expected primary priref')
        String requiredType = ff.getAttribute('catalog.dump.required.type')
        if (requiredType && typeOf(record) != requiredType) throw new IOException('Unexpected primary record type')
        if (ff.getAttribute('catalog.dump.duplicates.mode') == 'film_produksjon' &&
            record.'file.size'.text()?.trim() == '0') {
            throw new IOException('The sole production primary record has file.size=0; inspect the catalog before continuing')
        }
        role = 'PRIMARY'
        ff = session.putAllAttributes(ff, [
            'catalog.primary.priref': idOf(record), 'catalog.primary.type': typeOf(record) ?: '',
            'catalog.primary.urn': urn
        ])
    } else {
        String id = ff.getAttribute('catalog.dump.priref')
        if (!queue || queue[0].id != id || records.size() != 1 || idOf(records[0]) != id) {
            throw new IOException('Expected exactly the requested related record')
        }
        record = records[0]
        def request = queue.remove(0)
        role = request.role
        ancestors = request.ancestors ?: []
    }
    String id = idOf(record)
    String type = typeOf(record)
    if (!(type in ['ITEM_PART', 'ITEM', 'MANIFESTATION', 'WORK']) || record.@deleted.text() == 'true') {
        throw new IOException("Unsupported or deleted catalog record ${id} (${type})")
    }
    if (saved.any { it.id == id }) throw new IOException('Catalog response was already processed')
    String filename = role == 'PRIMARY' ? "PACKAGE_CATALOG_RECORD_${id}.xml" :
        (role == 'SOURCE' ? "COPIED_FROM_${id}.xml" : "${type}_${id}.xml")
    def parents = role != 'SOURCE' && type != 'WORK' ? parentsOf(record) : []
    saved << [id: id, type: type, role: role, filename: filename, parents: parents]
    Closure reaches
    reaches = { String from, String target, Set seen ->
        if (from == target) return true
        if (!seen.add(from)) return false
        def known = saved.find { it.id == from }
        (known?.parents ?: []).any { reaches(it, target, seen) }
    }
    def enqueue = { String relatedId, String relatedRole ->
        if (relatedRole == 'PARENT' && reaches(relatedId, id, new HashSet())) {
            throw new IOException('Cycle in direct catalog parent references')
        }
        if (saved.any { it.id == relatedId }) {
            return
        }
        def existing = queue.find { it.id == relatedId }
        if (!existing) queue << [id: relatedId, role: relatedRole, ancestors: relatedRole == 'PARENT' ? ancestors + id : []]
        else if (relatedRole == 'PARENT' && existing.role == 'SOURCE') {
            existing.role = 'PARENT'
            existing.ancestors = ancestors + id
        }
    }
    parents.each { enqueue(it, 'PARENT') }
    if (role == 'PRIMARY') sourcesOf(record).each { enqueue(it, 'SOURCE') }
    // Parents take precedence if a source is also in the direct parent chain.
    // Its single saved response then serves both references.
    queue = queue.sort { it.role == 'PARENT' ? 0 : 1 }
    if (saved.size() + queue.size() > 100) throw new IOException('Catalog traversal exceeds 100 distinct records')
    // Save each validated response directly in the package. Only COMPLETE may
    // proceed to submission/E-ARK; a failed run can leave partial exports here.
    writeBytes(descriptive.resolve(filename), bytes)
    ff = session.putAllAttributes(ff, [
        'catalog.dump.queue': json.toJson(queue), 'catalog.dump.records': json.toJson(saved),
        'catalog.dump.count': saved.size().toString()
    ])
    if (queue) {
        ff = session.putAllAttributes(ff, [
            'catalog.dump.phase': 'RELATED', 'catalog.dump.priref': queue[0].id,
            'catalog.dump.query': URLEncoder.encode("priref = ${queue[0].id}", 'UTF-8')
        ])
    } else {
        // Finish the snapshot only after all expected exports exist. Remove old
        // recognizable catalog exports; legacy METS and other XML are untouched.
        Set<String> filenames = saved*.filename as Set
        saved.each {
            if (!Files.isRegularFile(descriptive.resolve(it.filename))) {
                throw new IOException("Expected catalog export is missing: ${it.filename}")
            }
        }
        String pkg = ff.getAttribute('package.name')
        def obsolete = []
        Files.newDirectoryStream(descriptive, '*.xml').withCloseable { paths ->
            paths.each { path ->
                String name = path.fileName.toString()
                String shortName = name.startsWith(pkg + '_') ? name.substring(pkg.length() + 1) : name
                if (shortName ==~ /(?:PACKAGE_CATALOG_RECORD|DIGITAL_ITEM_PART|DIGITAL_ITEM|ANALOG_ITEM_PART|ANALOG_ITEM|ITEM_PART|ITEM|MANIFESTATION|WORK|COPIED_FROM)_[0-9_]+\.xml/ &&
                    !filenames.contains(name)) obsolete << path
            }
        }
        obsolete.each { Files.delete(it) }
        Path manifest = runDir.resolve('CATALOG.json')
        writeBytes(manifest, json.toJson([urn: ff.getAttribute('catalog.dump.urn'),
            primaryPriref: ff.getAttribute('catalog.primary.priref'), records: saved]).getBytes('UTF-8'))
        ff = session.putAllAttributes(ff, ['catalog.dump.status': 'COMPLETE', 'catalog.dump.manifest.path': manifest.toString()])
        ff = session.removeAttribute(ff, 'catalog.dump.priref')
        ff = session.removeAttribute(ff, 'catalog.dump.query')
    }
    ff = session.write(ff, { input, output -> } as StreamCallback)
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    ff = session.putAllAttributes(ff, [
        'catalog.dump.status': 'FAIL', 'error.stage': 'catalog.dump.save',
        'error.message': (e.message ?: e.toString()).take(2048)
    ])
    log.error('Saving catalog response failed', e)
    session.transfer(ff, REL_FAILURE)
}
