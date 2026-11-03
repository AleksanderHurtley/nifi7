import groovy.json.JsonOutput
import groovy.xml.XmlSlurper
import java.nio.file.*
import org.apache.nifi.processor.io.StreamCallback

def ff = session.get()
if (!ff) return

// This stage owns only its catalog.copied_from.* state, including on a retry.
ff.getAttributes().keySet().findAll { it.startsWith('catalog.copied_from.') }.each {
    ff = session.removeAttribute(ff, it)
}
if (ff.getAttribute('error.stage')?.startsWith('catalog.copied_from.')) {
    ['error.stage', 'error.message', 'error.details'].each { ff = session.removeAttribute(ff, it) }
}

try {
    String pkg = ff.getAttribute('package.name')?.trim()
    String dir = ff.getAttribute('metadata.descriptive.dir')?.trim()
    if (!(pkg ==~ /digifilm_\d+_\d{8}/) || !dir) {
        throw new IllegalArgumentException('Expected package.name and metadata.descriptive.dir')
    }
    Path descriptive = Paths.get(dir).toAbsolutePath().normalize()
    if (!Paths.get(dir).isAbsolute() || !Files.isDirectory(descriptive)) {
        throw new IOException('Expected an existing absolute metadata.descriptive.dir with current catalog exports')
    }
    def exports = []
    Files.newDirectoryStream(descriptive, "${pkg}_DIGITAL_ITEM_PART_*.xml").withCloseable { entries ->
        entries.each { if (Files.isRegularFile(it)) exports << it }
    }
    if (!exports) throw new IOException('No current DIGITAL_ITEM_PART exports; run after the existing catalog dump has completed')

    String urn = ff.getAttribute('media.urn')?.trim() ?: "URN:NBN:no-nb_${pkg}".toString()
    Set<String> ids = new LinkedHashSet<>()
    int matched = 0
    exports.sort { it.fileName.toString() }.each { path ->
        new XmlSlurper().parse(path.toFile()).recordList.record.each { record ->
            String type = record.record_type.value.find { it.@lang.text() == 'neutral' }?.text()?.trim()
            if (type != 'ITEM_PART' || !record.PID_data_URN.any { it.text()?.trim() == urn }) return
            if (record.@deleted.text() == 'true') throw new IOException('Current item-part record is marked deleted')
            matched++
            // Direct copy history only; do not follow a parent's or source's history.
            record.CopiedFrom.each { link ->
                String id = link.'copied_from.lref'.text()?.trim()
                if (!(id ==~ /[0-9]+/)) throw new IOException('CopiedFrom is missing a numeric source priref')
                ids.add(id)
            }
        }
    }
    if (matched == 0) throw new IOException("No current ITEM_PART matches the package URN: ${urn}")

    List<String> queue = ids.toList()
    ff = session.putAllAttributes(ff, [
        'catalog.copied_from.package': pkg,
        'catalog.copied_from.directory': descriptive.toString(),
        'catalog.copied_from.ids': JsonOutput.toJson(queue),
        'catalog.copied_from.count': queue.size().toString(),
        'catalog.copied_from.saved.count': '0',
        'catalog.copied_from.index': '0',
        'catalog.copied_from.status': queue ? 'FETCH' : 'COMPLETE'
    ])
    if (queue) ff = session.putAttribute(ff, 'catalog.copied_from.priref', queue[0])
    // The existing catalog barrier emits an empty package FlowFile. Keep that
    // contract; request/response content is not used by the submission extractors.
    ff = session.write(ff, { input, output -> } as StreamCallback)
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    ff = session.putAllAttributes(ff, [
        'catalog.copied_from.status': 'FAIL',
        'catalog.copied_from.error': (e.message ?: e.toString()).take(2048),
        'error.stage': 'catalog.copied_from.prepare',
        'error.message': (e.message ?: e.toString()).take(2048)
    ])
    log.error('Preparing copied-from catalog exports failed', e)
    session.transfer(ff, REL_FAILURE)
}
