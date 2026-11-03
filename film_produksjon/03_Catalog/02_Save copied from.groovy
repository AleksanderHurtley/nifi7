import groovy.json.JsonSlurper
import groovy.xml.XmlSlurper
import java.nio.file.*
import org.apache.nifi.processor.io.StreamCallback

def ff = session.get()
if (!ff) return

Path temporary = null
try {
    String pkg = ff.getAttribute('package.name')?.trim()
    String dir = ff.getAttribute('metadata.descriptive.dir')?.trim()
    String id = ff.getAttribute('catalog.copied_from.priref')?.trim()
    if (!(pkg ==~ /digifilm_\d+_\d{8}/) || !(id ==~ /[0-9]+/) || !dir) {
        throw new IllegalArgumentException('Missing or invalid package, source priref or descriptive directory')
    }
    Path descriptive = Paths.get(dir).toAbsolutePath().normalize()
    if (!Paths.get(dir).isAbsolute() || !Files.isDirectory(descriptive) ||
        pkg != ff.getAttribute('catalog.copied_from.package') ||
        descriptive.toString() != ff.getAttribute('catalog.copied_from.directory') ||
        ff.getAttribute('catalog.copied_from.status') != 'FETCH') {
        throw new IllegalArgumentException('Copied-from request state does not match this package/directory')
    }
    def ids = new JsonSlurper().parseText(ff.getAttribute('catalog.copied_from.ids') ?: '')
    int index = Integer.parseInt(ff.getAttribute('catalog.copied_from.index') ?: '')
    int count = Integer.parseInt(ff.getAttribute('catalog.copied_from.count') ?: '')
    int saved = Integer.parseInt(ff.getAttribute('catalog.copied_from.saved.count') ?: '')
    if (!(ids instanceof List) || ids.any { !(it instanceof String) || !(it ==~ /[0-9]+/) } ||
        ids.toSet().size() != ids.size() || count != ids.size() || index < 0 || index >= count ||
        saved != index || ids[index] != id) {
        throw new IllegalArgumentException('Inconsistent copied-from request queue')
    }
    String code = ff.getAttribute('invokehttp.status.code') ?: ''
    if (!(code ==~ /2[0-9][0-9]/)) throw new IOException("Expected a successful catalog HTTP response, got ${code}")

    byte[] bytes
    session.read(ff).withCloseable { input -> bytes = input.readAllBytes() }
    def xml = new XmlSlurper().parse(new ByteArrayInputStream(bytes))
    def records = xml.recordList.record
    if (xml.name() != 'adlibXML' || records.size() != 1) {
        throw new IOException('Expected exactly one catalog record in the copied-from response')
    }
    def record = records[0]
    String responseId = record.@priref.text()?.trim() ?: record.priref.text()?.trim()
    String type = record.record_type.value.find { it.@lang.text() == 'neutral' }?.text()?.trim()
    if (responseId != id || type != 'ITEM_PART' || record.@deleted.text() == 'true') {
        throw new IOException("Expected active source ITEM_PART ${id}; got ${responseId} (${type})")
    }

    Path target = descriptive.resolve("${pkg}_COPIED_FROM_${id}.xml")
    // Validate before replacing an earlier response; keep the original XML bytes.
    temporary = Files.createTempFile(descriptive, ".${pkg}_COPIED_FROM_${id}.", '.tmp')
    Files.write(temporary, bytes)
    try {
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    } catch (AtomicMoveNotSupportedException ignored) {
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING)
    }
    temporary = null
    int next = index + 1
    ff = session.putAllAttributes(ff, [
        'catalog.copied_from.index': next.toString(),
        'catalog.copied_from.saved.count': next.toString(),
        'catalog.copied_from.last.path': target.toString(),
        'catalog.copied_from.status': next < count ? 'FETCH' : 'COMPLETE'
    ])
    if (next < count) ff = session.putAttribute(ff, 'catalog.copied_from.priref', ids[next])
    else ff = session.removeAttribute(ff, 'catalog.copied_from.priref')
    ff = session.write(ff, { input, output -> } as StreamCallback)
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    ff = session.putAllAttributes(ff, [
        'catalog.copied_from.status': 'FAIL',
        'catalog.copied_from.error': (e.message ?: e.toString()).take(2048),
        'error.stage': 'catalog.copied_from.save',
        'error.message': (e.message ?: e.toString()).take(2048)
    ])
    log.error('Saving copied-from catalog export failed', e)
    session.transfer(ff, REL_FAILURE)
} finally {
    if (temporary != null) Files.deleteIfExists(temporary)
}
