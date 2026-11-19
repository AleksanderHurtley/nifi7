import java.net.URLEncoder
import java.nio.file.*
import org.apache.nifi.processor.io.StreamCallback

def ff = session.get()
if (!ff) return

// Keep the input configuration and reset the results from any previous run.
ff.getAttributes().keySet().findAll {
    it.startsWith('catalog.dump.') || it.startsWith('catalog.primary.') ||
        it.startsWith('catalog.copied_from.')
}.each { ff = session.removeAttribute(ff, it) }
if (ff.getAttribute('error.stage')?.startsWith('catalog.')) {
    ['error.stage', 'error.message', 'error.details'].each { ff = session.removeAttribute(ff, it) }
}

try {
    String pkg = ff.getAttribute('package.name')?.trim()
    String dir = ff.getAttribute('metadata.descriptive.dir')?.trim()
    String payloadDir = ff.getAttribute('payloads.dir')?.trim()
    String urn = ff.getAttribute('catalog.urn')?.trim() ?: ff.getAttribute('media.urn')?.trim() ?:
        ff.getAttribute('urn')?.trim()
    if (!pkg || pkg in ['.', '..'] || pkg.contains('/') || pkg.contains('\\') || !urn ||
        !dir || !payloadDir || !Paths.get(dir).isAbsolute() || !Paths.get(payloadDir).isAbsolute()) {
        throw new IllegalArgumentException('Expected package.name, catalog.urn (or media.urn/urn), absolute metadata.descriptive.dir and payloads.dir')
    }
    if (urn.contains("'")) throw new IllegalArgumentException('URN must not contain a query quote')
    String requiredType = ff.getAttribute('catalog.required.primary.type')?.trim() ?: 'ITEM_PART'
    if (!(requiredType in ['ITEM_PART', 'ITEM', 'MANIFESTATION', 'WORK'])) {
        throw new IllegalArgumentException('Unknown catalog.required.primary.type')
    }
    Path descriptive = Paths.get(dir).normalize()
    Path payloads = Paths.get(payloadDir).normalize()
    if (payloads.startsWith(descriptive) || descriptive.startsWith(payloads)) {
        throw new IllegalArgumentException('Catalog request evidence must be outside descriptive metadata')
    }
    Files.createDirectories(descriptive)
    Path runDir = Files.createDirectories(payloads.resolve('catalog-runs').resolve(UUID.randomUUID().toString()))
    ff = session.putAllAttributes(ff, [
        'catalog.dump.package': pkg,
        'catalog.dump.directory': descriptive.toString(),
        'catalog.dump.run.dir': runDir.toString(),
        'catalog.dump.urn': urn,
        'catalog.dump.phase': 'PRIMARY',
        'catalog.dump.status': 'FETCH',
        'catalog.dump.query': URLEncoder.encode("PID_data_URN = '${urn}'", 'UTF-8'),
        'catalog.dump.queue': '[]',
        'catalog.dump.records': '[]',
        'catalog.dump.count': '0',
        'catalog.dump.required.type': requiredType
    ])
    ff = session.write(ff, { input, output -> } as StreamCallback)
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    ff = session.putAllAttributes(ff, [
        'catalog.dump.status': 'FAIL',
        'error.stage': 'catalog.dump.prepare',
        'error.message': (e.message ?: e.toString()).take(2048)
    ])
    log.error('Preparing catalog dump failed', e)
    session.transfer(ff, REL_FAILURE)
}
