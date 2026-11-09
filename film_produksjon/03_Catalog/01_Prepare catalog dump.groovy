import java.net.URLEncoder
import java.nio.file.*
import org.apache.nifi.processor.io.StreamCallback

def ff = session.get()
if (!ff) return

def previousProposal = [id: ff.getAttribute('catalog.dump.proposed.primary.priref'),
    pkg: ff.getAttribute('catalog.dump.package'), urn: ff.getAttribute('catalog.dump.urn')]
// Configuration uses catalog.urn / catalog.expected.primary.priref /
// catalog.duplicates.mode. Only transient state is reset here.
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
    String expected = ff.getAttribute('catalog.expected.primary.priref')?.trim() ?:
        (previousProposal.pkg == pkg && previousProposal.urn == urn ? previousProposal.id : null)
    if (expected && !(expected ==~ /[0-9]+/)) throw new IllegalArgumentException('Expected primary priref must be numeric')
    String mode = ff.getAttribute('catalog.duplicates.mode') ?: 'stop'
    if (!(mode in ['stop', 'film_produksjon'])) throw new IllegalArgumentException('Unknown catalog.duplicates.mode')
    String requiredType = ff.getAttribute('catalog.required.primary.type')?.trim() ?:
        (mode == 'film_produksjon' ? 'ITEM_PART' : '')
    if (requiredType && !(requiredType in ['ITEM_PART', 'ITEM', 'MANIFESTATION', 'WORK'])) {
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
        'catalog.dump.duplicates.mode': mode,
        'catalog.dump.required.type': requiredType,
        'catalog.dump.expected.priref': expected ?: ''
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
