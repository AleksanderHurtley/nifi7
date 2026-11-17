import groovy.json.JsonSlurper
import java.nio.file.Files
import java.nio.file.Path

def ff = session.get()
if (!ff) return

final String candidatesAttribute = 'catalog.dump.duplicate.candidate.prirefs'
ff = session.removeAttribute(ff, candidatesAttribute)
ff = session.removeAttribute(ff, 'catalog.dump.duplicate.candidate.priref')

try {
    String urn = ff.getAttribute('catalog.dump.urn')
    String proposedId = ff.getAttribute('catalog.dump.proposed.primary.priref') ?: ''
    if (ff.getAttribute('catalog.dump.status') != 'FAIL' ||
        ff.getAttribute('catalog.dump.phase') != 'PRIMARY' ||
        ff.getAttribute('catalog.dump.duplicates.mode') != 'film_produksjon' ||
        ff.getAttribute('catalog.dump.package') != ff.getAttribute('package.name') ||
        ff.getAttribute('catalog.delete.attempted') || !urn) {
        throw new IllegalArgumentException('Expected a duplicate primary URN lookup before cleanup')
    }

    Path runDir = Path.of(ff.getAttribute('catalog.dump.run.dir')).normalize()
    Path runsBase = Path.of(ff.getAttribute('payloads.dir')).normalize().resolve('catalog-runs')
    Path reportPath = Path.of(ff.getAttribute('catalog.dump.report.path')).normalize()
    if (!runDir.isAbsolute() || runDir.parent != runsBase ||
        reportPath != runDir.resolve('DUPLICATES.json') || !Files.isRegularFile(reportPath)) {
        throw new IOException('Expected DUPLICATES.json in the current catalog run directory')
    }

    def report = Files.newBufferedReader(reportPath).withCloseable { reader -> new JsonSlurper().parse(reader) }
    if (!(report instanceof Map) || report.urn != urn || (report.proposedPrimaryPriref ?: '') != proposedId) {
        throw new IOException('Duplicate report does not match the current URN and proposed primary')
    }

    def candidates = report.candidates
    if (!(candidates instanceof List) || candidates.size() < 2 || !candidates.every {
        it instanceof Map && it.type == 'ITEM_PART' && it.priref instanceof String &&
            (it.priref ==~ /[0-9]+/) && it.size instanceof String
    } || candidates*.priref.toSet().size() != candidates.size()) {
        throw new IOException('Expected at least two distinct ITEM_PART records with numeric IDs')
    }

    def (zeroByte, retained) = candidates.split { it.size ==~ /0+/ }
    if (!zeroByte) {
        throw new IOException('No explicit zero-byte duplicates to delete')
    }
    if (!retained) {
        throw new IOException('Every matching record is zero bytes; cleanup would leave no record for the URN')
    }
    String expectedId = ff.getAttribute('catalog.dump.expected.priref') ?: proposedId
    if (expectedId && !(expectedId in retained*.priref)) {
        throw new IOException('The expected primary is not among the records that would remain')
    }
    if (retained.size() == 1) {
        expectedId = retained[0].priref
    }

    ff = session.putAllAttributes(ff, [
        (candidatesAttribute): zeroByte*.priref.join(','),
        'catalog.expected.primary.priref': expectedId
    ])
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    ff = session.putAllAttributes(ff, [
        'error.stage': 'catalog.dump.read_duplicate_report',
        'error.message': (e.message ?: e.toString()).take(2048)
    ])
    log.error('Reading catalog duplicate report failed', e)
    session.transfer(ff, REL_FAILURE)
}
