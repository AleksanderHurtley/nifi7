import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.format.ResolverStyle

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = 'initialize.flowfile'

try {
    String packageName = ff.getAttribute('package.name')?.trim()
    String packagePath = ff.getAttribute('package.path')?.trim()
    if (!packageName || !packagePath) {
        throw new IllegalArgumentException('Missing package.name or package.path attributes')
    }

    // Produksjon packages end in the date, e.g. digifilm_1300107_20130729.
    def matcher = packageName =~ /digifilm_\d+_(\d{8})/
    if (!matcher.matches()) {
        throw new IllegalArgumentException("Expected package.name in the form digifilm_<id>_<YYYYMMDD>: ${packageName}")
    }
    String packageDate = matcher.group(1)
    LocalDate.parse(packageDate, DateTimeFormatter.ofPattern('uuuuMMdd').withResolverStyle(ResolverStyle.STRICT))

    // Validate path strings here; leave source existence and media checks to fetch.
    Path sourceRoot = Paths.get('/global/film/00/film/produksjon')
    Path sourceDir = Paths.get(packagePath)
    if (!sourceDir.isAbsolute()) {
        throw new IllegalArgumentException("package.path must be absolute: ${packagePath}")
    }
    sourceDir = sourceDir.normalize()
    if (!sourceDir.startsWith(sourceRoot) || sourceRoot.relativize(sourceDir).nameCount != 2 ||
        sourceDir.fileName.toString() != packageName) {
        throw new IllegalArgumentException("package.path must identify ${packageName} under ${sourceRoot}/<year-month>/")
    }

    // Optional incoming attributes override these flow-specific defaults.
    def localRoot = { String attribute, String defaultPath ->
        Path root = Paths.get(ff.getAttribute(attribute)?.trim() ?: defaultPath)
        if (!root.isAbsolute()) {
            throw new IllegalArgumentException("${attribute} must be absolute: ${root}")
        }
        root = root.normalize()
        if (root.startsWith(sourceRoot) || sourceRoot.startsWith(root)) {
            throw new IllegalArgumentException("${attribute} must be a local output root outside the source tree: ${root}")
        }
        root
    }

    Path workBase = localRoot('work.base.dir', '/fc1/film_produksjon/work')
    Path payloadBase = localRoot('payloads.base.dir', '/fc1/film_produksjon/payloads')
    Path transferBase = localRoot('transfer.base.dir', '/fc1/film_produksjon/transfer')
    Path workDir = workBase.resolve(packageName)
    Path payloadDir = payloadBase.resolve(packageName)
    Path repDir = workDir.resolve("representations/primary_${packageDate}")
    Path repDataDir = repDir.resolve('data')
    Path descriptiveDir = workDir.resolve('metadata/descriptive')
    String videoFilename = "${packageName}.mov"

    ff = session.putAllAttributes(ff, [
        'flow.id': 'film_produksjon',
        'package.name': packageName,
        'package.path': sourceDir.toString(),
        'source.dir': sourceDir.toString(),
        'source.video.dir': sourceDir.resolve('lossless').toString(),
        'source.video.path': sourceDir.resolve('lossless').resolve(videoFilename).toString(),
        'source.metadata.dir': sourceDir.resolve('meta').toString(),
        'source.logs.dir': sourceDir.resolve('logs').toString(),
        'source.mets.path': sourceDir.resolve("${packageName}.xml").toString(),
        'work.base.dir': workBase.toString(),
        'work.dir': workDir.toString(),
        'payloads.base.dir': payloadBase.toString(),
        'payloads.dir': payloadDir.toString(),
        'events.payload.path': payloadDir.resolve('events.ndjson').toString(),
        'submission.payload.path': payloadDir.resolve('submission.json').toString(),
        'transfer.base.dir': transferBase.toString(),
        'transfer.dir': transferBase.resolve(packageName).toString(),
        'rep.dir': repDir.toString(),
        'rep.data.dir': repDataDir.toString(),
        'video.filename': videoFilename,
        'video.output.path': repDataDir.resolve(videoFilename).toString(),
        'metadata.descriptive.dir': descriptiveDir.toString(),
        'metadata.descriptive.depr_mets.dir': descriptiveDir.resolve('deprecated_mets').toString(),
        'metadata.preservation.dir': repDir.resolve('metadata/preservation').toString(),
        'metadata.technical.dir': repDir.resolve('metadata/technical').toString(),
        'metadata.other.unclassified.dir': workDir.resolve('metadata/other/unclassified').toString(),
        'total.pipeline.start': ff.getAttribute('total.pipeline.start')?.trim() ?: System.currentTimeMillis().toString()
    ])

    // Existing package.uuid, prod_db_id, description, and FlowFile content are retained.
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    log.error('Error configuring produksjon package paths', e)
    ff = session.putAllAttributes(ff, [
        'error.stage': ERROR_STAGE,
        'error.message': (e.message ?: 'Error configuring package paths').take(2048),
        'error.details': e.toString().take(2048)
    ])
    session.transfer(ff, REL_FAILURE)
}
