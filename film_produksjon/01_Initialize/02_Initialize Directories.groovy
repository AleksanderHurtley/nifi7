import java.nio.file.FileAlreadyExistsException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = 'initialize.directories'

List<String> directoryAttributes = [
    'work.dir',
    'rep.dir',
    'rep.data.dir',
    'metadata.descriptive.dir',
    'metadata.descriptive.depr_mets.dir',
    'metadata.preservation.dir',
    'metadata.other.dir',
    'payloads.dir'
]
List<String> payloadAttributes = ['events.payload.path', 'submission.payload.path']

try {
    Map<String, String> values = [:]
    (['package.name'] + directoryAttributes + payloadAttributes).each { String name ->
        values[name] = ff.getAttribute(name)?.trim()
    }
    def missing = values.findAll { name, value -> !value }.keySet()
    if (!missing.isEmpty()) {
        throw new IllegalArgumentException("Missing attributes: ${missing.join(', ')}")
    }

    // Check the complete local path set before creating anything.
    (directoryAttributes + payloadAttributes).each { String name ->
        if (!Paths.get(values[name]).isAbsolute()) {
            throw new IllegalArgumentException("${name} must be absolute: ${values[name]}")
        }
    }
    Path payloadDir = Paths.get(values['payloads.dir']).normalize()
    payloadAttributes.each { String name ->
        if (Paths.get(values[name]).normalize().parent != payloadDir) {
            throw new IllegalArgumentException("${name} must be directly under payloads.dir")
        }
    }

    directoryAttributes.each { String name ->
        Files.createDirectories(Paths.get(values[name]))
    }

    // Create empty payloads on the first run. Retain existing payloads on retries.
    payloadAttributes.each { String name ->
        Path path = Paths.get(values[name])
        try {
            Files.createFile(path)
        } catch (FileAlreadyExistsException existing) {
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("Expected a regular payload file: ${path}")
            }
        }
    }

    // Leave transfer.dir for E-ARK and unclassified directories for unknown files.
    ff = session.putAttribute(ff, 'setup.status', 'OK')
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    log.error('Error initializing produksjon directories', e)
    ff = session.putAllAttributes(ff, [
        'setup.status': 'FAIL',
        'setup.error': e.toString().take(2048),
        'error.stage': ERROR_STAGE,
        'error.message': (e.message ?: 'Directory initialization failed').take(2048),
        'error.details': e.toString().take(2048)
    ])
    session.transfer(ff, REL_FAILURE)
}
