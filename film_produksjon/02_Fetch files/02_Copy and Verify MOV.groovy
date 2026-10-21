import java.nio.file.*
import java.security.MessageDigest
import java.time.Instant
import java.time.temporal.ChronoUnit

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = 'fetch.video'
Path temporary = null
boolean checksumStarted = false

try {
    def required = { String name ->
        String value = ff.getAttribute(name)?.trim()
        if (!value) throw new IllegalArgumentException("Missing attribute: ${name}")
        value
    }
    if (required('metadata.org.status') != 'OK' || required('metadata.org.review.required') != 'false') {
        throw new IOException('Metadata must be organized and unclassified source files reviewed before copying media')
    }
    String fetchStartText = required('fetch.start')
    if (!fetchStartText.isLong() || fetchStartText.toLong() > System.currentTimeMillis()) {
        throw new IllegalArgumentException('Invalid fetch.start from metadata fetch')
    }
    String pkg = required('package.name')
    String filename = required('media.expected.filename')
    String expectedMd5 = required('media.expected.md5').toLowerCase(Locale.ROOT)
    String sizeText = required('media.expected.size')
    if (filename != "${pkg}.mov" || !(expectedMd5 ==~ /[a-f0-9]{32}/) || !(sizeText ==~ /\d+/) || sizeText.toLong() <= 0) {
        throw new IllegalArgumentException('Invalid expected media filename, size, or MD5')
    }
    long expectedSize = sizeText.toLong()
    def pathAttribute = { String name ->
        Path path = Paths.get(required(name))
        if (!path.isAbsolute()) throw new IllegalArgumentException("${name} must be absolute")
        path.normalize()
    }
    Path sourceDir = pathAttribute('source.dir')
    Path source = pathAttribute('source.video.path')
    Path work = pathAttribute('work.dir')
    Path dataDir = pathAttribute('rep.data.dir')
    Path target = pathAttribute('video.output.path')
    if (work.startsWith(sourceDir) || sourceDir.startsWith(work) || !dataDir.startsWith(work) ||
        dataDir == work || target != dataDir.resolve(filename) || source != sourceDir.resolve('lossless').resolve(filename)) {
        throw new IllegalArgumentException('Source and staged media paths do not match the initialized layout')
    }
    if (!Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("Source MOV not found: ${source}")
    }
    if (Files.size(source) != expectedSize) {
        throw new IOException("Source MOV size mismatch: expected ${expectedSize}, found ${Files.size(source)}")
    }
    if (!Files.isDirectory(dataDir) || !Files.isDirectory(work)) {
        throw new IOException('Run directory initialization before fetching the MOV')
    }

    boolean reused = Files.exists(target, LinkOption.NOFOLLOW_LINKS)
    Path toVerify = target
    if (reused) {
        if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Expected a regular staged MOV: ${target}")
        }
    } else {
        // Scratch is outside metadata/ and representations/ and belongs to this invocation.
        temporary = Files.createTempFile(work, '.fetch-video-', '.tmp')
        Files.copy(source, temporary, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)
        toVerify = temporary
    }

    long checksumStart = System.currentTimeMillis()
    checksumStarted = true
    ff = session.putAttribute(ff, 'checksum.start', checksumStart.toString())
    if (Files.size(toVerify) != expectedSize) {
        throw new IOException("Staged MOV size mismatch: expected ${expectedSize}, found ${Files.size(toVerify)}")
    }
    MessageDigest digest = MessageDigest.getInstance('MD5')
    Files.newInputStream(toVerify).withCloseable { input ->
        byte[] buffer = new byte[1024 * 1024]
        int read
        while ((read = input.read(buffer)) != -1) digest.update(buffer, 0, read)
    }
    String actualMd5 = digest.digest().collect { String.format('%02x', it) }.join()
    if (actualMd5 != expectedMd5) {
        throw new IOException("Staged MOV MD5 mismatch: expected ${expectedMd5}, found ${actualMd5}")
    }
    if (Files.size(toVerify) != expectedSize) throw new IOException('Staged MOV size changed during verification')
    if (!reused) {
        // Do not overwrite an independently staged file if another run finished meanwhile.
        Files.move(temporary, target)
        temporary = null
    }

    long end = System.currentTimeMillis()
    String completed = Instant.ofEpochMilli(end).truncatedTo(ChronoUnit.SECONDS).toString()
    ff = session.putAllAttributes(ff, [
        'fetch.status': 'OK',
        'fetch.video.reused': reused.toString(),
        'fetch.end': end.toString(),
        'fetch.duration': (end - fetchStartText.toLong()).toString(),
        'checksum.status': 'OK',
        'checksum.end': end.toString(),
        'checksum.durationMs': (end - checksumStart).toString(),
        'checksum.expected.count': '1',
        'checksum.checked.count': '1',
        'checksum.source': 'legacy_mets',
        'media.verified.md5': actualMd5,
        'media.verified.size': expectedSize.toString(),
        'transfer.event.datetime': completed
    ])
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    log.error('Produksjon MOV fetch failed', e)
    ff = session.putAllAttributes(ff, [
        'fetch.status': 'FAIL',
        'checksum.status': checksumStarted ? 'FAIL' : 'NOT_CHECKED',
        'error.stage': ERROR_STAGE,
        'error.message': (e.message ?: 'MOV fetch failed').take(2048),
        'error.details': e.toString().take(2048)
    ])
    session.transfer(ff, REL_FAILURE)
} finally {
    if (temporary != null) {
        try {
            Files.deleteIfExists(temporary)
        } catch (Exception cleanupError) {
            log.warn("Could not remove this invocation's temporary MOV ${temporary}: ${cleanupError.message}".toString())
        }
    }
}
