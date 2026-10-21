import groovy.xml.XmlSlurper
import java.nio.file.*

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = 'fetch.metadata.organize'

try {
    String pkg = ff.getAttribute('package.name')?.trim()
    if (!(pkg ==~ /digifilm_\d+_\d{8}/)) {
        throw new IllegalArgumentException('Missing or invalid package.name')
    }
    String videoName = "${pkg}.mov"
    def pathAttribute = { String name ->
        String value = ff.getAttribute(name)?.trim()
        if (!value) throw new IllegalArgumentException("Missing attribute: ${name}")
        Path path = Paths.get(value)
        if (!path.isAbsolute()) throw new IllegalArgumentException("${name} must be absolute")
        path.normalize()
    }

    Path source = pathAttribute('source.dir')
    Path work = pathAttribute('work.dir')
    if (source.startsWith(work) || work.startsWith(source)) {
        throw new IllegalArgumentException('Source and work directories must be separate')
    }
    def outputDirectory = { String name ->
        Path path = pathAttribute(name)
        if (!path.startsWith(work) || path == work) {
            throw new IllegalArgumentException("${name} must be under work.dir")
        }
        path
    }
    Path deprecated = outputDirectory('metadata.descriptive.depr_mets.dir')
    Path legacyMavis = outputDirectory('metadata.descriptive.legacy_mavis.dir')
    Path preservation = outputDirectory('metadata.preservation.dir')
    Path other = outputDirectory('metadata.other.dir')
    Path unclassified = outputDirectory('metadata.other.unclassified.dir')
    if (!Files.isDirectory(source, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("Source directory not found: ${source}")
    }
    if (!Files.isDirectory(source.resolve('meta'), LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("Source metadata directory not found: ${source.resolve('meta')}")
    }
    ff = session.putAttribute(ff, 'fetch.start', ff.getAttribute('fetch.start')?.trim() ?: System.currentTimeMillis().toString())

    int copied = 0
    int markers = 0
    Set<String> review = new TreeSet<>()
    def copyMetadata = { Path src, Path dst ->
        if (!Files.isRegularFile(src, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Expected a regular metadata file: ${src}")
        }
        if (Files.isSymbolicLink(dst)) throw new IOException("Refusing a symbolic-link destination: ${dst}")
        Files.createDirectories(dst.parent)
        Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES)
        copied++
    }
    def reviewEntry = { Path path, boolean copyFile ->
        Path relative = source.relativize(path)
        review.add(relative.toString())
        if (copyFile && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            copyMetadata(path, unclassified.resolve(relative))
        }
    }
    def entries = { Path directory ->
        List<Path> paths = []
        Files.newDirectoryStream(directory).withCloseable { stream -> stream.each { paths.add(it) } }
        paths.sort { it.fileName.toString() }
    }

    Path stagedMets = deprecated.resolve("${pkg}.xml")
    copyMetadata(source.resolve("${pkg}.xml"), stagedMets)

    // Inspect only the package root and the known directories. Do not unpack archives.
    entries(source).each { Path path ->
        String name = path.fileName.toString()
        if (name == "${pkg}.xml" || name == 'meta') return
        if (name == 'lossless' || name == 'logs') {
            if (!Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) reviewEntry(path, false)
            return
        }
        if (name.toLowerCase(Locale.ROOT).endsWith('.done') && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            copyMetadata(path, other.resolve('workflow_markers').resolve(name))
            markers++
        } else {
            reviewEntry(path, true)
        }
    }

    Set<String> mavisNames = [
        'mavisMezzaninCarrier.xml', 'mavisMezzaninComponent.xml',
        'mavisMezzaninTitle.xml', 'MAVIS_mezzaninCarrierUpdate.xml'
    ] as Set
    Path stagedJhove = null
    Path stagedCarrierUpdate = null
    entries(source.resolve('meta')).each { Path path ->
        String name = path.fileName.toString()
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            reviewEntry(path, false)
        } else if (mavisNames.contains(name)) {
            Path dst = legacyMavis.resolve(name)
            copyMetadata(path, dst)
            if (name == 'MAVIS_mezzaninCarrierUpdate.xml') stagedCarrierUpdate = dst
        } else if (name == 'ffprobe.xml') {
            copyMetadata(path, other.resolve(name))
        } else if (name == "JHOVE_${videoName}.xml") {
            stagedJhove = preservation.resolve(name)
            copyMetadata(path, stagedJhove)
        } else {
            reviewEntry(path, true)
        }
    }

    Path logs = source.resolve('logs')
    if (Files.isDirectory(logs, LinkOption.NOFOLLOW_LINKS)) {
        Files.walk(logs).withCloseable { paths ->
            paths.forEach { Path path ->
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) return
                if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    copyMetadata(path, other.resolve('logs').resolve(logs.relativize(path)))
                } else {
                    reviewEntry(path, false)
                }
            }
        }
    }
    Path mediaDir = source.resolve('lossless')
    if (!Files.isDirectory(mediaDir, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException("Source media directory not found: ${mediaDir}")
    }
    entries(mediaDir).each { Path path ->
        if (path.fileName.toString() != videoName || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            reviewEntry(path, false) // Do not recall/copy unexpected large media for review.
        }
    }

    // Bind the one media entry to its PREMIS object through ADMID, not document order.
    def mets = new XmlSlurper(false, true).parse(stagedMets.toFile())
    if (mets.name() != 'mets') throw new IOException('Source package XML is not METS')
    def files = mets.fileSec.depthFirst().findAll { it.name() == 'file' }
    if (files.size() != 1) throw new IOException("Expected one METS media file, found ${files.size()}")
    def media = files[0]
    List<String> adminIds = media.@ADMID.text().tokenize()
    def objects = mets.amdSec.techMD.findAll { adminIds.contains(it.@ID.text()) }.mdWrap.xmlData.object
    if (objects.size() != 1) throw new IOException('METS media ADMID must identify exactly one PREMIS object')
    def object = objects[0]
    if (object.originalName.text().trim() != videoName) {
        throw new IOException("PREMIS originalName does not match ${videoName}")
    }
    def md5Nodes = object.objectCharacteristics.fixity.findAll {
        it.messageDigestAlgorithm.text().trim().equalsIgnoreCase('MD5')
    }
    if (md5Nodes.size() != 1) throw new IOException('Expected exactly one PREMIS MD5 fixity value')
    String expectedMd5 = md5Nodes[0].messageDigest.text().trim().toLowerCase(Locale.ROOT)
    String expectedSize = object.objectCharacteristics.size.text().trim()
    if (!(expectedMd5 ==~ /[a-f0-9]{32}/) || !(expectedSize ==~ /\d+/) || expectedSize.toLong() <= 0) {
        throw new IOException('Missing or invalid PREMIS media MD5/size')
    }
    expectedSize = expectedSize.toLong().toString()
    String expectedUrn = "URN:NBN:no-nb_${pkg}"
    def locations = media.FLocat.findAll {
        it.attributes()['{http://www.w3.org/1999/xlink}href'] == expectedUrn
    }
    if (locations.size() != 1) throw new IOException("METS FLocat must identify ${expectedUrn}")

    // Cross-check the available historical evidence; the MOV is verified in step 2.
    def checkSize = { String value, String origin ->
        if (value && (!(value ==~ /\d+/) || value.toLong() != expectedSize.toLong())) {
            throw new IOException("${origin} size conflicts with PREMIS: ${value} != ${expectedSize}")
        }
    }
    def checkMd5 = { String value, String origin ->
        if (value && value.toLowerCase(Locale.ROOT) != expectedMd5) {
            throw new IOException("${origin} MD5 conflicts with PREMIS")
        }
    }
    checkSize(media.@SIZE.text().trim(), 'METS file')
    if (media.@CHECKSUMTYPE.text().equalsIgnoreCase('MD5')) checkMd5(media.@CHECKSUM.text().trim(), 'METS file')
    if (stagedJhove != null) {
        def report = new XmlSlurper(false, true).parse(stagedJhove.toFile())
        if (report.repInfo.size() != 1 || report.repInfo.@uri.text().tokenize('/').last() != videoName) {
            throw new IOException('JHOVE report does not identify the expected media file')
        }
        checkSize(report.repInfo.size.text().trim(), 'JHOVE')
        report.repInfo.checksums.checksum.findAll { it.@type.text().equalsIgnoreCase('MD5') }.each {
            checkMd5(it.text().trim(), 'JHOVE')
        }
    }
    if (stagedCarrierUpdate != null) {
        def carrier = new XmlSlurper(false, true).parse(stagedCarrierUpdate.toFile()).DigitalCarrier
        if (carrier.size() != 1) throw new IOException('MAVIS carrier update must contain exactly one DigitalCarrier')
        checkSize(carrier.fileSize.text().trim(), 'MAVIS carrier update')
        carrier.objectIdentifiers.ObjectIdentifier.findAll {
            it.identifierType.any { identifier ->
                identifier.attributes()['{http://www.w3.org/1999/xlink}href']?.endsWith('/MD5')
            }
        }.each { checkMd5(it.identifier.text().trim(), 'MAVIS carrier update') }
    }

    // Old review copies must not silently enter a later SIP after a rerun.
    if (Files.isDirectory(unclassified)) {
        Files.walk(unclassified).withCloseable { paths ->
            paths.filter { !Files.isDirectory(it, LinkOption.NOFOLLOW_LINKS) }.forEach {
                review.add(unclassified.relativize(it).toString())
            }
        }
    }
    ff = session.putAllAttributes(ff, [
        'metadata.org.status': 'OK',
        'metadata.org.count': copied.toString(),
        'metadata.org.marker.count': markers.toString(),
        'metadata.org.review.required': (!review.isEmpty()).toString(),
        'metadata.org.review.paths': review.join('; ').take(2048),
        'metadata.org.unclassified.count': review.size().toString(),
        'media.expected.filename': videoName,
        'media.expected.size': expectedSize,
        'media.expected.md5': expectedMd5,
        'media.urn': expectedUrn,
        'media.mets.path': stagedMets.toString()
    ])
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    log.error('Produksjon metadata fetch failed', e)
    ff = session.putAllAttributes(ff, [
        'metadata.org.status': 'FAIL',
        'error.stage': ERROR_STAGE,
        'error.message': (e.message ?: 'Metadata fetch failed').take(2048),
        'error.details': e.toString().take(2048)
    ])
    session.transfer(ff, REL_FAILURE)
}
