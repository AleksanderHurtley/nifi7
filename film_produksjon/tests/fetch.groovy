import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.security.MessageDigest

// Run from the repository root with Groovy 4 and groovy-xml.
class FetchTestFlowFile {
    Map<String, String> attributes
    String getAttribute(String name) { attributes[name] }
}
class FetchTestSession {
    FetchTestFlowFile flowFile
    String relationship
    FetchTestFlowFile get() { flowFile }
    FetchTestFlowFile putAttribute(FetchTestFlowFile ff, String name, String value) {
        ff.attributes[name] = value
        ff
    }
    FetchTestFlowFile putAllAttributes(FetchTestFlowFile ff, Map values) {
        assert values.every { key, value -> key instanceof String && value instanceof String }
        ff.attributes.putAll(values)
        ff
    }
    void transfer(FetchTestFlowFile ff, Object relationship) { this.relationship = relationship.toString() }
}

def loader = new GroovyClassLoader()
def scripts = [
    '01_Initialize/01_Initialize Flowfile.groovy',
    '01_Initialize/02_Initialize Directories.groovy',
    '02_Fetch files/01_Fetch and Organize metadata.groovy',
    '02_Fetch files/02_Copy and Verify MOV.groovy'
].collect { loader.parseClass(new File('film_produksjon', it)) }
def runStage = { int stage, Map attributes ->
    def ff = new FetchTestFlowFile(attributes: new LinkedHashMap(attributes))
    def session = new FetchTestSession(flowFile: ff)
    def script = scripts[stage].getDeclaredConstructor().newInstance()
    script.binding = new Binding([
        session: session, REL_SUCCESS: 'success', REL_FAILURE: 'failure',
        log: [error: { Object message, Exception error -> }, warn: { Object message -> }]
    ])
    script.run()
    [attributes: ff.attributes, relationship: session.relationship]
}

String pkg = 'digifilm_796646_20230321'
String originalMd5 = '872e9deca085f6f5ad419d6de0dcc615'
String originalSha1 = '77d78bb89d7dfa65ce68b968b60c3c556a6e1423'
String originalSize = '220812884653'
Path example = Path.of('film_produksjon/examples/sam-fs', pkg)
Path scratch = Files.createTempDirectory('produksjon-fetch-test-')
byte[] media = new byte[1024 * 1024 + 123]
new Random(42).nextBytes(media)
def hexDigest = { String algorithm, byte[] bytes ->
    MessageDigest.getInstance(algorithm).digest(bytes).collect { String.format('%02x', it) }.join()
}
String mediaMd5 = hexDigest('MD5', media)
int fixtureIndex = 0

def fixture = { boolean synthetic ->
    Path base = scratch.resolve('fixture-' + fixtureIndex++)
    Path source = Files.createDirectories(base.resolve('source').resolve(pkg))
    Files.createDirectories(source.resolve('meta'))
    Files.createDirectories(source.resolve('logs'))
    Files.createDirectories(source.resolve('lossless'))
    Files.walk(example).withCloseable { files ->
        files.filter { Files.isRegularFile(it) }.forEach { Path file ->
            Path target = source.resolve(example.relativize(file))
            Files.createDirectories(target.parent)
            if (synthetic) {
                // Replace ASCII digest/size tokens while retaining all other fixture bytes.
                String text = Files.readString(file, StandardCharsets.ISO_8859_1)
                    .replace(originalMd5, mediaMd5)
                    .replace(originalSha1, hexDigest('SHA-1', media))
                    .replace(originalSize, media.length.toString())
                Files.writeString(target, text, StandardCharsets.ISO_8859_1)
            } else {
                Files.copy(file, target)
            }
        }
    }
    if (synthetic) Files.write(source.resolve("lossless/${pkg}.mov"), media)
    def input = [
        'package.name': pkg,
        'package.path': "/global/film/00/film/produksjon/202303/${pkg}".toString(),
        'package.uuid': 'test-package-uuid', 'prod_db_id': '49062190',
        'work.base.dir': base.resolve('work').toString(),
        'payloads.base.dir': base.resolve('payloads').toString(),
        'transfer.base.dir': base.resolve('transfer').toString()
    ]
    def initialized = runStage(0, input)
    assert initialized.relationship == 'success': initialized.attributes
    def ready = runStage(1, initialized.attributes)
    assert ready.relationship == 'success': ready.attributes
    // Point the test at a local fixture rather than the real SAM-FS source.
    def attrs = ready.attributes + [
        'source.dir': source.toString(),
        'source.video.path': source.resolve("lossless/${pkg}.mov").toString()
    ]
    [source: source, attributes: attrs]
}
def changeText = { Path file, String from, String to ->
    Files.writeString(file, Files.readString(file, StandardCharsets.ISO_8859_1).replace(from, to), StandardCharsets.ISO_8859_1)
}
def assertNoScratchMov = { Map attrs ->
    Files.list(Path.of(attrs['work.dir'])).withCloseable { files ->
        assert files.noneMatch { it.fileName.toString().startsWith('.fetch-video-') }
    }
}

try {
    // Check the real captured XML and its encoding, without needing its 206 GiB MOV.
    def captured = fixture(false)
    def parsed = runStage(2, captured.attributes)
    assert parsed.relationship == 'success': parsed.attributes
    assert parsed.attributes['media.expected.md5'] == originalMd5
    assert parsed.attributes['media.expected.size'] == originalSize
    assert parsed.attributes['media.urn'] == "URN:NBN:no-nb_${pkg}"
    assert parsed.attributes['metadata.org.count'] == '7'
    assert parsed.attributes['metadata.org.review.required'] == 'false'
    Path stagedCarrier = Path.of(parsed.attributes['metadata.descriptive.legacy_mavis.dir'], 'MAVIS_mezzaninCarrierUpdate.xml')
    assert Arrays.equals(Files.readAllBytes(example.resolve('meta/MAVIS_mezzaninCarrierUpdate.xml')), Files.readAllBytes(stagedCarrier))
    assert runStage(3, parsed.attributes).relationship == 'failure' // Omitted media is not a successful transfer.

    def full = fixture(true)
    Files.writeString(full.source.resolve('jhove.done'), 'historical marker with content\n')
    Files.writeString(full.source.resolve('logs/ffmpeg.log'), 'historical conversion log\n')
    Files.createDirectory(full.source.resolve('logs/nested'))
    Files.writeString(full.source.resolve('logs/nested/extra.log'), 'nested log\n')
    def metadata = runStage(2, full.attributes)
    assert metadata.relationship == 'success': metadata.attributes
    assert metadata.attributes['metadata.org.marker.count'] == '1'
    assert metadata.attributes['media.expected.md5'] == mediaMd5
    assert metadata.attributes['metadata.org.review.required'] == 'false'
    assert Files.readString(Path.of(metadata.attributes['metadata.other.dir'], 'workflow_markers/jhove.done')) == 'historical marker with content\n'
    assert Files.readString(Path.of(metadata.attributes['metadata.other.dir'], 'logs/nested/extra.log')) == 'nested log\n'
    def fetched = runStage(3, metadata.attributes)
    assert fetched.relationship == 'success': fetched.attributes
    assert fetched.attributes['fetch.status'] == 'OK'
    assert fetched.attributes['checksum.status'] == 'OK'
    assert fetched.attributes['fetch.video.reused'] == 'false'
    assert fetched.attributes['media.verified.md5'] == mediaMd5
    assert fetched.attributes['prod_db_id'] == '49062190'
    Path target = Path.of(fetched.attributes['video.output.path'])
    assert Arrays.equals(Files.readAllBytes(target), media)
    assert Arrays.equals(Files.readAllBytes(full.source.resolve("lossless/${pkg}.mov")), media)
    assertNoScratchMov(fetched.attributes)
    assert runStage(3, metadata.attributes).attributes['fetch.video.reused'] == 'true'

    // Reuse still hashes the staged bytes and never overwrites corrupt existing media.
    byte[] corrupt = media.clone()
    corrupt[0] = (byte) (corrupt[0] ^ 1)
    Files.write(target, corrupt)
    def badExisting = runStage(3, metadata.attributes)
    assert badExisting.relationship == 'failure'
    assert badExisting.attributes['error.message'].contains('MD5 mismatch')
    assert Arrays.equals(Files.readAllBytes(target), corrupt)

    // A failed fresh copy leaves no final MOV and removes its own temporary file.
    def badCopy = fixture(true)
    def beforeCopy = runStage(2, badCopy.attributes)
    def badDigest = runStage(3, beforeCopy.attributes + ['media.expected.md5': '0' * 32])
    assert badDigest.relationship == 'failure'
    assert !Files.exists(Path.of(badDigest.attributes['video.output.path']))
    assertNoScratchMov(badDigest.attributes)
    def badSize = runStage(3, beforeCopy.attributes + ['media.expected.size': '1'])
    assert badSize.relationship == 'failure'
    assert !Files.exists(Path.of(badSize.attributes['video.output.path']))

    // Unknown metadata is retained for review and blocks the large-media step.
    def unknown = fixture(true)
    Path extra = unknown.source.resolve('meta/unexpected.xml')
    Files.writeString(extra, '<unexpected/>')
    def review = runStage(2, unknown.attributes)
    assert review.relationship == 'success': review.attributes
    assert review.attributes['metadata.org.review.required'] == 'true'
    Path reviewCopy = Path.of(review.attributes['metadata.other.unclassified.dir'], 'meta/unexpected.xml')
    assert Files.readString(reviewCopy) == '<unexpected/>'
    assert runStage(3, review.attributes).relationship == 'failure'
    assert !Files.exists(Path.of(review.attributes['video.output.path']))
    Files.delete(extra)
    assert runStage(2, unknown.attributes).attributes['metadata.org.review.required'] == 'true'
    Files.delete(reviewCopy)
    Files.writeString(unknown.source.resolve('lossless/extra.mov'), 'unexpected media')
    def extraMedia = runStage(2, unknown.attributes)
    assert extraMedia.attributes['metadata.org.review.required'] == 'true'
    assert extraMedia.attributes['metadata.org.review.paths'].contains('lossless/extra.mov')
    assert !Files.exists(Path.of(extraMedia.attributes['metadata.other.unclassified.dir'], 'lossless/extra.mov'))

    // Conflicting source reports and broken METS bindings fail before MOV copying.
    ['mets', 'jhove', 'carrier', 'missing_carrier', 'binding', 'missing_md5'].each { kind ->
        def invalid = fixture(true)
        Path mets = invalid.source.resolve("${pkg}.xml")
        switch (kind) {
            case 'mets': changeText(mets, 'CHECKSUM="' + mediaMd5 + '"', 'CHECKSUM="' + ('1' * 32) + '"'); break
            case 'jhove': changeText(invalid.source.resolve("meta/JHOVE_${pkg}.mov.xml"), mediaMd5, '1' * 32); break
            case 'carrier': changeText(invalid.source.resolve('meta/MAVIS_mezzaninCarrierUpdate.xml'), mediaMd5, '1' * 32); break
            case 'missing_carrier': Files.writeString(invalid.source.resolve('meta/MAVIS_mezzaninCarrierUpdate.xml'), '<mavis/>'); break
            case 'binding': changeText(mets, 'ADMID="OBJ_001 MEZZANIN_001"', 'ADMID="MEZZANIN_001"'); break
            case 'missing_md5': changeText(mets, '<premis:messageDigestAlgorithm>MD5</premis:messageDigestAlgorithm>', '<premis:messageDigestAlgorithm>unknown</premis:messageDigestAlgorithm>'); break
        }
        def result = runStage(2, invalid.attributes)
        assert result.relationship == 'failure': [kind, result.attributes]
        assert result.attributes['metadata.org.status'] == 'FAIL'
        assert !Files.exists(Path.of(result.attributes['video.output.path']))
    }
    println 'PASS: captured METS/JHOVE/MAVIS parsing, byte-preserved metadata, logs/markers, review gating, MOV copy/reuse, corruption detection, and temporary-file cleanup.'
} finally {
    Files.walk(scratch).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }
    loader.close()
}
