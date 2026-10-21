import java.nio.file.Files
import java.nio.file.Path

// Run from the repository root with Groovy 4. No NiFi server or source media needed.
class TestFlowFile {
    Map<String, String> attributes
    String getAttribute(String name) { attributes[name] }
}

class TestSession {
    TestFlowFile flowFile
    String relationship
    TestFlowFile get() { flowFile }
    TestFlowFile putAttribute(TestFlowFile ff, String name, String value) {
        ff.attributes[name] = value
        ff
    }
    TestFlowFile putAllAttributes(TestFlowFile ff, Map values) {
        assert values.every { key, value -> key instanceof String && value instanceof String }
        ff.attributes.putAll(values)
        ff
    }
    void transfer(TestFlowFile ff, Object relationship) { this.relationship = relationship.toString() }
}

def loader = new GroovyClassLoader()
def scripts = [
    loader.parseClass(new File('film_produksjon/01_Initialize/01_Initialize Flowfile.groovy')),
    loader.parseClass(new File('film_produksjon/01_Initialize/02_Initialize Directories.groovy'))
]
def runStage = { int stage, Map attributes ->
    def ff = new TestFlowFile(attributes: new LinkedHashMap(attributes))
    def session = new TestSession(flowFile: ff)
    def script = scripts[stage].getDeclaredConstructor().newInstance()
    script.binding = new Binding([
        session: session,
        log: [error: { Object message, Exception error -> }],
        REL_SUCCESS: 'success', REL_FAILURE: 'failure'
    ])
    script.run()
    [attributes: ff.attributes, relationship: session.relationship]
}

String pkg = 'digifilm_1300107_20130729'
String source = "/global/film/00/film/produksjon/201307/${pkg}"
def input = [
    'package.name': pkg,
    'package.path': source,
    'package.uuid': '1191aeb6-8e27-44f2-86a3-46e6398dfbbd',
    'prod_db_id': '49062190',
    'prod_db_description': "transfer_${pkg}".toString()
]

def defaults = runStage(0, input)
assert defaults.relationship == 'success'
assert defaults.attributes['work.base.dir'] == '/fc1/film_produksjon/work'
assert defaults.attributes['payloads.base.dir'] == '/fc1/film_produksjon/payloads'
assert defaults.attributes['transfer.base.dir'] == '/fc1/film_produksjon/transfer'
assert defaults.attributes['rep.dir'] == "/fc1/film_produksjon/work/${pkg}/representations/primary_20130729"
assert defaults.attributes['source.video.path'] == "${source}/lossless/${pkg}.mov"
assert defaults.attributes['video.output.path'] == "${defaults.attributes['rep.dir']}/data/${pkg}.mov"
assert defaults.attributes['transfer.dir'] == "/fc1/film_produksjon/transfer/${pkg}"
assert defaults.attributes['metadata.descriptive.depr_mets.dir'] == "${defaults.attributes['work.dir']}/metadata/descriptive/deprecated_mets"
assert !defaults.attributes.containsKey('metadata.preservation.source_mets.dir')
assert !defaults.attributes.containsKey('metadata.descriptive.legacy_mavis.dir')
assert defaults.attributes['package.uuid'] == input['package.uuid']
assert defaults.attributes['prod_db_id'] == input['prod_db_id']
assert !defaults.attributes.keySet().any { it.startsWith('dpx.') || it.startsWith('batches.') || it.startsWith('rawcooked.') }
assert runStage(0, defaults.attributes).attributes['total.pipeline.start'] == defaults.attributes['total.pipeline.start']

def recent = runStage(0, ['package.name': 'digifilm_796646_20230321',
    'package.path': '/global/film/00/film/produksjon/202303/digifilm_796646_20230321/'])
assert recent.relationship == 'success'
assert recent.attributes['rep.dir'].endsWith('/primary_20230321')
assert !recent.attributes['package.path'].endsWith('/')

def invalidInputs = [
    ['package.name': ''],
    ['package.path': 'relative/path'],
    ['package.path': "/global/film/00/film/produksjon_other/201307/${pkg}"],
    ['package.path': '/global/film/00/film/produksjon/201307/wrong_package'],
    ['package.path': "/global/film/00/film/produksjon/201307/../../${pkg}"],
    ['package.name': 'digifilm_1300107_20130229'],
    ['package.name': '../digifilm_1300107_20130729'],
    ['work.base.dir': 'relative/work'],
    ['payloads.base.dir': '/global/film/00/film/produksjon/payloads']
]
invalidInputs.each { overrides ->
    def result = runStage(0, input + overrides)
    assert result.relationship == 'failure': overrides
    assert result.attributes['error.stage'] == 'initialize.flowfile'
}
assert runStage(0, ['package.name': 'digifilm_1_20240229',
    'package.path': '/global/film/00/film/produksjon/202402/digifilm_1_20240229']).relationship == 'success'

Path scratch = Files.createTempDirectory('produksjon-initialize-test-')
try {
    def roots = [
        'work.base.dir': scratch.resolve('work').toString(),
        'payloads.base.dir': scratch.resolve('payloads').toString(),
        'transfer.base.dir': scratch.resolve('transfer').toString()
    ]
    def initialized = runStage(0, input + roots)
    assert initialized.relationship == 'success'
    def attrs = initialized.attributes

    // Validate every required path before creating any of this test's directories.
    def incomplete = new LinkedHashMap(attrs)
    incomplete.remove('metadata.preservation.dir')
    def rejected = runStage(1, incomplete)
    assert rejected.relationship == 'failure'
    assert rejected.attributes['setup.status'] == 'FAIL'
    assert !Files.exists(scratch.resolve('work'))

    def outsidePayload = runStage(1, attrs + ['events.payload.path': scratch.resolve('outside.ndjson').toString()])
    assert outsidePayload.relationship == 'failure'
    assert !Files.exists(scratch.resolve('work'))

    def ready = runStage(1, attrs)
    assert ready.relationship == 'success'
    assert ready.attributes['setup.status'] == 'OK'
    ['rep.data.dir', 'metadata.descriptive.depr_mets.dir', 'metadata.preservation.dir', 'metadata.other.dir'].each {
        assert Files.isDirectory(Path.of(attrs[it]))
    }
    assert !Files.exists(Path.of(attrs['metadata.descriptive.dir'], 'legacy_mavis'))
    assert !Files.exists(Path.of(attrs['metadata.preservation.dir'], 'source_mets'))
    assert !Files.exists(Path.of(attrs['metadata.other.unclassified.dir']))
    assert !Files.exists(Path.of(attrs['transfer.dir']))

    Path events = Path.of(attrs['events.payload.path'])
    Path submission = Path.of(attrs['submission.payload.path'])
    assert Files.size(events) == 0
    assert Files.size(submission) == 0
    Files.writeString(events, '{"existing":"event"}\n')
    Files.writeString(submission, '{"existing":"submission"}')
    assert runStage(1, attrs).relationship == 'success'
    assert Files.readString(events) == '{"existing":"event"}\n'
    assert Files.readString(submission) == '{"existing":"submission"}'

    def collisionAttrs = new LinkedHashMap(attrs)
    collisionAttrs['events.payload.path'] = events.resolveSibling('directory-not-file').toString()
    Files.createDirectory(Path.of(collisionAttrs['events.payload.path']))
    assert runStage(1, collisionAttrs).relationship == 'failure'
    assert Files.readString(events) == '{"existing":"event"}\n'

    println 'PASS: package dates, source paths, output roots, directory setup, failure routing, and payload preservation.'
} finally {
    // Remove only the temporary fixture tree created above.
    Files.walk(scratch).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }
    loader.close()
}
