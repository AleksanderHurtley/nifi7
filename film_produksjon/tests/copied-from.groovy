import groovy.json.JsonSlurper
import java.nio.file.*

// HTTP is simulated. Successful responses use captured source XML; inline
// minimal/error responses are test data, not live catalog dumps.
class CopiedFromFlowFile {
    Map<String, String> attributes
    byte[] body = 'input'.getBytes('UTF-8')
    String getAttribute(String key) { attributes[key] }
    Map getAttributes() { attributes }
}
class CopiedFromSession {
    CopiedFromFlowFile ff
    String relationship
    CopiedFromFlowFile get() { ff }
    CopiedFromFlowFile putAttribute(CopiedFromFlowFile file, String key, String value) {
        file.attributes[key] = value
        file
    }
    CopiedFromFlowFile putAllAttributes(CopiedFromFlowFile file, Map values) {
        assert values.every { k, v -> k instanceof String && v instanceof String }
        file.attributes.putAll(values)
        file
    }
    CopiedFromFlowFile removeAttribute(CopiedFromFlowFile file, String key) {
        file.attributes.remove(key)
        file
    }
    InputStream read(CopiedFromFlowFile file) { new ByteArrayInputStream(file.body) }
    CopiedFromFlowFile write(CopiedFromFlowFile file, Object callback) {
        def output = new ByteArrayOutputStream()
        callback.process(new ByteArrayInputStream(file.body), output)
        file.body = output.toByteArray()
        file
    }
    void transfer(CopiedFromFlowFile file, Object rel) { relationship = rel.toString() }
}
def loader = new GroovyClassLoader()
loader.parseClass('''package org.apache.nifi.processor.io
interface StreamCallback {
    void process(java.io.InputStream inputStream, java.io.OutputStream outputStream) throws java.io.IOException
}''')
def prepareClass = loader.parseClass(new File('film_produksjon/03_Catalog/01_Prepare copied from.groovy'))
def saveClass = loader.parseClass(new File('film_produksjon/03_Catalog/02_Save copied from.groovy'))
def builderClass = loader.parseClass(new File('film_produksjon/07_Submission body/05_Make submission body.groovy'))
def extractors = ['01_Get Work.groovy', '02_Get DIGITAL ITEM.groovy',
    '03_Get Analog item part bar_code.groovy', '04_Get digital item part.groovy'].collect {
    loader.parseClass(new File('film_produksjon/07_Submission body', it))
}
def run = { Class type, Map attrs, byte[] body = 'input'.getBytes('UTF-8') ->
    def ff = new CopiedFromFlowFile(attributes: new LinkedHashMap(attrs), body: body)
    def session = new CopiedFromSession(ff: ff)
    def script = type.getDeclaredConstructor().newInstance()
    script.binding = new Binding(session: session, log: [error: { Object msg, Exception e -> }],
        REL_SUCCESS: 'success', REL_FAILURE: 'failure')
    script.run()
    [attrs: ff.attributes, body: ff.body, relationship: session.relationship]
}
def parse = { String value -> new JsonSlurper().parseText(value) }
def xml = { String id, String fields = '', String type = 'ITEM_PART', String deleted = 'false' ->
    """<?xml version="1.0" encoding="UTF-8"?><adlibXML><recordList>
      <record priref="${id}" deleted="${deleted}">
        <record_type><value lang="neutral">${type}</value></record_type>${fields}
      </record></recordList></adlibXML>""".getBytes('UTF-8')
}
Path scratch = Files.createTempDirectory('produksjon-copied-from-test-')
try {
    String pkg = 'digifilm_815655_20130726'
    Path descriptive = Files.createDirectories(scratch.resolve('work/metadata/descriptive'))
    Path fixture = Path.of('film_produksjon/examples/catalog_dump/' + pkg)
    Files.newDirectoryStream(fixture, '*.xml').withCloseable { paths ->
        paths.each { Files.copy(it, descriptive.resolve(it.fileName)) }
    }
    Path part = descriptive.resolve(pkg + '_DIGITAL_ITEM_PART_70398020_162311522.xml')
    String originalPart = Files.readString(part)
    def attrs = [
        'package.name': pkg, 'package.uuid': 'keep-uuid', 'prod_db_id': 'keep-db-id',
        'metadata.descriptive.dir': descriptive.toString(),
        'submission.payload.path': scratch.resolve('submission.json').toString(),
        'transfer.dir': scratch.resolve('transfer').toString(),
        'media.urn': 'URN:NBN:no-nb_' + pkg, 'checksum.status': 'OK', 'priref': 'keep-parent-id',
        'metadata.pidDataUrns': 'stale-previous-package-urn',
        'catalog.copied_from.priref': '999', 'catalog.copied_from.saved.count': '99',
        'catalog.copied_from.error': 'old error',
        'error.stage': 'catalog.copied_from.http', 'error.message': 'old HTTP error'
    ]
    def prepared = run(prepareClass, attrs)
    assert prepared.relationship == 'success': prepared.attrs
    assert parse(prepared.attrs['catalog.copied_from.ids']) == ['69119828', '69119830']
    assert prepared.attrs['catalog.copied_from.count'] == '2'
    assert prepared.attrs['catalog.copied_from.saved.count'] == '0'
    assert prepared.attrs['catalog.copied_from.status'] == 'FETCH'
    assert prepared.attrs['catalog.copied_from.priref'] == '69119828'
    assert !prepared.attrs.containsKey('catalog.copied_from.error')
    assert !prepared.attrs.containsKey('error.stage')
    assert !prepared.attrs.containsKey('error.message')
    assert prepared.body.length == 0
    ['package.uuid', 'prod_db_id', 'priref', 'checksum.status'].each { assert prepared.attrs[it] == attrs[it] }

    // Sparse source metadata remains allowed, even when a richer record exists.
    assert run(saveClass, prepared.attrs + ['invokehttp.status.code': '200'], xml('69119828')).relationship == 'success'
    byte[] firstResponse = Files.readAllBytes(fixture.resolve(pkg + '_COPIED_FROM_69119828.xml'))
    def first = run(saveClass, prepared.attrs + ['invokehttp.status.code': '200'], firstResponse)
    assert first.relationship == 'success': first.attrs
    assert first.attrs['catalog.copied_from.status'] == 'FETCH'
    assert first.attrs['catalog.copied_from.priref'] == '69119830'
    assert first.attrs['catalog.copied_from.saved.count'] == '1'
    Path firstPath = descriptive.resolve(pkg + '_COPIED_FROM_69119828.xml')
    assert Arrays.equals(Files.readAllBytes(firstPath), firstResponse)

    // Its own CopiedFrom is not followed: acquisition is one generation only.
    byte[] audioResponse = Files.readAllBytes(fixture.resolve(pkg + '_COPIED_FROM_69119830.xml'))
    def second = run(saveClass, first.attrs + ['invokehttp.status.code': '200'], audioResponse)
    assert second.relationship == 'success': second.attrs
    assert second.attrs['catalog.copied_from.status'] == 'COMPLETE'
    assert second.attrs['catalog.copied_from.saved.count'] == '2'
    assert !second.attrs.containsKey('catalog.copied_from.priref')
    assert second.body.length == 0
    Path audioPath = descriptive.resolve(pkg + '_COPIED_FROM_69119830.xml')
    assert Arrays.equals(Files.readAllBytes(audioPath), audioResponse)
    def stage = second
    extractors.each { type ->
        stage = run(type, stage.attrs, stage.body)
        assert stage.relationship == 'success': stage.attrs
    }
    def payload = run(builderClass, stage.attrs, stage.body)
    assert payload.relationship == 'success': payload.attrs
    def metadata = parse(new String(payload.body, 'UTF-8')).metadata
    assert metadata.source*.identifier.collect { it[0].value } == ['69119828', '69119830']
    assert metadata.source[0].description == 'Per på landet — Image/x-dpx'
    assert metadata.source[1].description == 'Per på landet — Audio/wav'
    assert metadata.relation*.id == ['69119828', '69119830']
    assert !metadata.source.any { it.identifier[0].value == '62325762' }
    assert metadata.title.value == 'Per på landet'
    assert metadata.spatial == [[name: 'Norge', type: 'Country']]
    assert parse(new String(payload.body, 'UTF-8')) == new JsonSlurper().parse(new File(
        'film_produksjon/examples/submission/digifilm_815655_20130726.enriched-submission.json'))
    assert Files.readString(part) == originalPart

    // Never replace a valid earlier dump with an error, wrong record or HTML.
    def failures = [
        [code: '404', body: xml('69119830')],
        [code: '500', body: xml('69119830')],
        [code: '200', body: xml('69119829', '', 'ITEM')],
        [code: '200', body: xml('69119830', '', 'ITEM')],
        [code: '200', body: xml('69119830', '', 'ITEM_PART', 'true')],
        [code: '200', body: '<adlibXML><recordList/></adlibXML>'.getBytes('UTF-8')],
        [code: '200', body: '<html><body>Login required</body></html>'.getBytes('UTF-8')],
        [code: '200', body: '<adlibXML>'.getBytes('UTF-8')]
    ]
    failures.each { test ->
        def failed = run(saveClass, first.attrs + ['invokehttp.status.code': test.code], test.body)
        assert failed.relationship == 'failure': test
        assert failed.attrs['catalog.copied_from.status'] == 'FAIL'
        assert failed.attrs['catalog.copied_from.saved.count'] == '1'
        assert Arrays.equals(Files.readAllBytes(audioPath), audioResponse)
    }
    [
        ['package.name': '../escape'], ['catalog.copied_from.priref': '../escape'],
        ['catalog.copied_from.count': '9'], ['catalog.copied_from.package': 'another-package']
    ].each { changes ->
        assert run(saveClass, first.attrs + ['invokehttp.status.code': '200'] + changes, audioResponse).relationship == 'failure'
    }
    // Re-delivery from the same request state is safe: rewrite the same filename.
    def replay = run(saveClass, first.attrs + ['invokehttp.status.code': '200'], audioResponse)
    assert replay.relationship == 'success'
    assert replay.attrs['catalog.copied_from.saved.count'] == '2'

    Files.writeString(part, originalPart.replaceAll('(?s)<CopiedFrom>.*?</CopiedFrom>', ''))
    def zero = run(prepareClass, second.attrs)
    assert zero.relationship == 'success': zero.attrs
    assert zero.attrs['catalog.copied_from.status'] == 'COMPLETE'
    assert zero.attrs['catalog.copied_from.count'] == '0'
    assert zero.attrs['catalog.copied_from.saved.count'] == '0'
    assert !zero.attrs.containsKey('catalog.copied_from.priref')
    Files.writeString(part, originalPart.replace('69119830', '69119828'))
    def one = run(prepareClass, attrs)
    assert one.relationship == 'success': one.attrs
    assert one.attrs['catalog.copied_from.count'] == '1'
    Files.writeString(part, originalPart.replace('69119828</copied_from.lref>', 'invalid</copied_from.lref>'))
    assert run(prepareClass, attrs).relationship == 'failure'
    Files.writeString(part, originalPart)
    assert run(prepareClass, attrs + ['media.urn': 'URN:NBN:no-nb_wrong-package']).relationship == 'failure'
    assert run(prepareClass, attrs + ['metadata.descriptive.dir': scratch.resolve('missing').toString()]).relationship == 'failure'
    assert run(prepareClass, attrs + ['metadata.descriptive.dir': scratch.toString()]).relationship == 'failure'
    assert run(prepareClass, attrs + ['package.name': '../escape']).relationship == 'failure'
    Files.newDirectoryStream(descriptive, '*.tmp').withCloseable { assert !it.iterator().hasNext() }
    println 'PASS: sequential 0/1/2-source retrieval, deduplication, namespace reset, exact filenames/XML preservation, response validation, failure routing, safe replay and production-local submission enrichment.'
} finally {
    Files.walk(scratch).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
    }
    loader.close()
}
