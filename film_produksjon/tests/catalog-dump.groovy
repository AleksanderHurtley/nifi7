import groovy.json.JsonSlurper
import java.nio.file.*

// HTTP is simulated. Successful responses use captured source XML; inline
// minimal/error responses are test data, not live catalog dumps.
class CatalogDumpFlowFile {
    Map<String, String> attributes
    byte[] body = 'input'.getBytes('UTF-8')
    String getAttribute(String key) { attributes[key] }
    Map getAttributes() { attributes }
}
class CatalogDumpSession {
    CatalogDumpFlowFile ff
    String relationship
    CatalogDumpFlowFile get() { ff }
    CatalogDumpFlowFile putAttribute(CatalogDumpFlowFile file, String key, String value) {
        file.attributes[key] = value
        file
    }
    CatalogDumpFlowFile putAllAttributes(CatalogDumpFlowFile file, Map values) {
        assert values.every { k, v -> k instanceof String && v instanceof String }
        file.attributes.putAll(values)
        file
    }
    CatalogDumpFlowFile removeAttribute(CatalogDumpFlowFile file, String key) {
        file.attributes.remove(key)
        file
    }
    InputStream read(CatalogDumpFlowFile file) { new ByteArrayInputStream(file.body) }
    CatalogDumpFlowFile write(CatalogDumpFlowFile file, Object callback) {
        def output = new ByteArrayOutputStream()
        callback.process(new ByteArrayInputStream(file.body), output)
        file.body = output.toByteArray()
        file
    }
    void transfer(CatalogDumpFlowFile file, Object rel) { relationship = rel.toString() }
}
def loader = new GroovyClassLoader()
loader.parseClass('''package org.apache.nifi.processor.io
interface StreamCallback {
    void process(java.io.InputStream inputStream, java.io.OutputStream outputStream) throws java.io.IOException
}''')
def prepareClass = loader.parseClass(new File('film_produksjon/03_Catalog/01_Prepare catalog dump.groovy'))
def saveClass = loader.parseClass(new File('film_produksjon/03_Catalog/02_Save catalog response.groovy'))
def builderClass = loader.parseClass(new File('film_produksjon/07_Submission body/01_Build submission body.groovy'))
def run = { Class type, Map attrs, byte[] body = new byte[0] ->
    def ff = new CatalogDumpFlowFile(attributes: new LinkedHashMap(attrs), body: body)
    def session = new CatalogDumpSession(ff: ff)
    def script = type.getDeclaredConstructor().newInstance()
    script.binding = new Binding(session: session, log: [error: { Object msg, Exception e -> }],
        REL_SUCCESS: 'success', REL_FAILURE: 'failure')
    script.run()
    [attrs: ff.attributes, body: ff.body, relationship: session.relationship]
}
def parse = { String value -> new JsonSlurper().parseText(value) }
def xml = { String id, String type, String fields = '' ->
    """<adlibXML><recordList><record priref="${id}" deleted="false">
    <record_type><value lang="neutral">${type}</value></record_type>${fields}
    </record></recordList></adlibXML>""".getBytes('UTF-8')
}
Path scratch = Files.createTempDirectory('produksjon-catalog-dump-test-')
try {
    String pkg = 'digifilm_815655_20130726'
    String urn = 'URN:NBN:no-nb_' + pkg
    Path descriptive = Files.createDirectories(scratch.resolve('work/metadata/descriptive'))
    Path payloads = Files.createDirectories(scratch.resolve('payloads'))
    Path fixture = Path.of('film_produksjon/examples/catalog_dump/' + pkg)
    Files.newDirectoryStream(fixture, '*.xml').withCloseable { paths ->
        paths.each { Files.copy(it, descriptive.resolve(it.fileName)) }
    }
    Files.writeString(descriptive.resolve('ITEM_PART_162311522.xml'), 'previous filename')
    Files.writeString(descriptive.resolve('PACKAGE_CATALOG_RECORD_999.xml'), 'previous primary')
    Path legacy = Files.createDirectories(descriptive.resolve('deprecated_mets')).resolve(pkg + '.xml')
    Files.writeString(legacy, 'Do not change original METS')
    def attrs = ['package.name': pkg, 'media.urn': urn,
        'metadata.descriptive.dir': descriptive.toString(), 'payloads.dir': payloads.toString(),
        'submission.payload.path': payloads.resolve('submission.json').toString(),
        'catalog.duplicates.mode': 'film_produksjon',
        'catalog.dump.status': 'COMPLETE', 'catalog.primary.priref': 'STALE',
        'catalog.copied_from.status': 'COMPLETE', 'checksum.status': 'OK',
        'error.stage': 'catalog.dump.save', 'error.message': 'old error']
    def prepared = run(prepareClass, attrs)
    assert prepared.relationship == 'success': prepared.attrs
    assert !prepared.attrs.containsKey('catalog.primary.priref')
    assert !prepared.attrs.containsKey('catalog.copied_from.status')
    assert !prepared.attrs.containsKey('error.message')
    assert prepared.attrs['checksum.status'] == 'OK'
    assert java.net.URLDecoder.decode(prepared.attrs['catalog.dump.query'], 'UTF-8') == "PID_data_URN = '${urn}'"
    byte[] duplicates = Files.readAllBytes(fixture.resolve(pkg + '_DIGITAL_ITEM_PART_70398020_162311522.xml'))
    def duplicate = run(saveClass, prepared.attrs + ['invokehttp.status.code': '200'], duplicates)
    assert duplicate.relationship == 'failure': duplicate.attrs
    assert duplicate.attrs['catalog.dump.proposed.primary.priref'] == '162311522'
    assert !duplicate.attrs.containsKey('catalog.primary.priref')
    Path reportFile = Path.of(duplicate.attrs['catalog.dump.report.path'])
    def report = parse(Files.readString(reportFile))
    assert report.proposedPrimaryPriref == '162311522'
    assert !report.deletionApproved
    assert report.comparisons[0].differingFields*.field.contains('Alternative_number')
    assert report.comparisons[0].differingFields*.field.contains('Part_of')
    assert Arrays.equals(Files.readAllBytes(reportFile.parent.resolve('URN_RESPONSE.xml')), duplicates)
    assert !Files.exists(descriptive.resolve('PACKAGE_CATALOG_RECORD_162311522.xml'))
    def generic = run(prepareClass, attrs + ['catalog.duplicates.mode': 'stop'])
    def genericDuplicate = run(saveClass, generic.attrs + ['invokehttp.status.code': '200'], duplicates)
    assert genericDuplicate.relationship == 'failure'
    assert !genericDuplicate.attrs.containsKey('catalog.dump.proposed.primary.priref')

    // Synthetic new response after external cleanup; no live catalog change.
    def parsed = new groovy.xml.XmlParser().parse(new ByteArrayInputStream(duplicates))
    parsed.recordList[0].remove(parsed.recordList.record.find { it.attribute('priref') == '70398020' })
    parsed.diagnostic.hits[0].setValue('1')
    parsed.diagnostic.hits_on_display[0].setValue('1')
    byte[] unique = groovy.xml.XmlUtil.serialize(parsed).getBytes('UTF-8')
    def start = run(prepareClass, duplicate.attrs)
    assert start.relationship == 'success'
    assert start.attrs['catalog.dump.run.dir'] != prepared.attrs['catalog.dump.run.dir']
    assert start.attrs['catalog.dump.expected.priref'] == '162311522'
    def stage = run(saveClass, start.attrs + ['invokehttp.status.code': '200'], unique)
    assert stage.relationship == 'success': stage.attrs
    assert stage.attrs['catalog.primary.priref'] == '162311522'
    assert stage.attrs['catalog.dump.status'] == 'FETCH'
    Path primaryPath = descriptive.resolve('PACKAGE_CATALOG_RECORD_162311522.xml')
    assert Arrays.equals(Files.readAllBytes(primaryPath), unique)
    Path runDir = Path.of(stage.attrs['catalog.dump.run.dir'])
    assert !Files.exists(runDir.resolve(primaryPath.fileName))
    assert !Files.exists(runDir.resolve('CATALOG.json'))
    // A failed related request leaves the partial work dump for inspection,
    // without completing or removing old exports. Restart from Prepare.
    def interrupted = run(saveClass, stage.attrs + ['invokehttp.status.code': '500'], new byte[0])
    assert interrupted.relationship == 'failure'
    assert interrupted.attrs['catalog.dump.status'] == 'FAIL'
    assert Arrays.equals(Files.readAllBytes(primaryPath), unique)
    assert Files.exists(descriptive.resolve('PACKAGE_CATALOG_RECORD_999.xml'))
    def restarted = run(prepareClass, interrupted.attrs)
    assert restarted.relationship == 'success'
    assert restarted.attrs['catalog.dump.count'] == '0'
    stage = run(saveClass, restarted.attrs + ['invokehttp.status.code': '200'], unique)
    assert stage.relationship == 'success': stage.attrs
    runDir = Path.of(stage.attrs['catalog.dump.run.dir'])
    def requested = []
    def fixtureNames = ['162311521': 'DIGITAL_ITEM_162311521', '25815655': 'MANIFESTATION_25815655',
        '815655': 'WORK_815655', '69119828': 'COPIED_FROM_69119828', '69119830': 'COPIED_FROM_69119830']
    while (stage.attrs['catalog.dump.status'] == 'FETCH') {
        String id = stage.attrs['catalog.dump.priref']
        requested << id
        assert requested.size() <= 5
        byte[] bytes = Files.readAllBytes(fixture.resolve(pkg + '_' + fixtureNames[id] + '.xml'))
        stage = run(saveClass, stage.attrs + ['invokehttp.status.code': '200'], bytes)
        assert stage.relationship == 'success': stage.attrs
        String filename = parse(stage.attrs['catalog.dump.records']).find { it.id == id }.filename
        assert Arrays.equals(Files.readAllBytes(descriptive.resolve(filename)), bytes)
        assert !Files.exists(runDir.resolve(filename))
    }
    assert requested == ['162311521', '25815655', '815655', '69119828', '69119830']
    assert stage.attrs['catalog.dump.count'] == '6'
    Set expected = ['PACKAGE_CATALOG_RECORD_162311522.xml', 'ITEM_162311521.xml', 'MANIFESTATION_25815655.xml',
        'WORK_815655.xml', 'COPIED_FROM_69119828.xml', 'COPIED_FROM_69119830.xml'] as Set
    def actual = []
    Files.newDirectoryStream(descriptive, '*.xml').withCloseable { it.each { actual << it.fileName.toString() } }
    assert (actual as Set) == expected
    assert Files.readString(legacy) == 'Do not change original METS'
    def manifest = parse(Files.readString(Path.of(stage.attrs['catalog.dump.manifest.path'])))
    assert manifest.primaryPriref == '162311522'
    assert manifest.records*.id.unique().size() == 6
    def evidenceFiles = []
    Files.newDirectoryStream(runDir).withCloseable { it.each { evidenceFiles << it.fileName.toString() } }
    assert evidenceFiles.toSet() == ['URN_RESPONSE.xml', 'CATALOG.json'].toSet()
    def payload = run(builderClass, stage.attrs)
    assert payload.relationship == 'success': payload.attrs
    def metadata = parse(new String(payload.body, 'UTF-8')).metadata
    assert metadata.source*.identifier.collect { it[0].value } == ['69119828', '69119830']
    assert metadata.source*.description == ['Per på landet — Image/x-dpx', 'Per på landet — Audio/wav']
    assert metadata.relation*.title == ['Per på landet', 'Per på landet']
    assert metadata.title.value == 'Per på landet'
    assert parse(new String(payload.body, 'UTF-8')) == new JsonSlurper().parse(new File(
        'film_produksjon/examples/submission/digifilm_815655_20130726.titled-relations.submission.json'))
    def wrongExpected = run(prepareClass, attrs + ['catalog.expected.primary.priref': '70398020'])
    assert run(saveClass, wrongExpected.attrs + ['invokehttp.status.code': '200'], unique).relationship == 'failure'

    def itemStart = run(prepareClass, attrs + ['package.name': 'another-package', 'catalog.duplicates.mode': 'stop'])
    def item = run(saveClass, itemStart.attrs + ['invokehttp.status.code': '200'],
        xml('10', 'ITEM', "<PID_data_URN>${urn}</PID_data_URN>"))
    assert item.relationship == 'success': item.attrs
    assert item.attrs['catalog.dump.status'] == 'COMPLETE'
    assert item.attrs['catalog.primary.type'] == 'ITEM'
    assert item.attrs['catalog.dump.count'] == '1'
    assert Files.exists(descriptive.resolve('PACKAGE_CATALOG_RECORD_10.xml'))
    assert !Files.exists(descriptive.resolve('ITEM_10.xml'))
    assert parse(item.attrs['catalog.dump.records'])[0].filename == 'PACKAGE_CATALOG_RECORD_10.xml'
    def miniStart = { -> run(prepareClass, attrs + ['catalog.expected.primary.priref': '']) }
    def feed = { state, body -> run(saveClass, state.attrs + ['invokehttp.status.code': '200'], body) }
    def primary = { extra -> xml('10', 'ITEM_PART', "<PID_data_URN>${urn}</PID_data_URN>" + extra) }
    assert feed(miniStart(), primary('<file.size>0</file.size>')).relationship == 'failure'
    def both = feed(miniStart(), primary('''<Part_of><part_of_reference.lref>20</part_of_reference.lref></Part_of>
        <CopiedFrom><copied_from.lref>20</copied_from.lref></CopiedFrom>'''))
    assert parse(both.attrs['catalog.dump.queue']).size() == 1
    both = feed(both, xml('20', 'ITEM', '<CopiedFrom><copied_from.lref>99</copied_from.lref></CopiedFrom>'))
    assert both.relationship == 'success': both.attrs
    assert both.attrs['catalog.dump.count'] == '2'
    def diamond = feed(miniStart(), primary('''<Part_of><part_of_reference.lref>20</part_of_reference.lref></Part_of>
        <Part_of><part_of_reference.lref>21</part_of_reference.lref></Part_of>'''))
    ['20','21'].each { id ->
        assert diamond.attrs['catalog.dump.priref'] == id
        diamond = feed(diamond, xml(id, 'ITEM', '<Part_of><part_of_reference.lref>30</part_of_reference.lref></Part_of>'))
    }
    assert diamond.attrs['catalog.dump.priref'] == '30'
    diamond = feed(diamond, xml('30', 'WORK'))
    assert diamond.relationship == 'success': diamond.attrs
    assert diamond.attrs['catalog.dump.count'] == '4'
    def cycle = feed(miniStart(), primary('<Part_of><part_of_reference.lref>20</part_of_reference.lref></Part_of>'))
    assert feed(cycle, xml('20', 'ITEM', '<Part_of><part_of_reference.lref>10</part_of_reference.lref></Part_of>')).relationship == 'failure'
    assert feed(cycle, xml('99', 'ITEM')).relationship == 'failure'
    def siblingCycle = feed(miniStart(), primary('''<Part_of><part_of_reference.lref>20</part_of_reference.lref></Part_of>
        <Part_of><part_of_reference.lref>21</part_of_reference.lref></Part_of>'''))
    siblingCycle = feed(siblingCycle, xml('20', 'ITEM', '<Part_of><part_of_reference.lref>21</part_of_reference.lref></Part_of>'))
    assert feed(siblingCycle, xml('21', 'ITEM', '<Part_of><part_of_reference.lref>20</part_of_reference.lref></Part_of>')).relationship == 'failure'
    assert feed(miniStart(), '<adlibXML><recordList/></adlibXML>'.getBytes('UTF-8')).relationship == 'failure'
    assert feed(miniStart(), '<html/>'.getBytes('UTF-8')).relationship == 'failure'
    assert feed(miniStart(), '<adlibXML>'.getBytes('UTF-8')).relationship == 'failure'
    assert run(saveClass, miniStart().attrs + ['invokehttp.status.code': '500'], unique).relationship == 'failure'
    byte[] truncated = new String(unique, 'UTF-8').replace('<hits>1</hits>', '<hits>2</hits>').getBytes('UTF-8')
    assert feed(miniStart(), truncated).relationship == 'failure'
    assert run(prepareClass, attrs + ['package.name': '../escape']).relationship == 'failure'
    println 'PASS: generic URN lookup, duplicate report, expected-priref validation, optional hierarchy, direct work exports, partial failure/restart, evidence-only payload directory, deduplication, stale-file cleanup, cycle/failure handling and submission integration.'
} finally {
    Files.walk(scratch).withCloseable { it.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
    loader.close()
}
