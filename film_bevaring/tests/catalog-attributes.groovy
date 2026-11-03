import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.nio.file.*

// Run from the repository root with Groovy 4. No NiFi server is needed.
class CatalogAttributesFlowFile {
    Map<String, String> attributes
    byte[] body = 'input content'.getBytes('UTF-8')
    String getAttribute(String key) { attributes[key] }
    Map getAttributes() { attributes }
}
class CatalogAttributesSession {
    CatalogAttributesFlowFile ff
    String relationship
    CatalogAttributesFlowFile get() { ff }
    CatalogAttributesFlowFile putAttribute(CatalogAttributesFlowFile file, String key, String value) {
        file.attributes[key] = value
        file
    }
    CatalogAttributesFlowFile removeAttribute(CatalogAttributesFlowFile file, String key) {
        file.attributes.remove(key)
        file
    }
    CatalogAttributesFlowFile write(CatalogAttributesFlowFile file, Object callback) {
        def output = new ByteArrayOutputStream()
        callback.process(new ByteArrayInputStream(file.body), output)
        file.body = output.toByteArray()
        file
    }
    void transfer(CatalogAttributesFlowFile file, Object rel) { relationship = rel.toString() }
}

def stages = [
    [name: '01_Get Work.groovy', suffix: 'WORK_1', keys: [
        'metadata.mainTitle', 'metadata.alternativeTitles', 'metadata.creators',
        'metadata.contributors', 'metadata.publishers', 'metadata.workRelations',
        'metadata.countries', 'metadata.dates', 'metadata.descriptions'
    ], xml: '''
      <Title><title>Current film</title><title.type>Originaltittel</title.type></Title>
      <Title><title>Current alternate</title><title.type>Alternativ tittel</title.type></Title>
      <Credits><credit.name>Current director</credit.name><credit.type>Regissør</credit.type></Credits>
      <Credits><credit.name>Current company</credit.name><credit.type>Produksjonsselskap</credit.type></Credits>
      <Credits><credit.name>Current publisher</credit.name><credit.type>Utgiver</credit.type></Credits>
      <Dating><dating.date.start>1964</dating.date.start><dating.type>Produksjon</dating.type></Dating>
      <production_country>Norge</production_country>
      <Description><description>Current description</description><description.type>Norsk til publisering</description.type></Description>
      <Segment_of><segment_of.title>Current collection</segment_of.title><segment_of.lref>123</segment_of.lref></Segment_of>
    '''],
    [name: '02_Get DIGITAL ITEM.groovy', suffix: 'DIGITAL_ITEM_2',
     keys: ['metadata.languages'], xml: '''
      <Language><language>Norsk</language><language.usage>Originalspråk</language.usage></Language>
    '''],
    [name: '03_Get Analog item part bar_code.groovy', suffix: 'ANALOG_ITEM_PART_3',
     keys: ['metadata.barcode'], xml: '<bar_code>CURRENT-BARCODE</bar_code>'],
    [name: '04_Get digital item part.groovy', suffix: 'DIGITAL_ITEM_PART_4',
     keys: ['metadata.pidDataUrns', 'metadata.digitalItemPartRelations', 'metadata.provenance'], xml: '''
      <PID_data_URN>URN:NBN:no-nb_digifilm_815655_20130726</PID_data_URN>
      <CopiedFrom><copied_from.lref>69119828</copied_from.lref><copied_from.title>Current source</copied_from.title></CopiedFrom>
      <Part_of><part_of_reference>
        <CopiedFrom><copied_from.lref>69119827</copied_from.lref></CopiedFrom>
        <CopiedTo><copied_to.lref>999</copied_to.lref></CopiedTo>
      </part_of_reference></Part_of>
    ''']
]
def loader = new GroovyClassLoader()
loader.parseClass('''package org.apache.nifi.processor.io
interface StreamCallback {
    void process(java.io.InputStream inputStream, java.io.OutputStream outputStream) throws java.io.IOException
}''')
stages.each { stage ->
    stage.script = loader.parseClass(new File('film_bevaring/07_Submission body', stage.name))
}
def builder = loader.parseClass(new File('film_produksjon/07_Submission body/05_Make submission body.groovy'))
def run = { Class selected, Map attrs ->
    def ff = new CatalogAttributesFlowFile(attributes: new LinkedHashMap(attrs))
    def session = new CatalogAttributesSession(ff: ff)
    def script = selected.getDeclaredConstructor().newInstance()
    script.binding = new Binding(session: session, REL_SUCCESS: 'success', REL_FAILURE: 'failure')
    script.run()
    [ff: ff, relationship: session.relationship]
}
def parse = { String json -> new JsonSlurper().parseText(json) }
def errorKeys = ['submission.payload.status', 'submission.payload.error']
Path scratch = Files.createTempDirectory('catalog-attributes-test-')
try {
    def protectedAttrs = [
        'package.name': 'digifilm_815655_20130726', 'package.uuid': 'keep-package-uuid',
        'prod_db_id': 'keep-db-id', 'work.dir': 'keep-work-dir',
        'metadata.preservation.dir': 'keep-preservation-dir',
        'metadata.org.review.required': 'false', 'checksum.status': 'OK',
        'submission.payload.path': scratch.resolve('submission.json').toString()
    ]
    def stale = stages.collectMany { it.keys }.collectEntries { [(it): 'STALE'] }
    stages.eachWithIndex { stage, i ->
        Path dir = Files.createDirectory(scratch.resolve('stage-' + i))
        Path xml = dir.resolve('digifilm_815655_20130726_' + stage.suffix + '.xml')
        def base = protectedAttrs + stale + [
            'metadata.descriptive.dir': dir.toString(),
            'transfer.dir': scratch.resolve('no-retained-sip').toString(),
            'submission.payload.status': 'FAIL', 'submission.payload.error': 'previous error'
        ]
        def checkProtected = { result ->
            protectedAttrs.each { k, v -> assert result.ff.attributes[k] == v }
            assert result.ff.attributes['metadata.descriptive.dir'] == dir.toString()
            (stale.keySet() - stage.keys).each { k -> assert result.ff.attributes[k] == 'STALE' }
        }
        def checkAbsentOutputs = { result ->
            stage.keys.findAll { it != 'metadata.provenance' }.each { k ->
                assert !result.ff.attributes.containsKey(k): [stage.name, k, result.ff.attributes[k]]
            }
        }
        Files.writeString(xml, '<adlibXML><recordList><record>' + stage.xml + '</record></recordList></adlibXML>')
        def rich = run(stage.script, base)
        assert rich.relationship == 'success': stage.name
        stage.keys.each { k -> assert rich.ff.attributes[k] && rich.ff.attributes[k] != 'STALE' }
        errorKeys.each { k -> assert !rich.ff.attributes.containsKey(k) }
        checkProtected(rich)
        if (stage.suffix == 'DIGITAL_ITEM_PART_4') {
            assert parse(rich.ff.attributes['metadata.digitalItemPartRelations']).relation*.id == ['69119828']
        }

        // Reuse the successful FlowFile after optional fields disappear.
        Files.writeString(xml, '<adlibXML><recordList><record/></recordList></adlibXML>')
        def sparse = run(stage.script, rich.ff.attributes)
        assert sparse.relationship == 'success': stage.name
        checkAbsentOutputs(sparse)
        checkProtected(sparse)

        Files.delete(xml)
        def absent = run(stage.script, base)
        assert absent.relationship == 'success': stage.name
        checkAbsentOutputs(absent)
        checkProtected(absent)

        // Bad XML and missing required inputs route to failure without stale outputs.
        Files.writeString(xml, '<adlibXML><recordList>')
        def malformed = run(stage.script, base)
        assert malformed.relationship == 'failure': stage.name
        stage.keys.each { k -> assert !malformed.ff.attributes.containsKey(k) }
        assert malformed.ff.attributes['submission.payload.status'] == 'FAIL'
        assert malformed.ff.attributes['submission.payload.error'] != 'previous error'
        checkProtected(malformed)
        def missingInput = new LinkedHashMap(base)
        missingInput.remove('metadata.descriptive.dir')
        def missing = run(stage.script, missingInput)
        assert missing.relationship == 'failure': stage.name
        stage.keys.each { k -> assert !missing.ff.attributes.containsKey(k) }
        assert missing.ff.attributes['submission.payload.error'].contains('metadata.descriptive.dir')

        // Prefer one snapshot per export type. Identical work/SIP copies must
        // produce the same outputs, and a changed SIP must not add older data.
        Files.writeString(xml, '<adlibXML><recordList><record>' + stage.xml + '</record></recordList></adlibXML>')
        Path sipDir = Files.createDirectories(scratch.resolve('sip-' + i + '/metadata/descriptive'))
        Path sipXml = sipDir.resolve(xml.fileName)
        Files.copy(xml, sipXml)
        def bothAttrs = base + ['transfer.dir': sipDir.parent.parent.toString()]
        def identicalCopies = run(stage.script, bothAttrs)
        assert identicalCopies.relationship == 'success': stage.name
        stage.keys.each { k -> assert identicalCopies.ff.attributes[k] == rich.ff.attributes[k]: [stage.name, k] }
        def olderXml = stage.xml.replace('Current', 'Retained').replace('CURRENT', 'RETAINED')
            .replace('Norge', 'Sverige').replace('Norsk', 'Engelsk').replace('1964', '1950')
            .replace('815655', '999999').replace('69119828', '999990')
        Files.writeString(sipXml, '<adlibXML><recordList><record>' + olderXml + '</record></recordList></adlibXML>')
        def changedCopies = run(stage.script, bothAttrs)
        assert changedCopies.relationship == 'success': stage.name
        stage.keys.each { k -> assert changedCopies.ff.attributes[k] == rich.ff.attributes[k]: [stage.name, k] }
        Files.delete(xml)
        def sipFallback = run(stage.script, bothAttrs)
        assert sipFallback.relationship == 'success': stage.name
        stage.keys.each { k -> assert sipFallback.ff.attributes[k] && sipFallback.ff.attributes[k] != 'STALE' }
        assert stage.keys.any { k -> sipFallback.ff.attributes[k] != rich.ff.attributes[k] }: stage.name
    }

    // Regression: the supplied WORK export has no alternative titles or work relations.
    Path retained = Files.createDirectories(scratch.resolve('retained/metadata/descriptive'))
    Path captured = Path.of('film_produksjon/examples/catalog_dump/digifilm_815655_20130726_WORK_815655.xml')
    Files.copy(captured, retained.resolve(captured.fileName))
    def oldAttrs = protectedAttrs + [
        'metadata.descriptive.dir': scratch.resolve('deleted-work').toString(),
        'transfer.dir': scratch.resolve('retained').toString(),
        'metadata.alternativeTitles': JsonOutput.toJson([alternative: [[type: 'Alternativ tittel', value: 'COLUMBI EGG SOM SKOLEMAT']]]),
        'metadata.workRelations': JsonOutput.toJson([relation: [[id: '1570469', title: 'Old SAS collection', type: 'SegmentOf']]]),
        'metadata.publishers': JsonOutput.toJson([publisher: [[name: 'Old publisher']]]),
        'metadata.creators': JsonOutput.toJson([creator: [[name: 'Old director']]]),
        'metadata.digitalItemPartRelations': JsonOutput.toJson([relation: [
            [id: '69119828', title: '', type: 'CopiedFrom'],
            [id: '69119830', title: '', type: 'CopiedFrom']
        ]])
    ]
    def current = run(stages[0].script, oldAttrs)
    assert current.relationship == 'success'
    assert !current.ff.attributes.containsKey('metadata.alternativeTitles')
    assert !current.ff.attributes.containsKey('metadata.workRelations')
    assert !current.ff.attributes.containsKey('metadata.publishers')
    assert parse(current.ff.attributes['metadata.mainTitle']).title.value == 'Per på landet'
    assert parse(current.ff.attributes['metadata.creators']).creator[0].name == 'Gran, Sverre'
    def payload = run(builder, current.ff.attributes)
    assert payload.relationship == 'success': payload.ff.attributes
    def metadata = parse(new String(payload.ff.body, 'UTF-8')).metadata
    assert metadata.alternative == []
    assert metadata.publisher == []
    assert metadata.relation*.id == ['69119828', '69119830']
    assert metadata.relation.every { it.type == 'CopiedFrom' }
    assert metadata.description[0].value.startsWith('Kortfilm om bygutten Per')
    assert !new String(payload.ff.body, 'UTF-8').contains('COLUMBI')

    // A Catalog retry may stage the same WORK in work while the SIP still has
    // the earlier copy. Reading both must not repeat countries in the payload.
    Path workCopy = Files.createDirectories(scratch.resolve('restaged-work/metadata/descriptive'))
    Files.copy(captured, workCopy.resolve(captured.fileName))
    def repeated = run(stages[0].script, current.ff.attributes + ['metadata.descriptive.dir': workCopy.toString()])
    assert repeated.relationship == 'success'
    assert parse(repeated.ff.attributes['metadata.countries']).spatial == [[name: 'Norge', type: 'Country']]
    def repeatedPayload = run(builder, repeated.ff.attributes)
    assert repeatedPayload.relationship == 'success'
    assert parse(new String(repeatedPayload.ff.body, 'UTF-8')).metadata == metadata

    println 'PASS: all four extractors reset outputs, deduplicate identical copies, prefer current work over changed SIP exports, retain SIP fallback, preserve unrelated attributes and route failures; Per på landet payload has no stale titles/relations or duplicate countries.'
} finally {
    Files.walk(scratch).withCloseable { stream ->
        stream.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
    }
}
