import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import groovy.xml.XmlSlurper
import java.nio.file.*

class SubmissionBodyFlowFile {
    Map<String, String> attributes
    byte[] body = 'original body'.getBytes('UTF-8')
    Map getAttributes() { attributes }
}
class SubmissionBodySession {
    SubmissionBodyFlowFile ff
    String relationship
    SubmissionBodyFlowFile get() { ff }
    SubmissionBodyFlowFile putAttribute(SubmissionBodyFlowFile file, String key, String value) {
        file.attributes[key] = value
        file
    }
    SubmissionBodyFlowFile write(SubmissionBodyFlowFile file, Object callback) {
        def output = new ByteArrayOutputStream()
        callback.process(new ByteArrayInputStream(file.body), output)
        file.body = output.toByteArray()
        file
    }
    void transfer(SubmissionBodyFlowFile file, Object rel) { relationship = rel.toString() }
}

def loader = new GroovyClassLoader()
loader.parseClass('''package org.apache.nifi.processor.io
interface StreamCallback {
    void process(java.io.InputStream inputStream, java.io.OutputStream outputStream) throws java.io.IOException
}''')
def scriptClass = loader.parseClass(new File('film_produksjon/07_Submission body/05_Make submission body.groovy'))
def baselineClass = loader.parseClass(new File('film_bevaring/07_Submission body/05_Make submission body.groovy'))
def run = { Map attrs, Class selected = scriptClass ->
    def ff = new SubmissionBodyFlowFile(attributes: new LinkedHashMap(attrs))
    def session = new SubmissionBodySession(ff: ff)
    def script = selected.getDeclaredConstructor().newInstance()
    script.binding = new Binding(session: session, REL_SUCCESS: 'success', REL_FAILURE: 'failure')
    script.run()
    [ff: ff, relationship: session.relationship]
}
def parse = { result -> new JsonSlurper().parseText(new String(result.ff.body, 'UTF-8')) }
def attrs = [
    'package.name': 'digifilm_1300107_20130729',
    'metadata.pidDataUrns': 'URN:NBN:no-nb_digifilm_1300107_20130729',
    'metadata.barcode': 'OWN-BARCODE',
    'metadata.mainTitle': JsonOutput.toJson([title: [value: 'Tittel med æøå']]),
    'metadata.workRelations': JsonOutput.toJson([relation: [[id: '999', type: 'CopiedFrom', title: 'Work relation']]]),
    'metadata.digitalItemPartRelations': JsonOutput.toJson([relation: [
        [id: '69185513', type: 'CopiedFrom', title: ''],
        [id: '69185515', type: 'CopiedFrom', title: 'Kilde "A" fra sjøen'],
        [id: '69185513', type: 'CopiedFrom', title: 'Kilde B'],
        [id: '70398215', type: 'CopiedTo', title: 'Not a source']
    ]])
]
Path scratch = Files.createTempDirectory('produksjon-source-title-test-')
try {
    def result = run(attrs)
    assert result.relationship == 'success': result.ff.attributes
    def output = parse(result)
    assert output.metadata.source == [
        [identifier: [[type: 'Collections priref', value: '69185513']],
         URI: 'https://collections.nb.no/collections/link/xplus/audiovisualcatalogue/69185513', description: 'Kilde B'],
        [identifier: [[type: 'Collections priref', value: '69185515']],
         URI: 'https://collections.nb.no/collections/link/xplus/audiovisualcatalogue/69185515', description: 'Kilde "A" fra sjøen']
    ]
    assert new String(result.ff.body, 'UTF-8').contains('æøå')
    def baseline = run(attrs, baselineClass)
    assert baseline.relationship == 'success'
    output.metadata.remove('source')
    assert output == parse(baseline) // Existing fields and relation contents remain unchanged.

    [null, [relation: []], [relation: [[id: '70398215', type: 'CopiedTo']]]].each { relations ->
        def withoutSource = new LinkedHashMap(attrs)
        if (relations == null) withoutSource.remove('metadata.digitalItemPartRelations')
        else withoutSource['metadata.digitalItemPartRelations'] = JsonOutput.toJson(relations)
        def absent = run(withoutSource)
        assert absent.relationship == 'success'
        assert !parse(absent).metadata.containsKey('source')
    }

    // The captured catalog dump contains two digital parts copied from one source.
    def catalog = new XmlSlurper().parse(new File('film_bevaring/catalog_dump/descriptive/digifilm_798430_20130919_FOIA00000513_DIGITAL_ITEM_PART_70016212_70016214.xml'))
    def relations = catalog.recordList.record.CopiedFrom.collect { node ->
        [id: node.'copied_from.lref'.text(), title: node.'copied_from.title'.text(), type: 'CopiedFrom']
    }
    assert relations.size() == 2
    def captured = run(attrs + ['metadata.digitalItemPartRelations': JsonOutput.toJson([relation: relations])])
    assert captured.relationship == 'success'
    assert parse(captured).metadata.source == [[
        identifier: [[type: 'Collections priref', value: '61306767']],
        URI: 'https://collections.nb.no/collections/link/xplus/audiovisualcatalogue/61306767', description: 'Solungen'
    ]]

    def noTitle = run(attrs + ['metadata.digitalItemPartRelations': '{"relation":[{"id":"42","type":"CopiedFrom"}]}'])
    assert noTitle.relationship == 'success'
    assert !parse(noTitle).metadata.source[0].containsKey('description')

    // Source titles belong to source record IDs, not the current package's title.
    Path descriptive = Files.createDirectories(scratch.resolve('work/metadata/descriptive'))
    Files.writeString(descriptive.resolve('source-records.xml'), '''<adlibXML><recordList>
      <record priref="69185513">
        <Title><title>Secondary title</title></Title>
        <Title><title>Kilde B – sjøen</title><title.main>x</title.main></Title>
      </record>
      <record><priref>69185515</priref>
        <Title><title>Alternate title</title></Title>
        <Title><title>Kilde A</title><title.type>Originaltittel</title.type></Title>
      </record>
      <record priref="70398215"><Title><title>Current package title</title></Title></record>
    </recordList></adlibXML>''')
    def blankTitles = JsonOutput.toJson([relation: [
        [id: '69185513', type: 'CopiedFrom', title: ''],
        [id: '69185515', type: 'CopiedFrom', title: '']
    ]])
    def fromRecords = run(attrs + ['metadata.digitalItemPartRelations': blankTitles,
        'metadata.descriptive.dir': descriptive.toString()])
    assert fromRecords.relationship == 'success': fromRecords.ff.attributes
    assert parse(fromRecords).metadata.source*.description == ['Kilde B – sjøen', 'Kilde A']
    assert parse(fromRecords).metadata.title.value == 'Tittel med æøå'
    def fromRelation = run(attrs + ['metadata.descriptive.dir': descriptive.toString()])
    assert parse(fromRelation).metadata.source*.description == ['Kilde B', 'Kilde "A" fra sjøen']

    // A source title can also be read from the retained SIP after work deletion.
    Path transfer = Files.createDirectories(scratch.resolve('transfer/metadata/descriptive'))
    Files.writeString(transfer.resolve('source-record.xml'), '''<adlibXML><recordList>
      <record priref="42"><Title><title>Retained source title</title></Title></record>
    </recordList></adlibXML>''')
    def retained = run(attrs + ['metadata.digitalItemPartRelations': '{"relation":[{"id":"42","type":"CopiedFrom"}]}',
        'metadata.descriptive.dir': scratch.resolve('missing-work').toString(),
        'transfer.dir': scratch.resolve('transfer').toString()])
    assert retained.relationship == 'success': retained.ff.attributes
    assert parse(retained).metadata.source[0].description == 'Retained source title'
    def unmatched = run(attrs + ['metadata.digitalItemPartRelations': '{"relation":[{"id":"43","type":"CopiedFrom"}]}',
        'metadata.descriptive.dir': descriptive.toString()])
    assert unmatched.relationship == 'success'
    assert !parse(unmatched).metadata.source[0].containsKey('description')

    // Real produksjon export: a current-record title does not fill empty source titles.
    Path productionDump = Path.of('film_produksjon/examples/catalog_dump/digifilm_801190_20130711_DIGITAL_ITEM_PART_70398711.xml')
    def productionRecord = new XmlSlurper().parse(productionDump.toFile()).recordList.record[0]
    assert productionRecord.@priref.text() == '70398711'
    def productionRelations = productionRecord.CopiedFrom.collect { node ->
        [id: node.'copied_from.lref'.text(), title: node.'copied_from.title'.text(), type: 'CopiedFrom']
    }
    assert productionRelations*.id == ['68902056', '68902058']
    assert productionRelations.every { !it.title }
    String productionTitle = productionRecord.Title.find { it.'title.main'.text() == 'x' }.title.text()
    assert productionTitle == 'Over Nordpolen : med Hjalmar Riiser-Larsen'
    def production = run(attrs + [
        'package.name': 'digifilm_801190_20130711',
        'metadata.pidDataUrns': 'URN:NBN:no-nb_digifilm_801190_20130711',
        'metadata.mainTitle': JsonOutput.toJson([title: [value: productionTitle]]),
        'metadata.digitalItemPartRelations': JsonOutput.toJson([relation: productionRelations]),
        'metadata.descriptive.dir': productionDump.parent.toString()
    ])
    assert production.relationship == 'success': production.ff.attributes
    assert parse(production).metadata.source.collect { it.identifier[0].value } == ['68902056', '68902058']
    assert parse(production).metadata.source.every { !it.containsKey('description') }
    assert parse(production).metadata.title.value == productionTitle

    // Source formats must come from the matching source, not the output MOV.
    // Field values are confirmed by the user's Collections screenshots.
    Path formatDump = Files.createDirectories(scratch.resolve('source-format-dumps'))
    Files.writeString(formatDump.resolve('source-records.xml'), '''<adlibXML><recordList>
      <record priref="68902056"><file.format>Image/x-dpx</file.format>
        <Title><title>Over Nordpolen : med Hjalmar Riiser-Larsen</title></Title>
      </record>
      <record priref="68902058"><file.format>Audio/wav</file.format>
        <Part_of><part_of.title>Over Nordpolen : med Hjalmar Riiser-Larsen</part_of.title></Part_of>
      </record>
      <record priref="70398711"><file.format>Video/quicktime</file.format>
        <Title><title>Current MOV</title></Title>
      </record>
    </recordList></adlibXML>''')
    def formats = run(attrs + [
        'metadata.digitalItemPartRelations': JsonOutput.toJson([relation: productionRelations]),
        'metadata.descriptive.dir': formatDump.toString()
    ])
    assert formats.relationship == 'success': formats.ff.attributes
    assert parse(formats).metadata.source*.description == [
        'Over Nordpolen : med Hjalmar Riiser-Larsen — Image/x-dpx',
        'Over Nordpolen : med Hjalmar Riiser-Larsen — Audio/wav'
    ]
    assert parse(formats).metadata.source.collect { it.identifier[0].value } == ['68902056', '68902058']
    assert parse(formats).metadata.source.every { !it.description.contains('Video/quicktime') }

    Files.writeString(formatDump.resolve('format-only.xml'), '''<adlibXML><recordList>
      <record priref="42"><file.format>Audio/wav</file.format></record>
    </recordList></adlibXML>''')
    def formatOnly = run(attrs + ['metadata.digitalItemPartRelations': '{"relation":[{"id":"42","type":"CopiedFrom"}]}',
        'metadata.descriptive.dir': formatDump.toString()])
    assert formatOnly.relationship == 'success'
    assert parse(formatOnly).metadata.source[0].description == 'Audio/wav'

    // Parent ITEM 68902057 describes child ITEM_PART 68902058 explicitly.
    // Never assign a parent's own format/title to a different child source.
    Path hierarchyDump = Files.createDirectories(scratch.resolve('hierarchy-dumps'))
    Files.writeString(hierarchyDump.resolve('components.xml'), '''<adlibXML><recordList>
      <record priref="68902057">
        <Title><title>Parent title must not replace child title</title></Title>
        <file.format>Wrong-parent-format</file.format>
        <Parts><parts.reference.lref>68902058</parts.reference.lref>
          <parts.title>Over Nordpolen : med Hjalmar Riiser-Larsen</parts.title>
          <parts.fileformat>Audio/wav</parts.fileformat>
        </Parts>
        <Parts><parts.reference.lref>123</parts.reference.lref>
          <parts.title>Unrelated carrier</parts.title><parts.fileformat>Video/quicktime</parts.fileformat>
        </Parts>
      </record>
      <record priref="25801190"><Parts>
        <parts_reference><priref>68902056</priref>
          <Title><title>Over Nordpolen : med Hjalmar Riiser-Larsen</title></Title>
        </parts_reference><parts.fileformat>Image/x-dpx</parts.fileformat>
      </Parts></record>
    </recordList></adlibXML>''')
    def hierarchy = run(attrs + [
        'metadata.digitalItemPartRelations': JsonOutput.toJson([relation: productionRelations]),
        'metadata.descriptive.dir': hierarchyDump.toString()
    ])
    assert hierarchy.relationship == 'success': hierarchy.ff.attributes
    assert parse(hierarchy).metadata.source*.description == [
        'Over Nordpolen : med Hjalmar Riiser-Larsen — Image/x-dpx',
        'Over Nordpolen : med Hjalmar Riiser-Larsen — Audio/wav'
    ]
    assert parse(hierarchy).metadata.source.collect { it.identifier[0].value } == ['68902056', '68902058']

    // Verify the Parts XML shape against a real exported manifestation, in isolation.
    Path capturedHierarchy = Files.createDirectories(scratch.resolve('captured-hierarchy'))
    Files.copy(Path.of('film_bevaring/catalog_dump/descriptive/digifilm_798430_20130919_FOIA00000513_MANIFESTATION_25798430.xml'),
        capturedHierarchy.resolve('manifestation.xml'))
    def capturedParts = run(attrs + [
        'metadata.digitalItemPartRelations': JsonOutput.toJson([relation: [
            [id: '70016211', type: 'CopiedFrom'], [id: '70016213', type: 'CopiedFrom']
        ]]),
        'metadata.descriptive.dir': capturedHierarchy.toString()
    ])
    assert capturedParts.relationship == 'success': capturedParts.ff.attributes
    assert parse(capturedParts).metadata.source*.description == ['Solungen — Image/x-dpx', 'Solungen — Audio/wav']

    // Actual four-file produksjon capture: formats identify source components,
    // while the existing item-part relations identify different carrier IDs.
    Path fourDumps = Path.of('film_produksjon/examples/catalog_dump/digifilm_727480_20130730')
    def actualPart = new XmlSlurper().parse(fourDumps.resolve('digifilm_727480_20130730_DIGITAL_ITEM_PART_70398205.xml').toFile()).recordList.record[0]
    def actualItem = new XmlSlurper().parse(fourDumps.resolve('digifilm_727480_20130730_DIGITAL_ITEM_70398204.xml').toFile()).recordList.record[0]
    def actualManifestation = new XmlSlurper().parse(fourDumps.resolve('digifilm_727480_20130730_MANIFESTATION_25727480.xml').toFile()).recordList.record[0]
    def actualRelations = actualPart.CopiedFrom.collect { node ->
        [id: node.'copied_from.lref'.text(), title: node.'copied_from.title'.text(), type: 'CopiedFrom']
    }
    assert actualRelations*.id == ['69194879', '69194881']
    assert actualItem.CopiedFrom.collect { it.'copied_from.lref'.text() } == ['69194878', '69194880']
    assert actualManifestation.Parts.find { it.'parts.reference.lref'.text() == '69194878' }.'parts.fileformat'.text() == 'Image/x-dpx'
    assert actualManifestation.Parts.find { it.'parts.reference.lref'.text() == '69194880' }.'parts.fileformat'.text() == 'Audio/wav'
    def fourFileAttributes = attrs + [
        'package.name': 'digifilm_727480_20130730',
        'metadata.pidDataUrns': 'URN:NBN:no-nb_digifilm_727480_20130730',
        'metadata.mainTitle': '{"title":{"value":"Kontakten knyttes"}}',
        'metadata.digitalItemPartRelations': JsonOutput.toJson([relation: actualRelations]),
        'metadata.descriptive.dir': fourDumps.toString()
    ]
    def fourFileResult = run(fourFileAttributes)
    assert fourFileResult.relationship == 'success': fourFileResult.ff.attributes
    assert parse(fourFileResult).metadata.source.collect { it.identifier[0].value } == ['69194878', '69194880']
    assert parse(fourFileResult).metadata.source*.description == ['Kontakten knyttes — Image/x-dpx', 'Kontakten knyttes — Audio/wav']
    assert parse(fourFileResult).metadata.source == new JsonSlurper().parse(new File(
        'film_produksjon/examples/submission/digifilm_727480_20130730.source.json')).metadata.source
    assert parse(fourFileResult).metadata.relation.findAll { it.type == 'CopiedFrom' && it.id != '999' }*.id == ['69194879', '69194881']
    def fourFileMetadata = parse(fourFileResult)
    fourFileMetadata.metadata.remove('source')
    assert fourFileMetadata == parse(run(fourFileAttributes, baselineClass))
    // No offset/ordering assumption transfers component metadata to carrier IDs.
    def wrongUrn = run(fourFileAttributes + ['metadata.pidDataUrns': 'URN:NBN:no-nb_some_other_package'])
    assert wrongUrn.relationship == 'success'
    assert parse(wrongUrn).metadata.source.collect { it.identifier[0].value } == ['69194879', '69194881']
    assert parse(wrongUrn).metadata.source.every { !it.containsKey('description') }
    Path sameUrnDump = Files.createDirectories(scratch.resolve('same-urn-on-item'))
    Files.newDirectoryStream(fourDumps, '*.xml').withCloseable { files ->
        files.each { path -> Files.copy(path, sameUrnDump.resolve(path.fileName)) }
    }
    Path itemWithUrn = sameUrnDump.resolve('digifilm_727480_20130730_DIGITAL_ITEM_70398204.xml')
    Files.writeString(itemWithUrn, Files.readString(itemWithUrn).replace('</record>',
        '<PID_data_URN>URN:NBN:no-nb_digifilm_727480_20130730</PID_data_URN></record>'))
    def itemAlsoIdentified = run(fourFileAttributes + ['metadata.descriptive.dir': sameUrnDump.toString()])
    assert itemAlsoIdentified.relationship == 'success': itemAlsoIdentified.ff.attributes
    assert parse(itemAlsoIdentified).metadata.source == parse(fourFileResult).metadata.source
    Path retainedFour = Files.createDirectories(scratch.resolve('retained-four/metadata/descriptive'))
    Files.newDirectoryStream(fourDumps, '*.xml').withCloseable { files ->
        files.each { path -> Files.copy(path, retainedFour.resolve(path.fileName)) }
    }
    def retainedFourResult = run(fourFileAttributes + [
        'metadata.descriptive.dir': scratch.resolve('deleted-work').toString(),
        'transfer.dir': scratch.resolve('retained-four').toString()
    ])
    assert retainedFourResult.relationship == 'success': retainedFourResult.ff.attributes
    assert parse(retainedFourResult).metadata.source == parse(fourFileResult).metadata.source
    ['not-json', '{"relation":[{"id":"not-a-priref","type":"CopiedFrom"}]}'].each { bad ->
        def failed = run(attrs + ['metadata.digitalItemPartRelations': bad])
        assert failed.relationship == 'failure'
        assert failed.ff.attributes['submission.payload.status'] == 'FAIL'
        assert new String(failed.ff.body, 'UTF-8') == 'original body'
    }
    println 'PASS: component sources via resource URN and parent ITEM, actual four-file dumps in work/SIP, title/format descriptions, unchanged existing metadata, carrier fallback, ID binding, UTF-8, and failure routing.'
} finally {
    Files.walk(scratch).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
    }
    loader.close()
}
