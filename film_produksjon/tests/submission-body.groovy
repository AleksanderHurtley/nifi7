import groovy.json.JsonSlurper
import groovy.xml.XmlParser
import groovy.xml.XmlUtil
import java.nio.file.*

// Offline NiFi session simulation. PutFile remains a separate processor.
class SubmissionBodyFlowFile {
    Map<String, String> attributes
    byte[] body = 'original body'.getBytes('UTF-8')
    String getAttribute(String key) { attributes[key] }
    Map getAttributes() { attributes }
}
class SubmissionBodySession {
    SubmissionBodyFlowFile ff
    String relationship
    SubmissionBodyFlowFile get() { ff }
    SubmissionBodyFlowFile putAttribute(SubmissionBodyFlowFile file, String key, String value) {
        file.attributes[key] = value; file
    }
    SubmissionBodyFlowFile putAllAttributes(SubmissionBodyFlowFile file, Map values) {
        assert values.every { k, v -> k instanceof String && v instanceof String }
        file.attributes.putAll(values); file
    }
    SubmissionBodyFlowFile removeAttribute(SubmissionBodyFlowFile file, String key) {
        file.attributes.remove(key); file
    }
    SubmissionBodyFlowFile write(SubmissionBodyFlowFile file, Object callback) {
        def output = new ByteArrayOutputStream()
        callback.process(new ByteArrayInputStream(file.body), output)
        file.body = output.toByteArray(); file
    }
    void transfer(SubmissionBodyFlowFile file, Object rel) { relationship = rel.toString() }
}
def loader = new GroovyClassLoader()
loader.parseClass('''package org.apache.nifi.processor.io
interface StreamCallback {
    void process(java.io.InputStream inputStream, java.io.OutputStream outputStream) throws java.io.IOException
}''')
def scriptClass = loader.parseClass(new File('film_produksjon/07_Submission body/01_Build submission body.groovy'))
def run = { Map attrs, byte[] body = 'original body'.getBytes('UTF-8') ->
    def ff = new SubmissionBodyFlowFile(attributes: new LinkedHashMap(attrs), body: body)
    def session = new SubmissionBodySession(ff: ff)
    def script = scriptClass.getDeclaredConstructor().newInstance()
    script.binding = new Binding(session: session, log: [error: { Object msg, Exception e -> }],
        REL_SUCCESS: 'success', REL_FAILURE: 'failure')
    script.run()
    [attrs: ff.attributes, body: ff.body, relationship: session.relationship]
}
def parse = { result -> new JsonSlurper().parseText(new String(result.body, 'UTF-8')) }
def textXml = { String value -> value.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;') }
def xml = { String id, String type, String fields ->
    """<adlibXML><recordList><record priref="${id}" deleted="false">
    <record_type><value lang="neutral">${type}</value></record_type>${fields}
    </record></recordList></adlibXML>"""
}
Path scratch = Files.createTempDirectory('produksjon-unified-submission-test-')
try {
    String pkg = 'digifilm_815655_20130726'
    String urn = 'URN:NBN:no-nb_' + pkg
    Path fixture = Path.of('film_produksjon/examples/catalog_dump/' + pkg)
    Path payloads = Files.createDirectories(scratch.resolve('payloads'))
    Path payload = payloads.resolve('submission.json')
    Files.writeString(payload, 'Only PutFile may replace this')
    def attrs = ['package.name': pkg, 'catalog.dump.status': 'COMPLETE',
        'catalog.primary.priref': '162311522', 'catalog.primary.urn': urn,
        'metadata.descriptive.dir': fixture.toAbsolutePath().toString(),
        'payloads.dir': payloads.toString(), 'submission.payload.path': payload.toString(),
        'package.uuid': 'keep-uuid', 'prod_db_id': 'keep-db-id', 'checksum.status': 'OK',
        'metadata.preservation.dir': 'keep-preservation-path', 'metadata.org.status': 'OK',
        'media.verified.md5': 'keep-md5', 'media.verified.size': 'keep-size']
    def owned = ['metadata.mainTitle', 'metadata.alternativeTitles', 'metadata.creators',
        'metadata.contributors', 'metadata.publishers', 'metadata.workRelations',
        'metadata.countries', 'metadata.dates', 'metadata.descriptions', 'metadata.languages',
        'metadata.barcode', 'metadata.pidDataUrns', 'metadata.digitalItemPartRelations', 'metadata.provenance']
    def stale = owned.collectEntries { [(it): 'not even valid JSON from previous package'] }
    def rich = run(attrs + stale + ['submission.payload.status': 'FAIL', 'submission.payload.error': 'old failure',
        'error.stage': 'submission.body', 'error.message': 'old failure'])
    assert rich.relationship == 'success': rich.attrs
    def expected = new JsonSlurper().parse(new File(
        'film_produksjon/examples/submission/digifilm_815655_20130726.titled-relations.submission.json'))
    assert parse(rich) == expected
    assert rich.attrs.filename == 'submission.json'
    assert rich.attrs['mime.type'] == 'application/json'
    assert rich.attrs['submission.payload.status'] == 'OK'
    assert !rich.attrs.containsKey('submission.payload.error')
    assert !rich.attrs.containsKey('error.stage')
    owned.each { assert !rich.attrs.containsKey(it): it }
    attrs.each { key, value -> assert rich.attrs[key] == value }
    assert Files.readString(payload) == 'Only PutFile may replace this'
    assert new String(rich.body, 'UTF-8').contains('på landet')
    assert !new String(rich.body, 'UTF-8').contains('\\u00')

    // New filenames, with exactly the selected primary and its actual parents.
    // The captured combined response contains two candidates; do not use both.
    Path current = Files.createDirectories(scratch.resolve('work/metadata/descriptive'))
    def primaryDoc = new XmlParser().parse(fixture.resolve(pkg + '_DIGITAL_ITEM_PART_70398020_162311522.xml').toFile())
    primaryDoc.recordList[0].remove(primaryDoc.recordList.record.find { it.attribute('priref') == '70398020' })
    Files.writeString(current.resolve('PACKAGE_CATALOG_RECORD_162311522.xml'), XmlUtil.serialize(primaryDoc))
    ['DIGITAL_ITEM_162311521': 'ITEM_162311521', 'MANIFESTATION_25815655': 'MANIFESTATION_25815655',
     'WORK_815655': 'WORK_815655', 'COPIED_FROM_69119828': 'COPIED_FROM_69119828',
     'COPIED_FROM_69119830': 'COPIED_FROM_69119830'].each { oldName, newName ->
        Files.copy(fixture.resolve(pkg + '_' + oldName + '.xml'), current.resolve(newName + '.xml'))
    }
    def currentAttrs = attrs + ['metadata.descriptive.dir': current.toString()]
    assert parse(run(currentAttrs)) == expected
    // Unrelated but valid WORK records do not add metadata to this payload.
    Files.writeString(current.resolve('WORK_999.xml'), xml('999', 'WORK',
        '<Title><title>Old unrelated film</title></Title><production_country>Sverige</production_country>'))
    assert parse(run(currentAttrs)) == expected

    // Work is authoritative, including missing optional WORK fields. Do not fill
    // them from a prior SIP. A title can come from the primary/parent instead.
    Path oldSip = Files.createDirectories(scratch.resolve('retained/metadata/descriptive'))
    Files.copy(fixture.resolve(pkg + '_WORK_815655.xml'), oldSip.resolve('WORK_815655.xml'))
    Files.delete(current.resolve('WORK_815655.xml'))
    def noWork = run(currentAttrs + stale + ['transfer.dir': oldSip.parent.parent.toString()])
    assert noWork.relationship == 'success': noWork.attrs
    def noWorkMetadata = parse(noWork).metadata
    assert noWorkMetadata.title == [value: 'Per på landet']
    ['alternative','creator','contributor','publisher','spatial','date','description'].each {
        assert noWorkMetadata[it] == []: it
    }
    assert noWorkMetadata.source == expected.metadata.source
    assert !new String(noWork.body, 'UTF-8').contains('Old unrelated film')
    // Once work is absent, read one complete retained snapshot instead.
    Files.delete(oldSip.resolve('WORK_815655.xml'))
    Files.newDirectoryStream(current, '*.xml').withCloseable { paths -> paths.each { Files.copy(it, oldSip.resolve(it.fileName)) } }
    Files.copy(fixture.resolve(pkg + '_WORK_815655.xml'), oldSip.resolve('WORK_815655.xml'))
    def retained = run(currentAttrs + ['metadata.descriptive.dir': scratch.resolve('deleted-work').toString(),
        'transfer.dir': oldSip.parent.parent.toString()])
    assert retained.relationship == 'success': retained.attrs
    assert parse(retained) == expected

    // Reuse the incoming body from a different package. The four-export
    // capture has source IDs but no records explicitly tying them to formats.
    String another = 'digifilm_727480_20130730'
    def anotherPayload = run(attrs + stale + ['package.name': another,
        'catalog.primary.priref': '70398205', 'catalog.primary.urn': 'URN:NBN:no-nb_' + another,
        'metadata.descriptive.dir': Path.of('film_produksjon/examples/catalog_dump/' + another).toAbsolutePath().toString()], rich.body)
    assert anotherPayload.relationship == 'success': anotherPayload.attrs
    assert parse(anotherPayload).objectId == another
    assert parse(anotherPayload).metadata.title.value == 'Kontakten knyttes'
    assert parse(anotherPayload).metadata.source == new JsonSlurper().parse(new File(
        'film_produksjon/examples/submission/digifilm_727480_20130730.source.json')).metadata.source

    // Fresh minimal package with a primary title: zero, one or two sources,
    // no WORK, no previously extracted metadata and no catalog API calls.
    Path minimal = Files.createDirectories(scratch.resolve('minimal'))
    def minimalAttrs = attrs + ['metadata.descriptive.dir': minimal.toString(), 'catalog.primary.priref': '10']
    String trickyTitle = 'Båt "æøå" {test}: A\\B\\ — slutt'
    String primaryFields = "<PID_data_URN>${urn}</PID_data_URN><Title><title>${textXml(trickyTitle)}</title></Title>"
    Path primary = minimal.resolve('PACKAGE_CATALOG_RECORD_10.xml')
    Files.writeString(primary, xml('10','ITEM_PART',primaryFields))
    def zero = run(minimalAttrs + stale)
    assert zero.relationship == 'success': zero.attrs
    assert parse(zero).metadata.title.value == trickyTitle
    assert !parse(zero).metadata.containsKey('source')
    assert parse(zero).metadata.relation == []
    assert parse(zero).metadata.identifier == [[type:'URN',value:urn,lang:'eng']]
    assert parse(zero).metadata.provenance == expected.metadata.provenance
    String source40 = '<CopiedFrom><copied_from.lref>40</copied_from.lref><copied_from.title/></CopiedFrom>'
    Files.writeString(primary, xml('10','ITEM_PART',primaryFields + source40 + source40))
    def one = run(minimalAttrs)
    assert one.relationship == 'success': one.attrs
    assert parse(one).metadata.source == [[identifier:[[type:'Collections priref',value:'40']],
        URI:'https://collections.nb.no/collections/link/xplus/audiovisualcatalogue/40']]
    assert parse(one).metadata.relation == [[title:'',id:'40',type:'CopiedFrom']]
    Files.writeString(minimal.resolve('COPIED_FROM_40.xml'), xml('40','ITEM_PART','''
      <Title><title>Wrong alternate</title><title.type>Originaltittel</title.type></Title>
      <Title><title>Riktig kilde</title><title.main>x</title.main></Title>
      <file.format>Audio/wav</file.format><bar_code>SOURCE-ONLY-BARCODE</bar_code>
      <CopiedFrom><copied_from.lref>999</copied_from.lref></CopiedFrom>'''))
    String source50 = '<CopiedFrom><copied_from.lref>50</copied_from.lref></CopiedFrom>'
    Files.writeString(primary, xml('10','ITEM_PART',primaryFields + source40 + source50 +
        '<CopiedTo><copied_to.lref>60</copied_to.lref></CopiedTo>'))
    Files.writeString(minimal.resolve('COPIED_FROM_50.xml'), xml('50','ITEM_PART','<file.format>Image/x-dpx</file.format>'))
    def two = parse(run(minimalAttrs))
    assert two.metadata.source*.description == ['Riktig kilde — Audio/wav','Image/x-dpx']
    assert two.metadata.relation == [[title:'Riktig kilde',id:'40',type:'CopiedFrom'],
        [title:'',id:'50',type:'CopiedFrom'],[title:'',id:'60',type:'CopiedTo']]
    assert !two.metadata.identifier.any { it.value == 'SOURCE-ONLY-BARCODE' }
    assert !two.metadata.source.any { it.identifier[0].value == '999' }

    // Explicit child references can enrich sources; never infer a carrier ID
    // from its component ID or copy a parent/MOV format into the source.
    Files.delete(minimal.resolve('COPIED_FROM_40.xml'))
    Files.delete(minimal.resolve('COPIED_FROM_50.xml'))
    String parentLink = '<Part_of><part_of_reference.lref>20</part_of_reference.lref></Part_of>'
    Files.writeString(primary, xml('10','ITEM_PART',primaryFields + parentLink + source40 + source50))
    Files.writeString(minimal.resolve('ITEM_20.xml'), xml('20','ITEM','''
      <Title><title>Parent shared film</title></Title><file.format>Video/quicktime</file.format>
      <Part_of><part_of_reference.lref>30</part_of_reference.lref></Part_of>
      <Language><language>Norsk</language><language.usage>Originalspråk</language.usage></Language>
      <Language><language>Norsk</language><language.usage>Originalspråk</language.usage></Language>
      <Language><language>Engelsk</language></Language>
      <CopiedFrom><copied_from.lref>888</copied_from.lref></CopiedFrom>
      <Parts><parts.reference.lref>40</parts.reference.lref><parts.fileformat>Audio/wav</parts.fileformat></Parts>
      <Parts><parts.reference.lref>49</parts.reference.lref><parts.fileformat>Image/x-dpx</parts.fileformat></Parts>'''))
    Files.writeString(minimal.resolve('WORK_30.xml'), xml('30','WORK','''
      <Title><title>Original film</title><title.type>Originaltittel</title.type></Title>
      <Title><title>Original film</title><title.type>Tittelvariant</title.type></Title>
      <Title><title>Another title</title></Title><Title><title>Another title</title></Title>
      <Credits><credit.name>Director</credit.name><credit.type>Regissør</credit.type></Credits>
      <Credits><credit.name>Company</credit.name><credit.type>Produksjonsselskap</credit.type></Credits>
      <Credits><credit.name>Producer</credit.name><credit.type>Produksjon/produsent</credit.type></Credits>
      <Credits><credit.name>Publisher</credit.name><credit.type>Utgiver</credit.type></Credits>
      <Credits><credit.name>Distributor</credit.name><credit.type>Distribusjon</credit.type></Credits>
      <Credits><credit.name>Broadcaster</credit.name><credit.type>Kringkaster</credit.type></Credits>
      <Dating><dating.date.start>1964</dating.date.start><dating.type>Produksjon</dating.type></Dating>
      <Dating><dating.date.start>1964</dating.date.start><dating.type>Produksjon</dating.type></Dating>
      <Dating><dating.date.start>1965</dating.date.start></Dating>
      <production_country>Norge</production_country><production_country>Norge</production_country>
      <Description><description>English note</description><description.type>Engelsk til publisering</description.type></Description>
      <Description><description>Norsk notat</description></Description>
      <Segment_of><segment_of.lref>123</segment_of.lref><segment_of.title>Collection</segment_of.title></Segment_of>
      <av_series><av_series.lref>124</av_series.lref><av_series.title>Series</av_series.title><av_series.episode_no>2</av_series.episode_no></av_series>
      <related_object><related_object.reference.lref>50</related_object.reference.lref></related_object>'''))
    Files.writeString(minimal.resolve('ANALOG_ITEM_PART_70.xml'), xml('70','ITEM_PART','<bar_code>BOX-42</bar_code>'))
    def mapped = run(minimalAttrs)
    assert mapped.relationship == 'success': mapped.attrs
    def m = parse(mapped).metadata
    assert m.title == [value:'Original film']
    assert m.alternative == [[type:'Ukjent',value:'Another title']]
    assert m.creator == [[name:'Director',type:'Person',role:'Regissør']]
    assert m.contributor == [[name:'Company',type:'korporasjon',role:'Produksjonsselskap'],[name:'Producer',type:'Person',role:'Produksjon/produsent']]
    assert m.publisher == ['Publisher','Distributor','Broadcaster'].collect { [name:it,type:'korporasjon'] }
    assert m.date == [[type:'Produksjon',value:'1964'],[type:'Ukjent',value:'1965']]
    assert m.spatial == [[name:'Norge',type:'Country']]
    assert m.description == [[value:'English note',lang:'eng'],[value:'Norsk notat',lang:'nob']]
    assert m.language == [[type:'Originalspråk',value:'nob'],[type:'Ukjent',value:'Engelsk']]
    assert m.identifier[-1] == [type:'HYLLESIGNATUR',value:'BOX-42',lang:'nob']
    assert m.relation.findAll { it.type in ['SegmentOf','IsPartOfSeries'] } == [[title:'Collection',id:'123',type:'SegmentOf'],
        [title:'Series',id:'124',type:'IsPartOfSeries'],[title:'2',id:'124',type:'IsPartOfSeries']]
    assert m.relation.find { it.type == 'RelatedTo' }.title == ''
    assert m.source[0].description == 'Parent shared film — Audio/wav'
    assert !m.source[1].containsKey('description') // 49 cannot supply 50's format.
    assert m.relation.find { it.id == '40' && it.type == 'CopiedFrom' }.title == 'Parent shared film'
    assert !m.source.any { it.identifier[0].value == '888' }
    // Existing explicit relationship titles are not overwritten by source titles.
    Files.writeString(primary, xml('10','ITEM_PART',primaryFields + source40.replace('<copied_from.title/>','<copied_from.title>Behold</copied_from.title>')))
    Files.writeString(minimal.resolve('COPIED_FROM_40.xml'), xml('40','ITEM_PART','<Title><title>Different</title></Title><file.format>Audio/wav</file.format>'))
    def existingTitle = parse(run(minimalAttrs))
    assert existingTitle.metadata.relation == [[title:'Behold',id:'40',type:'CopiedFrom']]
    assert existingTitle.metadata.source[0].description == 'Behold — Audio/wav'

    // Missing required identity, incomplete catalog, mismatched URN/record and
    // malformed XML fail without changing the incoming body or writing a file.
    def failures = [
        ['catalog.dump.status':'FETCH'], ['catalog.dump.status':'FAIL'], ['catalog.primary.priref':'9999'],
        ['catalog.primary.priref':'../escape'], ['media.urn':'wrong'], ['catalog.urn':'wrong'],
        ['catalog.primary.urn':'wrong'], ['objectId':'old-package'], ['metadata.descriptive.dir':'relative'],
        ['payloads.dir':'relative'], ['metadata.descriptive.dir':scratch.resolve('missing').toString()]
    ]
    failures.each { change ->
        def failed = run(minimalAttrs + stale + change)
        assert failed.relationship == 'failure': change
        assert failed.attrs['submission.payload.status'] == 'FAIL'
        assert failed.attrs['error.stage'] == 'submission.body'
        assert new String(failed.body,'UTF-8') == 'original body'
        owned.each { assert !failed.attrs.containsKey(it) }
    }
    ['package.name','catalog.primary.priref','catalog.dump.status','metadata.descriptive.dir','payloads.dir'].each { key ->
        def missing = new LinkedHashMap(minimalAttrs); missing.remove(key)
        assert run(missing).relationship == 'failure': key
    }
    String validPrimary = Files.readString(primary)
    [validPrimary.replace(urn,'URN:wrong'), validPrimary.replace('deleted="false"','deleted="true"'),
     validPrimary.replace('>40</copied_from.lref>','>bad-id</copied_from.lref>'), '<adlibXML>'].each { invalid ->
        Files.writeString(primary, invalid)
        assert run(minimalAttrs).relationship == 'failure'
    }
    Files.writeString(primary, xml('10','ITEM_PART',"<PID_data_URN>${urn}</PID_data_URN>"))
    assert run(minimalAttrs).relationship == 'failure' // No title anywhere on its line.
    Files.writeString(primary, validPrimary)
    Files.writeString(minimal.resolve('ITEM_PART_10.xml'), validPrimary)
    assert run(minimalAttrs).relationship == 'failure' // Conflicting duplicate exports.
    Files.delete(minimal.resolve('ITEM_PART_10.xml'))
    Files.writeString(minimal.resolve('unrelated.xml'), '<not catalog XML')
    Files.createDirectories(minimal.resolve('deprecated_mets'))
    Files.writeString(minimal.resolve('deprecated_mets/old.xml'), '<not catalog XML')
    assert run(minimalAttrs).relationship == 'success' // Unrecognized/legacy files are not parsed.
    assert Files.readString(payload) == 'Only PutFile may replace this'
    println 'PASS: unified payload matches captured examples; selected-primary/parent mapping, source enrichment, optional WORK, no stale attributes, snapshot fallback, metadata defaults/deduplication, UTF-8/JSON escaping, filename/MIME, no disk writes and failure routing.'
} finally {
    Files.walk(scratch).withCloseable { it.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) } }
    loader.close()
}
