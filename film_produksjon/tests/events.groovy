import groovy.json.JsonSlurper
import java.nio.file.*
import java.security.MessageDigest
import java.time.Instant

// Run from the repository root with Groovy 4, groovy-xml, and groovy-json.
class EventTestFlowFile {
    Map<String, String> attributes
    String getAttribute(String name) { attributes[name] }
}
class EventTestSession {
    EventTestFlowFile flowFile
    String relationship
    EventTestFlowFile get() { flowFile }
    EventTestFlowFile putAllAttributes(EventTestFlowFile ff, Map values) {
        assert values.every { key, value -> key instanceof String && value instanceof String }
        ff.attributes.putAll(values)
        ff
    }
    void transfer(EventTestFlowFile ff, Object relationship) { this.relationship = relationship.toString() }
}
def loader = new GroovyClassLoader()
def scriptClass = loader.parseClass(new File('film_produksjon/04_Events/01_Create events payload.groovy'))
def cleanupClass = loader.parseClass(new File('film_produksjon/05_Cleanup work/E-ARK - Cleanup.groovy'))
def runScript = { Class compiled, Map attrs ->
    def ff = new EventTestFlowFile(attributes: new LinkedHashMap(attrs))
    def session = new EventTestSession(flowFile: ff)
    def script = compiled.getDeclaredConstructor().newInstance()
    script.binding = new Binding([session: session, REL_SUCCESS: 'success', REL_FAILURE: 'failure',
        log: [error: { Object message, Exception error -> }, warn: { Object message -> }]])
    script.run()
    [attributes: ff.attributes, relationship: session.relationship]
}
def runStage = { Map attrs -> runScript(scriptClass, attrs) }
String pkg = 'digifilm_796646_20230321'
String sourceText = Files.readString(Path.of("film_produksjon/examples/sam-fs/${pkg}/${pkg}.xml"))
byte[] media = 'small synthetic MOV fixture'.getBytes('UTF-8')
String md5 = MessageDigest.getInstance('MD5').digest(media).encodeHex().toString()
String relativeMov = "representations/primary_20230321/data/${pkg}.mov"
Path scratch = Files.createTempDirectory('produksjon-events-test-')
int fixtureIndex = 0
def fixture = {
    Path base = scratch.resolve('fixture-' + fixtureIndex++)
    Path work = base.resolve('work').resolve(pkg)
    Path sip = base.resolve('transfer').resolve(pkg)
    Path payloads = Files.createDirectories(base.resolve('payloads').resolve(pkg))
    Path mets = sip.resolve("metadata/descriptive/deprecated_mets/${pkg}.xml")
    Files.createDirectories(mets.parent)
    Files.writeString(mets, sourceText.replace('220812884653', media.length.toString())
        .replace('872e9deca085f6f5ad419d6de0dcc615', md5))
    Path stagedMets = work.resolve("metadata/descriptive/deprecated_mets/${pkg}.xml")
    Files.createDirectories(stagedMets.parent)
    Files.copy(mets, stagedMets)
    [work, sip].each { root ->
        Path mov = root.resolve(relativeMov)
        Files.createDirectories(mov.parent)
        Files.write(mov, media)
    }
    Files.writeString(sip.resolve('METS.xml'), '<mets/>') // SIP existence guard, not a schema-validation fixture.
    Path events = Files.createFile(payloads.resolve('events.ndjson'))
    def attrs = [
        'package.name': pkg, 'work.dir': work.toString(), 'work.base.dir': work.parent.toString(), 'transfer.dir': sip.toString(),
        'payloads.dir': payloads.toString(), 'events.payload.path': events.toString(),
        'media.mets.path': stagedMets.toString(), 'video.output.path': work.resolve(relativeMov).toString(),
        'media.urn': "URN:NBN:no-nb_${pkg}".toString(), 'media.expected.filename': "${pkg}.mov".toString(),
        'media.expected.md5': md5, 'media.verified.md5': md5,
        'media.expected.size': media.length.toString(), 'media.verified.size': media.length.toString(),
        'fetch.status': 'OK', 'checksum.status': 'OK', 'checksum.checked.count': '1', 'checksum.expected.count': '1',
        'metadata.org.status': 'OK', 'metadata.org.review.required': 'false',
        'eark.status': 'OK', 'eark.end': Instant.parse('2026-01-02T12:30:01.234Z').toEpochMilli().toString(),
        'transfer.event.datetime': '2026-01-02T12:00:00Z',
        'nifi.agent.version': '2.2.0', 'eark.agent.version': '1.0.10'
    ]
    [attributes: attrs, events: events, mets: mets, sip: sip, work: work]
}
def records = { Path file -> Files.readAllLines(file).collect { new JsonSlurper().parseText(it) } }
def replaceText = { Path file, String from, String to ->
    Files.writeString(file, Files.readString(file).replace(from, to))
}
def assertNoTemporary = { Path events ->
    Files.list(events.parent).withCloseable { files ->
        assert files.noneMatch { it.fileName.toString().startsWith('.events-') }
    }
}

try {
    def sample = fixture()
    byte[] sourceBefore = Files.readAllBytes(sample.mets)
    // Reproduce the real order: work is gone, while old FlowFile paths still point to it.
    def cleaned = runScript(cleanupClass, sample.attributes)
    assert cleaned.relationship == 'success': cleaned.attributes
    assert !Files.exists(sample.work)
    def built = runStage(cleaned.attributes)
    assert built.relationship == 'success': built.attributes
    assert built.attributes['events.payload.status'] == 'OK'
    assert built.attributes['events.payload.count'] == '3'
    assert built.attributes['events.source.mets.path'] == sample.mets.toString()
    def events = records(sample.events)
    assert events*.eventType == ['creation', 'transfer', 'information package creation']
    assert events.every { it.packageId == pkg && it.outcome == 'success' && !it.containsKey('event') }
    assert events.every { it.agent.agentType == 'software' && it.agent.containsKey('agentNote') && !it.agent.containsKey('agentNotes') }
    assert events[0].agent.agentName == 'FFmpeg'
    assert !events[0].agent.containsKey('agentVersion')
    assert events[0].eventDetail.contains('Lavf58.62.100')
    assert events[0].eventDetail.contains('derivation')
    assert events[0].eventDateTime == '2023-05-04T07:02:30Z'
    assert events[0].fileRef.relativePath == relativeMov
    assert !events[1].containsKey('fileRef') && !events[2].containsKey('fileRef')
    assert events[1].eventDateTime == '2026-01-02T12:00:00Z'
    assert events[1].agent.agentVersion == '2.2.0'
    assert events[1].eventDetail.contains('Java MessageDigest') && !events[1].eventDetail.contains('md5sum')
    assert events[2].eventDateTime == '2026-01-02T12:30:01Z'
    assert events[2].agent.agentVersion == '1.0.10'
    assert Arrays.equals(Files.readAllBytes(sample.mets), sourceBefore)
    String originalEvents = Files.readString(sample.events)
    assert originalEvents.contains('muliggjør')
    assert originalEvents.contains('arbeidsområde')
    assert originalEvents.contains('størrelse')
    assert originalEvents.contains('håndtering')
    assert runStage(built.attributes).relationship == 'success'
    assert Files.readString(sample.events) == originalEvents // Retry replaces, never appends.
    assertNoTemporary(sample.events)
    def withoutWorkPaths = new LinkedHashMap(built.attributes)
    ['work.dir', 'work.base.dir', 'media.mets.path', 'video.output.path'].each { withoutWorkPaths.remove(it) }
    assert runStage(withoutWorkPaths).relationship == 'success'
    assert Files.readString(sample.events) == originalEvents

    // Accept the live now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX") value,
    // retaining its instant rather than treating it as an epoch or resetting it to now.
    ['2026-01-02T13:30:01.234+01:00', '2026-01-02T14:30:01.234+02:00',
     '2026-01-02T12:30:01.234Z'].each { isoDate ->
        def fromIso = runStage(withoutWorkPaths + ['eark.end': isoDate])
        assert fromIso.relationship == 'success': fromIso.attributes
        assert fromIso.attributes['eark.end'] == isoDate
        assert fromIso.attributes['sip.event.datetime'] == '2026-01-02T12:30:01Z'
        assert Files.readString(sample.events) == originalEvents
    }

    [
        ['eark.end': ''], ['eark.status': 'FAIL'], ['eark.end': 'not-an-epoch'],
        ['eark.end': '2026-01-02T12:30:01.234'], // Missing timezone must not be guessed.
        ['eark.end': '999999999999999999999999999999'],
        ['eark.end': (Instant.now().toEpochMilli() + 86400000L).toString()],
        ['eark.end': Instant.now().plusSeconds(86400).toString()],
        ['transfer.event.datetime': '2026-01-03T00:00:00Z'],
        ['transfer.event.datetime': '2026-01-02T12:00:00'],
        ['checksum.status': 'FAIL'], ['metadata.org.review.required': 'true'],
        ['checksum.checked.count': '0'], ['media.expected.md5': '0' * 32],
        ['media.verified.size': '1'], ['media.urn': 'wrong'],
        ['nifi.agent.version': ''], ['eark.agent.version': ''],
        ['events.payload.path': sample.mets.toString()]
    ].each { overrides ->
        def rejected = runStage(sample.attributes + overrides)
        assert rejected.relationship == 'failure': overrides
        assert rejected.attributes['error.stage'] == 'events.payload.build'
        assert Files.readString(sample.events) == originalEvents
        assert Arrays.equals(Files.readAllBytes(sample.mets), sourceBefore)
        assertNoTemporary(sample.events)
    }

    ['timezone', 'conflicting_date', 'outcome', 'object', 'event_link', 'duplicate_application', 'agent', 'fixity'].each { kind ->
        def invalid = fixture()
        assert runStage(invalid.attributes).relationship == 'success'
        String before = Files.readString(invalid.events)
        switch (kind) {
            case 'timezone': replaceText(invalid.mets, '2023-05-04T09:02:30+02:00', '2023-05-04T09:02:30'); break
            case 'conflicting_date': replaceText(invalid.mets, '<premis:eventDateTime>2023-05-04T09:02:30+02:00', '<premis:eventDateTime>2023-05-05T09:02:30+02:00'); break
            case 'outcome': replaceText(invalid.mets, '>successful<', '>failure<'); break
            case 'object': replaceText(invalid.mets, "<premis:originalName>${pkg}.mov", '<premis:originalName>other.mov'); break
            case 'event_link': replaceText(invalid.mets, '<premis:linkingEventIdentifierValue>', '<premis:linkingEventIdentifierValue>wrong-'); break
            case 'duplicate_application': replaceText(invalid.mets, '</premis:creatingApplication>', '</premis:creatingApplication><premis:creatingApplication/>'); break
            case 'agent': replaceText(invalid.mets, '>FFmpeg<', '>Unknown software<'); break
            case 'fixity': replaceText(invalid.mets, md5, '0' * 32); break
        }
        def rejected = runStage(invalid.attributes)
        assert rejected.relationship == 'failure': [kind, rejected.attributes]
        assert Files.readString(invalid.events) == before
    }

    def noSip = fixture()
    Files.delete(noSip.sip.resolve('METS.xml'))
    assert runStage(noSip.attributes).relationship == 'failure'
    assert Files.size(noSip.events) == 0

    def missingRetainedMets = fixture()
    Files.delete(missingRetainedMets.mets)
    assert Files.exists(Path.of(missingRetainedMets.attributes['media.mets.path']))
    assert runStage(missingRetainedMets.attributes).relationship == 'failure' // No fallback to stale work metadata.
    assert Files.size(missingRetainedMets.events) == 0

    def changedSize = fixture()
    Files.writeString(changedSize.sip.resolve(relativeMov), 'different size')
    assert runStage(changedSize.attributes).relationship == 'failure'
    assert Files.size(changedSize.events) == 0

    def version = fixture()
    replaceText(version.mets, 'Lavf58.62.100', '6.0')
    assert runStage(version.attributes).relationship == 'success'
    assert records(version.events)[0].agent.agentVersion == '6.0'

    // Write all Norwegian letters as UTF-8, but retain JSON escaping for quotes,
    // backslashes, and embedded newlines so every event still occupies one line.
    def unicode = fixture()
    String specialVersion = 'Lavf-test æøå ÆØÅ "sitat" \\mappe\nny linje'
    replaceText(unicode.mets,
        '<premis:creatingApplicationVersion>Lavf58.62.100</premis:creatingApplicationVersion>',
        '<premis:creatingApplicationVersion>' + specialVersion + '</premis:creatingApplicationVersion>')
    assert runStage(unicode.attributes).relationship == 'success'
    String unicodeOutput = Files.readString(unicode.events)
    assert unicodeOutput.contains('æøå ÆØÅ')
    assert Files.readAllLines(unicode.events).size() == 3
    assert records(unicode.events)[0].eventDetail.contains(specialVersion)
    assert records(unicode.events)[1].eventDetail.contains('arbeidsområde')

    ['{"packageId":"other","eventType":"creation"}\n',
     "{\"packageId\":\"${pkg}\",\"eventType\":\"migration\"}\n",
     originalEvents + originalEvents,
     'not-json\n'].each { existing ->
        Files.writeString(sample.events, existing)
        assert runStage(sample.attributes).relationship == 'failure'
        assert Files.readString(sample.events) == existing
        assertNoTemporary(sample.events)
    }
    println 'PASS: three events after work cleanup, readable UTF-8 æøå and safe JSON escaping, historical linkage, epoch/ISO dates, fileRef, agent versions, retries, and SIP/fixity gating.'
} finally {
    Files.walk(scratch).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }
    loader.close()
}
