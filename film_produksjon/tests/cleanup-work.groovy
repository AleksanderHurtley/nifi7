import java.nio.file.*

class CleanupTestFlowFile {
    Map<String, String> attributes
    String getAttribute(String name) { attributes[name] }
}
class CleanupTestSession {
    CleanupTestFlowFile flowFile
    String relationship
    CleanupTestFlowFile get() { flowFile }
    CleanupTestFlowFile putAllAttributes(CleanupTestFlowFile ff, Map values) { ff.attributes.putAll(values); ff }
    void transfer(CleanupTestFlowFile ff, Object relationship) { this.relationship = relationship.toString() }
}
def loader = new GroovyClassLoader()
def scriptClass = loader.parseClass(new File('film_produksjon/05_Cleanup work/E-ARK - Cleanup.groovy'))
def runStage = { Map attributes ->
    def ff = new CleanupTestFlowFile(attributes: new LinkedHashMap(attributes))
    def session = new CleanupTestSession(flowFile: ff)
    def script = scriptClass.getDeclaredConstructor().newInstance()
    script.binding = new Binding([session: session, REL_SUCCESS: 'success', REL_FAILURE: 'failure',
        log: [error: { Object message, Exception error -> }]])
    script.run()
    [attributes: ff.attributes, relationship: session.relationship]
}
String pkg = 'digifilm_1300107_20130729'
Path scratch = Files.createTempDirectory('produksjon-cleanup-work-test-')
int fixtureIndex = 0
def fixture = {
    Path base = scratch.resolve('fixture-' + fixtureIndex++)
    Path workBase = Files.createDirectories(base.resolve('work'))
    Path work = Files.createDirectory(workBase.resolve(pkg))
    Path source = Files.createDirectories(base.resolve('source').resolve(pkg))
    Files.writeString(source.resolve('keep.xml'), 'original source')
    Path sip = Files.createDirectories(base.resolve('transfer').resolve(pkg))
    Path payloads = Files.createDirectories(base.resolve('payloads').resolve(pkg))
    Files.writeString(payloads.resolve('submission.json'), '{"objectId":"test"}')
    Path events = payloads.resolve('events.ndjson')
    Files.writeString(events, 'external event payload retained unchanged\n')
    [work, sip].each { root ->
        Path data = Files.createDirectories(root.resolve('representations/primary_20130729/data'))
        Files.writeString(data.resolve("${pkg}.mov"), 'MOV')
        Files.createDirectories(root.resolve('metadata/descriptive'))
        Files.writeString(root.resolve('metadata/descriptive/keep.xml'), '<metadata/>')
    }
    Files.writeString(work.resolve('scratch.tmp'), 'leftover')
    Files.writeString(sip.resolve('METS.xml'), '<mets/>')
    Files.createDirectory(workBase.resolve('another-package'))
    Files.writeString(workBase.resolve('another-package/keep.txt'), 'other package')
    Files.createSymbolicLink(work.resolve('scratch-link'), source.resolve('keep.xml'))
    [work: work, workBase: workBase, source: source, sip: sip, payloads: payloads, events: events,
     attributes: ['package.name': pkg, 'work.base.dir': workBase.toString(), 'work.dir': work.toString()]]
}
def assertKept = { f ->
    assert Files.readString(f.source.resolve('keep.xml')) == 'original source'
    assert Files.readString(f.sip.resolve('METS.xml')) == '<mets/>'
    assert Files.readString(f.sip.resolve("representations/primary_20130729/data/${pkg}.mov")) == 'MOV'
    assert Files.readString(f.payloads.resolve('submission.json')) == '{"objectId":"test"}'
    assert Files.readString(f.events) == 'external event payload retained unchanged\n'
    assert Files.isDirectory(f.workBase)
    assert Files.readString(f.workBase.resolve('another-package/keep.txt')) == 'other package'
}
try {
    def good = fixture()
    def cleaned = runStage(good.attributes)
    assert cleaned.relationship == 'success': cleaned.attributes
    assert cleaned.attributes['cleanup.work.status'] == 'OK'
    assert cleaned.attributes['cleanup.work.deleted'] == 'true'
    assert !Files.exists(good.work, LinkOption.NOFOLLOW_LINKS)
    assertKept(good)
    def again = runStage(cleaned.attributes)
    assert again.relationship == 'success': again.attributes
    assert again.attributes['cleanup.work.deleted'] == 'false'
    assertKept(good)

    def guarded = fixture()
    [
        ['package.name': ''], ['work.dir': ''], ['work.base.dir': ''], ['work.base.dir': '/'],
        ['work.base.dir': 'relative/base'], ['work.dir': 'relative/work'],
        ['work.dir': guarded.workBase.toString()], ['work.dir': guarded.source.toString()],
        ['work.dir': guarded.work.resolve('metadata').toString()], ['package.name': '../other']
    ].each { overrides ->
        def rejected = runStage(guarded.attributes + overrides)
        assert rejected.relationship == 'failure': overrides
        assert rejected.attributes['error.stage'] == 'cleanup.work'
        assert Files.readString(guarded.work.resolve('scratch.tmp')) == 'leftover'
        assertKept(guarded)
    }
    // Workflow status, SIP completeness, and payload contents are not cleanup's responsibility.
    def independent = fixture()
    Files.delete(independent.sip.resolve('METS.xml'))
    def independentResult = runStage(independent.attributes + ['eark.status': 'FAIL', 'events.payload.status': 'FAIL'])
    assert independentResult.relationship == 'success': independentResult.attributes
    assert !Files.exists(independent.work)
    assert Files.readString(independent.events) == 'external event payload retained unchanged\n'

    // No required staging layout: even an empty package directory is removable.
    Path emptyBase = Files.createDirectories(scratch.resolve('empty-work'))
    Path emptyWork = Files.createDirectory(emptyBase.resolve(pkg))
    assert runStage(['package.name': pkg, 'work.base.dir': emptyBase.toString(), 'work.dir': emptyWork.toString()]).relationship == 'success'
    assert !Files.exists(emptyWork)

    ['work_symlink', 'base_symlink', 'not_directory'].each { kind ->
        def invalid = fixture()
        def attrs = invalid.attributes
        switch (kind) {
            case 'work_symlink':
                Path linked = scratch.resolve('linked-' + fixtureIndex++)
                Files.createDirectories(linked)
                Files.createSymbolicLink(linked.resolve(pkg), invalid.work)
                attrs = attrs + ['work.base.dir': linked.toString(), 'work.dir': linked.resolve(pkg).toString()]
                break
            case 'not_directory':
                Path alternateBase = Files.createDirectories(scratch.resolve('file-work-' + fixtureIndex++))
                Files.writeString(alternateBase.resolve(pkg), 'not a directory')
                attrs = attrs + ['work.base.dir': alternateBase.toString(), 'work.dir': alternateBase.resolve(pkg).toString()]
                break
            case 'base_symlink':
                Path linked = scratch.resolve('linked-' + fixtureIndex++)
                Files.createSymbolicLink(linked, invalid.workBase)
                attrs = attrs + ['work.base.dir': linked.toString(), 'work.dir': linked.resolve(pkg).toString()]
                break
        }
        def rejected = runStage(attrs)
        assert rejected.relationship == 'failure': [kind, rejected.attributes]
        assert Files.readString(invalid.work.resolve('scratch.tmp')) == 'leftover'
        assert Files.readString(invalid.source.resolve('keep.xml')) == 'original source'
    }
    println 'PASS: deletion-only cleanup with three inputs, exact package path/symlink guards, source/SIP/payload/other-package preservation, and idempotent retry.'
} finally {
    // Only the temporary fixture tree created above; do not follow its symlinks.
    Files.walk(scratch).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }
    loader.close()
}
