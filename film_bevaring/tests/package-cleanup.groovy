import java.nio.file.*

class PackageCleanupFlowFile {
    Map<String, String> attributes
    String getAttribute(String key) { attributes[key] }
}
class PackageCleanupSession {
    PackageCleanupFlowFile ff
    String relationship
    PackageCleanupFlowFile get() { ff }
    PackageCleanupFlowFile putAttribute(PackageCleanupFlowFile file, String key, String value) {
        file.attributes[key] = value
        file
    }
    PackageCleanupFlowFile removeAttribute(PackageCleanupFlowFile file, String key) {
        file.attributes.remove(key)
        file
    }
    void transfer(PackageCleanupFlowFile file, Object rel) { relationship = rel.toString() }
}

def loader = new GroovyClassLoader()
def scriptFile = new File(args ? args[0] : 'film_bevaring/10_Package cleanup/01_Delete package directories.groovy')
def scriptClass = loader.parseClass(scriptFile)
def run = { Map attrs ->
    def ff = new PackageCleanupFlowFile(attributes: new LinkedHashMap(attrs))
    def session = new PackageCleanupSession(ff: ff)
    def script = scriptClass.getDeclaredConstructor().newInstance()
    script.binding = new Binding(session: session, REL_SUCCESS: 'success', REL_FAILURE: 'failure')
    script.run()
    [attributes: ff.attributes, relationship: session.relationship]
}
Path scratch = Files.createTempDirectory('bevaring-package-cleanup-test-').toRealPath()
def labels = ['payloads', 'transfer', 'work']
int next = 0
def fixture = { String pkg = 'digifilm_18098525_20200406_FOIA00002229' ->
    Path home = Files.createDirectory(scratch.resolve('fixture-' + next++))
    Path source = Files.createDirectories(home.resolve('source').resolve(pkg))
    Files.writeString(source.resolve('keep.xml'), 'source metadata')
    def attrs = ['package.name': pkg]
    def paths = [:]
    labels.each { label ->
        Path base = Files.createDirectory(home.resolve(label))
        Path target = Files.createDirectory(base.resolve(pkg))
        Files.createDirectories(target.resolve('nested'))
        Files.writeString(target.resolve('nested/data.bin'), label)
        Files.writeString(target.resolve('.hidden'), 'hidden file')
        Files.createDirectory(base.resolve('another-package'))
        Files.writeString(base.resolve('another-package/keep.txt'), 'other package')
        Files.createSymbolicLink(target.resolve('source-link'), source)
        Files.createSymbolicLink(target.resolve('broken-link'), source.resolve('missing'))
        attrs["${label}.base.dir".toString()] = base.toString()
        paths[label] = target
    }
    [home: home, source: source, attrs: attrs, paths: paths]
}
def kept = { f ->
    assert Files.readString(f.source.resolve('keep.xml')) == 'source metadata'
    labels.each { label ->
        Path base = Paths.get(f.attrs["${label}.base.dir".toString()])
        assert Files.readString(base.resolve('another-package/keep.txt')) == 'other package'
    }
}
try {
    ['digifilm_18098525_20200406_FOIA00002229', 'digifilm_727480_20130730'].each { pkg ->
        def f = fixture(pkg)
        // Cleanup deliberately does not depend on any workflow status.
        def result = run(f.attrs + ['eark.status': 'FAIL', 'events.payload.status': 'FAIL'])
        assert result.relationship == 'success': result
        assert result.attributes['cleanup.package.status'] == 'OK'
        assert result.attributes['cleanup.package.deleted.any'] == 'true'
        labels.each { label ->
            assert !Files.exists(f.paths[label], LinkOption.NOFOLLOW_LINKS)
            assert result.attributes["cleanup.package.deleted.${label}".toString()] == 'true'
            assert result.attributes["cleanup.package.path.${label}".toString()] == f.paths[label].toString()
        }
        kept(f)
        def retry = run(result.attributes)
        assert retry.relationship == 'success'
        assert retry.attributes['cleanup.package.deleted.any'] == 'false'
        labels.each { label ->
            assert retry.attributes["cleanup.package.deleted.${label}".toString()] == 'false'
            assert retry.attributes["cleanup.package.exists.${label}".toString()] == 'false'
        }
        kept(f)
    }

    def missing = fixture()
    Files.walk(missing.paths.work).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }
    def missingResult = run(missing.attrs)
    assert missingResult.relationship == 'success'
    assert missingResult.attributes['cleanup.package.deleted.work'] == 'false'
    assert missingResult.attributes['cleanup.package.deleted.payloads'] == 'true'
    kept(missing)

    def invalid = fixture()
    [
        ['package.name': ''], ['package.name': '.'], ['package.name': '..'],
        ['package.name': 'child/..'], ['package.name': '../another-package'],
        ['package.name': '/absolute'], ['package.name': 'child/nested'], ['package.name': 'child\\nested'],
        ['work.base.dir': ''], ['work.base.dir': '/'], ['work.base.dir': 'relative/work']
    ].each { overrides ->
        def rejected = run(invalid.attrs + overrides + ['cleanup.package.deleted.any': 'true', 'cleanup.package.deleted.work': 'true'])
        assert rejected.relationship == 'failure': overrides
        assert rejected.attributes['error.stage'] == 'cleanup.package'
        assert rejected.attributes['cleanup.package.deleted.any'] == 'false'
        assert rejected.attributes['cleanup.package.deleted.work'] == 'false'
        labels.each { label -> assert Files.exists(invalid.paths[label].resolve('nested/data.bin')) }
        kept(invalid)
    }

    // The last target must be validated before the first is deleted.
    ['base-link', 'target-link', 'broken-target-link', 'file-target'].each { kind ->
        def f = fixture()
        Path alternate = Files.createDirectory(f.home.resolve('alternate'))
        def attrs = f.attrs
        if (kind == 'base-link') {
            Path link = f.home.resolve('linked-base')
            Files.createSymbolicLink(link, f.paths.work.parent)
            attrs = attrs + ['work.base.dir': link.toString()]
        } else {
            Path target = alternate.resolve(attrs['package.name'])
            if (kind == 'file-target') Files.writeString(target, 'not a directory')
            else Files.createSymbolicLink(target, kind == 'target-link' ? f.source : f.home.resolve('missing'))
            attrs = attrs + ['work.base.dir': alternate.toString()]
        }
        def rejected = run(attrs)
        assert rejected.relationship == 'failure': [kind, rejected]
        labels.each { label -> assert Files.exists(f.paths[label].resolve('nested/data.bin')) }
        kept(f)
    }
    println 'PASS: deletion-only cleanup, both package-name formats, missing work, retries, all-target preflight, invalid paths, symlinks, and source/other-package preservation.'
} finally {
    Files.walk(scratch).withCloseable { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
    }
    loader.close()
}
