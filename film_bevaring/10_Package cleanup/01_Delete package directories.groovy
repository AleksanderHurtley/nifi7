import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = 'cleanup.package'
def labels = ['payloads', 'transfer', 'work']

try {
    // Reset per-run results so a retry cannot retain earlier deletion flags.
    ff = session.putAttribute(ff, 'cleanup.package.deleted.any', 'false')
    labels.each { label ->
        ff = session.putAttribute(ff, "cleanup.package.exists.${label}", 'false')
        ff = session.putAttribute(ff, "cleanup.package.deleted.${label}", 'false')
        ff = session.removeAttribute(ff, "cleanup.package.path.${label}")
    }

    def required = { String key ->
        String value = ff.getAttribute(key)?.trim()
        if (!value) throw new IllegalArgumentException("Missing required attribute: ${key}")
        value
    }
    String pkg = required('package.name')
    Path name = Paths.get(pkg)
    if (name.isAbsolute() || name.nameCount != 1 || pkg in ['.', '..'] ||
            pkg.contains('/') || pkg.contains('\\')) {
        throw new IllegalArgumentException('package.name must be a single directory name, not a path')
    }

    def attributes = { Path path ->
        try {
            Files.readAttributes(path, BasicFileAttributes, LinkOption.NOFOLLOW_LINKS)
        } catch (NoSuchFileException ignored) {
            null
        }
    }

    // Validate every target before deletion. The calling flow decides when to run.
    def targets = labels.collect { String label ->
        Path base = Paths.get(required("${label}.base.dir"))
        if (!base.isAbsolute()) throw new IllegalArgumentException("${label}.base.dir must be absolute")
        base = base.normalize()
        Path target = base.resolve(pkg).normalize()
        if (base == base.root || target == base || target.parent != base) {
            throw new IllegalArgumentException("Refusing cleanup: invalid ${label} package path: ${target}")
        }
        def baseAttrs = attributes(base)
        if (baseAttrs != null && !baseAttrs.isDirectory()) {
            throw new IOException("Refusing cleanup: ${label} base is not a real directory (symlinks are not allowed): ${base}")
        }
        Path realBase = baseAttrs != null ? base.toRealPath() : null
        if (realBase != null && realBase == realBase.root) {
            throw new IOException("Refusing cleanup: ${label} base resolves to filesystem root")
        }
        def targetAttrs = attributes(target)
        if (targetAttrs != null) {
            if (!targetAttrs.isDirectory()) {
                throw new IOException("Refusing cleanup: ${label} target is not a real directory (symlinks are not allowed): ${target}")
            }
            Path realTarget = target.toRealPath()
            if (realBase == null || realTarget.parent != realBase || realTarget.fileName.toString() != pkg) {
                throw new IOException("Refusing cleanup: resolved ${label} target escapes its package base")
            }
        }
        ff = session.putAttribute(ff, "cleanup.package.path.${label}", target.toString())
        ff = session.putAttribute(ff, "cleanup.package.exists.${label}", (targetAttrs != null).toString())
        [label: label, path: target, exists: targetAttrs != null]
    }

    targets.each { target ->
        if (target.exists) {
            // No FOLLOW_LINKS: delete links inside the package, never their targets.
            Files.walkFileTree(target.path, new SimpleFileVisitor<Path>() {
                @Override
                FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file)
                    FileVisitResult.CONTINUE
                }

                @Override
                FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                    if (error != null) throw error
                    Files.deleteIfExists(dir)
                    FileVisitResult.CONTINUE
                }
            })
            ff = session.putAttribute(ff, "cleanup.package.deleted.${target.label}", 'true')
            ff = session.putAttribute(ff, 'cleanup.package.deleted.any', 'true')
        }
    }
    ff = session.putAttribute(ff, 'cleanup.package.status', 'OK')
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    ff = session.putAttribute(ff, 'cleanup.package.status', 'FAIL')
    ff = session.putAttribute(ff, 'error.stage', ERROR_STAGE)
    ff = session.putAttribute(ff, 'error.message', (e.message ?: 'Package cleanup failed').take(2048))
    ff = session.putAttribute(ff, 'error.details', e.toString().take(2048))
    session.transfer(ff, REL_FAILURE)
}
