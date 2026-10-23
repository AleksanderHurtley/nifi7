import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = 'cleanup.work'

try {
    def required = { String name ->
        String value = ff.getAttribute(name)?.trim()
        if (!value) throw new IllegalArgumentException("Missing attribute: ${name}")
        value
    }
    def pathAttribute = { String name ->
        Path path = Paths.get(required(name))
        if (!path.isAbsolute()) throw new IllegalArgumentException("${name} must be absolute")
        path.normalize()
    }
    String pkg = required('package.name')
    if (!(pkg ==~ /digifilm_\d+_\d{8}/)) throw new IllegalArgumentException('Invalid package.name')
    Path base = pathAttribute('work.base.dir')
    Path work = pathAttribute('work.dir')
    if (base == base.root || work != base.resolve(pkg)) {
        throw new IllegalArgumentException('Refusing cleanup: work.dir must be exactly work.base.dir/package.name, not the base itself or another path')
    }
    if (Files.isSymbolicLink(base)) {
        throw new IOException('Refusing cleanup: work.base.dir is a symlink')
    }
    boolean exists = Files.exists(work, LinkOption.NOFOLLOW_LINKS)
    if (exists && !Files.isDirectory(work, LinkOption.NOFOLLOW_LINKS)) {
        throw new IOException('Refusing cleanup: work.dir is not a real directory (symlinks are not allowed)')
    }
    if (exists) {
        Path realBase = base.toRealPath()
        Path realWork = work.toRealPath()
        if (realBase == realBase.root || realWork.parent != realBase || realWork.fileName.toString() != pkg) {
            throw new IOException('Refusing cleanup: resolved work path escapes its package base')
        }
        // Default walkFileTree does not follow symlinks. Any scratch symlink is deleted, not its target.
        Files.walkFileTree(work, new SimpleFileVisitor<Path>() {
            @Override
            FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file)
                return FileVisitResult.CONTINUE
            }

            @Override
            FileVisitResult postVisitDirectory(Path dir, IOException error) throws IOException {
                if (error != null) throw error
                Files.deleteIfExists(dir)
                return FileVisitResult.CONTINUE
            }
        })
    }
    ff = session.putAllAttributes(ff, [
        'cleanup.work.status': 'OK', 'cleanup.work.deleted': exists.toString(),
        'cleanup.work.path': work.toString()
    ])
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    log.error('Produksjon work cleanup failed', e)
    ff = session.putAllAttributes(ff, [
        'cleanup.work.status': 'FAIL', 'error.stage': ERROR_STAGE,
        'error.message': (e.message ?: 'Work cleanup failed').take(2048),
        'error.details': e.toString().take(2048)
    ])
    session.transfer(ff, REL_FAILURE)
}
