import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes

def ff = session.get()
if (!ff) return

// List only immediate, non-hidden directories. Do not follow directory symlinks.
// Explicit attribute reads throw on inspection errors; isDirectory alone would
// return false and could silently omit packages when permissions are insufficient.
def listDirectories = { Path directory ->
    List<Path> directories = []
    Files.newDirectoryStream(directory).withCloseable { entries ->
        entries.each { Path entry ->
            if (!entry.fileName.toString().startsWith('.')) {
                def attributes = Files.readAttributes(entry, BasicFileAttributes, LinkOption.NOFOLLOW_LINKS)
                if (attributes.isDirectory()) {
                    directories.add(entry)
                }
            }
        }
    }
    directories.sort { it.fileName.toString() }
}

List<Path> packages = []
Path root

try {
    def rootPath = ff.getAttribute('inputDirectory')?.trim()
    if (!rootPath) {
        throw new IllegalArgumentException('Missing inputDirectory attribute')
    }

    root = Paths.get(rootPath).toAbsolutePath().normalize()
    if (!Files.isDirectory(root)) {
        throw new IllegalArgumentException("Not a directory: ${root}")
    }

    // produksjon/<year-month>/<package> -- two directory levels, not three.
    // Collect the listing before emitting children so listing errors produce no partial batch.
    listDirectories(root).each { Path monthDir ->
        packages.addAll(listDirectories(monthDir))
    }
} catch (Exception e) {
    log.error('Package listing failed', e)
    ff = session.putAllAttributes(ff, [
        'error.stage'  : 'discovery.list_packages',
        'error.message': (e.message ?: e.toString()).take(2048)
    ])
    session.transfer(ff, REL_FAILURE)
    return
}

// Leave session errors unhandled so ExecuteGroovyScript's rollback strategy
// rolls back the entire batch if creating or transferring a child fails.
packages.each { Path packageDir ->
    def child = session.create(ff)
    child = session.putAllAttributes(child, [
        'package.name': packageDir.fileName.toString(),
        'package.path': packageDir.toString()
    ])
    session.transfer(child, REL_SUCCESS)
}

log.info("Listed ${packages.size()} film-produksjon packages under ${root}".toString())
session.remove(ff)
