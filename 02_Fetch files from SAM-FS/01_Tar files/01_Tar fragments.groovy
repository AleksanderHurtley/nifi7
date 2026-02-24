import java.nio.file.*

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = "fetch.tar.fragments"
final int ERROR_DETAILS_MAX = 2048

def capDetails = { s ->
    if (s == null) return null
    String t = s.toString()
    (t.length() > ERROR_DETAILS_MAX) ? t.substring(0, ERROR_DETAILS_MAX) : t
}

def setFailure = { flowFile, String message, String details = null ->
    def out = session.putAttribute(flowFile, "error.stage", ERROR_STAGE)
    out = session.putAttribute(out, "error.message", message ?: "TAR fragment discovery failed")
    if (details != null && details.toString().trim()) {
        out = session.putAttribute(out, "error.details", capDetails(details))
    }
    return out
}

try {
    def pkgPath = ff.getAttribute("package.path")
    def pkgName = ff.getAttribute("package.name")

    if (!pkgPath || !pkgName) {
        throw new IllegalArgumentException("Missing package.name or package.path")
    }

    def pkgDir = new File(pkgPath)
    if (!pkgDir.exists() || !pkgDir.isDirectory()) {
        throw new IllegalStateException("Package directory does not exist: ${pkgPath}")
    }

    // TAR files live in images/
    def imageDir = new File(pkgDir, "images")

    if (!imageDir.exists() || !imageDir.isDirectory()) {
        throw new IllegalStateException("Images directory does not exist: ${imageDir.absolutePath}")
    }

    def tarList = imageDir.listFiles({ f ->
        f.isFile() && f.name.toLowerCase().endsWith(".tar")
    } as FileFilter)

    if (tarList == null) {
        throw new IllegalStateException("listFiles returned null for ${imageDir.absolutePath}")
    }

    def tarFiles = tarList.toList()

    if (tarFiles.isEmpty()) {
        throw new IllegalStateException("No TAR files found in ${imageDir.absolutePath}")
    }

    tarFiles.sort { it.name }

    int total = tarFiles.size()

    tarFiles.eachWithIndex { tarFile, idx ->
        def child = session.create(ff)

        child = session.putAllAttributes(child, [
            "tar.name"            : tarFile.name as String,
            "tar.path"            : tarFile.absolutePath as String,
            "fragment.identifier" : pkgName as String,
            "fragment.index"      : idx.toString(),
            "fragment.count"      : total.toString()
        ])

        session.transfer(child, REL_SUCCESS)
    }

    // Remove original
    session.remove(ff)

} catch (Exception e) {
    log.error("Error in TAR listing: ${e.message}", e)
    ff = setFailure(ff, e.message ?: "TAR fragment discovery failed", e.toString())
    session.transfer(ff, REL_FAILURE)
}
