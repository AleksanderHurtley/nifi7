import java.nio.file.*
import java.security.MessageDigest

def ff = session.get()
if (!ff) return

def getAttr = { String k ->
    def v = ff.getAttribute(k)
    (v && v.trim()) ? v.trim() : null
}

// ------------------------------------------------------------
// Timing start
// ------------------------------------------------------------
long chkStart = System.currentTimeMillis()
ff = session.putAttribute(ff, "checksum.start", chkStart.toString())

try {
    // ------------------------------------------------------------
    // Required attributes
    // ------------------------------------------------------------
    def pkg             = getAttr("package.name")
    def dpxMetaDirStr   = getAttr("metadata.preservation.dpx.dir")
    def manifestPathStr = getAttr("dpxmeta.manifest.path")
    def dpxDirStr       = getAttr("dpx.unpack.dir")
    def dpxPreDirStr    = getAttr("dpx.unpack.dir.pre")

    def missing = []
    [["package.name", pkg],
     ["dpx.unpack.dir", dpxDirStr]].each { if (!it[1]) missing << it[0] }

    if (!missing.isEmpty()) {
        throw new RuntimeException("Missing required attributes: ${missing.join(', ')}")
    }

    Path dpxDir = Paths.get(dpxDirStr)
    if (!Files.isDirectory(dpxDir)) {
        throw new RuntimeException("DPX dir not found: ${dpxDir}")
    }

    boolean hasPreDir = false
    Path dpxPreDir = null
    if (dpxPreDirStr) {
        dpxPreDir = Paths.get(dpxPreDirStr)
        hasPreDir = Files.isDirectory(dpxPreDir)
    }

    // ------------------------------------------------------------
    // 1) Parse expected checksums from DPX manifest XML
    // ------------------------------------------------------------
    def expected = [:]              // filename -> md5
    def expectedSource = [:]        // filename -> source file name
    def duplicateConflicts = []     // same filename, different md5 between metadata entries
    def sourceKind = "dpx_manifest.xml"

    Path manifestPath = null
    if (manifestPathStr) {
        manifestPath = Paths.get(manifestPathStr)
    } else if (dpxMetaDirStr && pkg) {
        manifestPath = Paths.get(dpxMetaDirStr).resolve("${pkg}_dpx_manifest.xml")
    }

    if (manifestPath == null) {
        throw new RuntimeException("Missing both dpxmeta.manifest.path and metadata.preservation.dpx.dir; cannot load expected checksums")
    }
    if (!Files.isRegularFile(manifestPath)) {
        throw new RuntimeException("DPX manifest not found: ${manifestPath}")
    }

    def xml = new groovy.xml.XmlSlurper(false, false).parse(manifestPath.toFile())
    if ((xml.name()?.toString() ?: "") != "dpxManifest") {
        throw new RuntimeException("Invalid DPX manifest root element (expected dpxManifest): ${manifestPath}")
    }

    def packageIdAttr = (xml.@packageId?.toString() ?: "").trim()
    if (!packageIdAttr || packageIdAttr != pkg) {
        throw new RuntimeException("DPX manifest packageId does not match package.name (${pkg}): ${manifestPath}")
    }

    def algoAttr = (xml.@checksumAlgorithm?.toString() ?: "").trim().toUpperCase()
    if (algoAttr != "MD5") {
        throw new RuntimeException("DPX manifest checksumAlgorithm must be MD5: ${manifestPath}")
    }

    def batchNodes = xml.batches?.batch
    if (!batchNodes || batchNodes.size() == 0) {
        throw new RuntimeException("Invalid DPX manifest: missing batches/batch nodes: ${manifestPath}")
    }

    batchNodes.each { b ->
        def batchId = (b.@id?.toString() ?: "").trim()
        if (!batchId) {
            throw new RuntimeException("Invalid DPX manifest: batch id missing in ${manifestPath}")
        }

        def fileNodes = b.file
        if (!fileNodes || fileNodes.size() == 0) {
            throw new RuntimeException("Invalid DPX manifest: batch '${batchId}' has no file entries (${manifestPath})")
        }

        fileNodes.each { f ->
            def name = (f.@name?.toString() ?: "").trim()
            def md5  = (f.@md5?.toString() ?: "").trim()?.toLowerCase()

            if (!name) {
                throw new RuntimeException("Invalid DPX manifest: file/@name missing in batch '${batchId}' (${manifestPath})")
            }
            if (!(md5 ==~ /[a-f0-9]{32}/)) {
                throw new RuntimeException("Invalid DPX manifest: file/@md5 invalid for '${name}' in batch '${batchId}' (${manifestPath})")
            }

            def existing = expected[name]
            if (existing == null) {
                expected[name] = md5
                expectedSource[name] = manifestPath.fileName.toString()
            } else if (existing != md5) {
                duplicateConflicts << [
                    file       : name,
                    md5_first  : existing,
                    first_from : expectedSource[name],
                    md5_next   : md5,
                    next_from  : manifestPath.fileName.toString()
                ]
            }
        }
    }

    if (expected.isEmpty()) {
        throw new RuntimeException("No expected DPX checksums found in manifest: ${manifestPath}")
    }

    // ------------------------------------------------------------
    // 2) Build index of DPX files on disk (PRE overrides if same name)
    // ------------------------------------------------------------
    def actualIndex = [:] // filename -> absolute Path

    if (hasPreDir) {
        Files.newDirectoryStream(dpxPreDir).withCloseable { ds ->
            ds.each { Path p ->
                def n = p.fileName.toString().toLowerCase()
                if (Files.isRegularFile(p) && n.endsWith(".dpx")) {
                    actualIndex[p.fileName.toString()] = p.toAbsolutePath()
                }
            }
        }
    }

    Files.newDirectoryStream(dpxDir).withCloseable { ds ->
        ds.each { Path p ->
            def n = p.fileName.toString().toLowerCase()
            if (Files.isRegularFile(p) && n.endsWith(".dpx")) {
                actualIndex[p.fileName.toString()] = p.toAbsolutePath()
            }
        }
    }

    // ------------------------------------------------------------
    // 3) Validate: expected -> actual checksum
    // ------------------------------------------------------------
    def mismatches = []
    long checked = 0

    expected.each { fname, expMd5 ->
        Path absPath = actualIndex[fname]
        if (!absPath) {
            mismatches << [ file: fname, reason: "Missing on disk (expected from DPX manifest metadata)" ]
            return
        }

        checked++

        try {
            MessageDigest md = MessageDigest.getInstance("MD5")
            absPath.withInputStream { is ->
                byte[] buf = new byte[1024 * 1024]
                int r
                while ((r = is.read(buf)) != -1) {
                    md.update(buf, 0, r)
                }
            }
            String actual = md.digest().collect { String.format("%02x", it) }.join()

            if (!actual || actual != expMd5.toLowerCase()) {
                mismatches << [
                    file     : fname,
                    expected : expMd5,
                    actual   : actual ?: "",
                    reason   : "Checksum mismatch"
                ]
            }
        } catch (Exception ex) {
            mismatches << [ file: fname, reason: "MD5 compute failed", error: ex.message?.take(2000) ]
        }
    }

    // ------------------------------------------------------------
    // 4) Detect unexpected DPX files
    // ------------------------------------------------------------
    def unexpected = []
    actualIndex.keySet().each { fname ->
        if (!expected.containsKey(fname)) {
            unexpected << [ file: fname, reason: "Present on disk but not referenced in DPX manifest metadata" ]
        }
    }

    // ------------------------------------------------------------
    // Timing end
    // ------------------------------------------------------------
    long chkEnd = System.currentTimeMillis()
    long durationMs = chkEnd - chkStart

    ff = session.putAttribute(ff, "checksum.end", chkEnd.toString())
    ff = session.putAttribute(ff, "checksum.durationMs", durationMs.toString())

    def status = (mismatches.isEmpty() && duplicateConflicts.isEmpty() && unexpected.isEmpty()) ? "OK" : "FAIL"
    ff = session.putAttribute(ff, "checksum.status", status)

    // Stats as attributes
    ff = session.putAttribute(ff, "checksum.expected.count", expected.size().toString())
    ff = session.putAttribute(ff, "checksum.checked.count", checked.toString())
    ff = session.putAttribute(ff, "checksum.mismatches.count", mismatches.size().toString())
    ff = session.putAttribute(ff, "checksum.unexpected.count", unexpected.size().toString())
    ff = session.putAttribute(ff, "checksum.conflicts.count", duplicateConflicts.size().toString())
    ff = session.putAttribute(ff, "checksum.batch.files", manifestPath.fileName.toString())
    ff = session.putAttribute(ff, "checksum.source", sourceKind)

    // ------------------------------------------------------------
    // Route + event attributes
    // ------------------------------------------------------------
    if (status == "OK") {
        session.transfer(ff, REL_SUCCESS)
    } else {
        def summary = "Fixity validation failed: mismatches=${mismatches.size()}, conflicts=${duplicateConflicts.size()}, unexpected=${unexpected.size()}"
        ff = session.putAttribute(ff, "error.message", summary)
        log.error("DPX message digest validation failed for ${pkg}: mismatches=${mismatches.size()}, conflicts=${duplicateConflicts.size()}, unexpected=${unexpected.size()}")
        session.transfer(ff, REL_FAILURE)
    }

} catch (Exception e) {
    long chkEnd = System.currentTimeMillis()
    long durationMs = chkEnd - chkStart

    ff = session.putAttribute(ff, "checksum.end", chkEnd.toString())
    ff = session.putAttribute(ff, "checksum.durationMs", durationMs.toString())
    ff = session.putAttribute(ff, "checksum.status", "ERROR")
    ff = session.putAttribute(ff, "error.message", e.message ?: "Checksum validation error")

    def pkgErr = getAttr("package.name") ?: "UNKNOWN"
    log.error("Checksum validation error for ${pkgErr}: ${e.message}", e)
    session.transfer(ff, REL_FAILURE)
}
