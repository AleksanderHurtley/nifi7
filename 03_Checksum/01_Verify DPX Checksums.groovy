import groovy.xml.XmlSlurper

import java.nio.file.*
import java.security.MessageDigest
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.Instant

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
    def pkg            = getAttr("package.name")
    def dpxMetaDirStr  = getAttr("metadata.preservation.dpx.dir")
    def dpxDirStr      = getAttr("dpx.unpack.dir")
    def dpxPreDirStr   = getAttr("dpx.unpack.dir.pre")

    def missing = []
    [["package.name", pkg],
     ["metadata.preservation.dpx.dir", dpxMetaDirStr],
     ["dpx.unpack.dir", dpxDirStr]].each { if (!it[1]) missing << it[0] }

    if (!missing.isEmpty()) {
        throw new RuntimeException("Missing required attributes: ${missing.join(', ')}")
    }

    Path dpxMetaDir = Paths.get(dpxMetaDirStr)
    if (!Files.isDirectory(dpxMetaDir)) {
        throw new RuntimeException("DPX metadata dir not found: ${dpxMetaDir}")
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
    // 1) Parse expected checksums from <pkg>_batch*.xml
    // ------------------------------------------------------------
    def expected = [:]              // filename -> md5
    def expectedSource = [:]        // filename -> batch xml filename
    def duplicateConflicts = []     // same filename, different md5 between batch files

    def batchFiles = []
    Files.newDirectoryStream(dpxMetaDir, "${pkg}_batch*.xml").withCloseable { ds ->
        ds.each { Path p -> if (Files.isRegularFile(p)) batchFiles << p }
    }
    batchFiles = batchFiles.sort { it.fileName.toString() }

    if (batchFiles.isEmpty()) {
        throw new RuntimeException("No ${pkg}_batch*.xml found in: ${dpxMetaDir}")
    }

    def slurper = new XmlSlurper(false, false)

    batchFiles.each { Path xmlPath ->
        def xml = slurper.parse(xmlPath.toFile())
        def filesNode = xml.files
        if (!filesNode || filesNode.size() == 0) return

        filesNode.file.each { f ->
            def name = (f.@name?.toString() ?: "").trim()
            def md5  = (f.@md5?.toString()  ?: "").trim()
            if (!name || !md5) return

            def existing = expected[name]
            if (existing == null) {
                expected[name] = md5
                expectedSource[name] = xmlPath.fileName.toString()
            } else if (existing != md5) {
                duplicateConflicts << [
                    file       : name,
                    md5_first  : existing,
                    first_from : expectedSource[name],
                    md5_next   : md5,
                    next_from  : xmlPath.fileName.toString()
                ]
            }
        }
    }

    if (expected.isEmpty()) {
        throw new RuntimeException("No expected DPX checksums found in ${pkg}_batch*.xml files in: ${dpxMetaDir}")
    }

    // ------------------------------------------------------------
    // 2) Build index of DPX files on disk (PRE overrides if same name)
    // ------------------------------------------------------------
    def actualIndex = [:] // filename -> absolute Path

    if (hasPreDir) {
        Files.newDirectoryStream(dpxPreDir, "*.dpx").withCloseable { ds ->
            ds.each { Path p ->
                if (Files.isRegularFile(p)) actualIndex[p.fileName.toString()] = p.toAbsolutePath()
            }
        }
    }

    Files.newDirectoryStream(dpxDir, "*.dpx").withCloseable { ds ->
        ds.each { Path p ->
            if (Files.isRegularFile(p)) actualIndex[p.fileName.toString()] = p.toAbsolutePath()
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
            mismatches << [ file: fname, reason: "Missing on disk (expected from batch checksum metadata)" ]
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

            if (!actual || actual != expMd5) {
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
            unexpected << [ file: fname, reason: "Present on disk but not referenced in batch checksum metadata" ]
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
    ff = session.putAttribute(ff, "checksum.batch.files", batchFiles.collect { it.fileName.toString() }.join(","))

    // ------------------------------------------------------------
    // Route + event attributes
    // ------------------------------------------------------------
    if (status == "OK") {
        def endIsoUtc = Instant.ofEpochMilli(chkEnd)
                .atOffset(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.SECONDS)
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

        ff = session.putAttribute(ff, "event.datetime", endIsoUtc)
        ff = session.putAttribute(ff, "event.type", "fixity check")
        ff = session.putAttribute(ff, "event.detail", "Fixity check after transfer: computed MD5 checksums for DPX files in the staging area and compared them to MD5 values recorded in SAM-FS archival storage metadata to confirm bit-level integrity.")
        ff = session.putAttribute(ff, "event.outcome", "success")

        session.transfer(ff, REL_SUCCESS)
    } else {
        log.error("DPX message digest validation failed for ${pkg}: mismatches=${mismatches.size()}, conflicts=${duplicateConflicts.size()}, unexpected=${unexpected.size()}")
        session.transfer(ff, REL_FAILURE)
    }

} catch (Exception e) {
    long chkEnd = System.currentTimeMillis()
    long durationMs = chkEnd - chkStart

    ff = session.putAttribute(ff, "checksum.end", chkEnd.toString())
    ff = session.putAttribute(ff, "checksum.durationMs", durationMs.toString())
    ff = session.putAttribute(ff, "checksum.status", "ERROR")

    def pkgErr = getAttr("package.name") ?: "UNKNOWN"
    log.error("Checksum validation error for ${pkgErr}: ${e.message}", e)
    session.transfer(ff, REL_FAILURE)
}
