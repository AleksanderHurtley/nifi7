import org.apache.nifi.processor.io.StreamCallback

import java.nio.file.*
import java.io.File
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = "fetch.metrics"
final int ERROR_DETAILS_MAX = 2048

def capDetails = { s ->
    if (s == null) return null
    String t = s.toString()
    (t.length() > ERROR_DETAILS_MAX) ? t.substring(0, ERROR_DETAILS_MAX) : t
}

def setFailure = { flowFile, String message, String details = null ->
    def out = session.putAttribute(flowFile, "error.stage", ERROR_STAGE)
    out = session.putAttribute(out, "error.message", message ?: "Fetch metrics calculation failed")
    if (details != null && details.toString().trim()) {
        out = session.putAttribute(out, "error.details", capDetails(details))
    }
    return out
}

try {
    // ------------------------------------------------------------
    // FETCH END AND DURATION
    // ------------------------------------------------------------
    long end = System.currentTimeMillis()
    ff = session.putAttribute(ff, "fetch.end", end.toString())

    def startAttr = ff.getAttribute("fetch.start")
    if (!(startAttr?.isLong())) {
        throw new RuntimeException("Missing or invalid fetch.start: '${startAttr}'")
    }

    long start = startAttr as long
    long duration = end - start
    if (duration < 0) {
        throw new RuntimeException("Negative fetch.duration: start=${start} end=${end}")
    }
    ff = session.putAttribute(ff, "fetch.duration", duration.toString())

    // event.datetime derived from end (UTC, seconds precision)
    String eventDt = Instant.ofEpochMilli(end)
        .atOffset(ZoneOffset.UTC)
        .truncatedTo(ChronoUnit.SECONDS)
        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

    ff = session.putAttribute(ff, "event.datetime", eventDt)

    // ------------------------------------------------------------
    // PACKAGE SIZE START
    // ------------------------------------------------------------
    def workDir = ff.getAttribute("work.dir")
    def dpxDir  = ff.getAttribute("dpx.unpack.dir")

    if (!dpxDir) {
        throw new RuntimeException("Missing dpx.unpack.dir")
    }

    Path dpxPath = Paths.get(dpxDir)

    long sizeBytes = 0L
    Files.walk(dpxPath).withCloseable { s ->
        sizeBytes = s
            .filter { Files.isRegularFile(it) }
            .mapToLong { Files.size(it) }
            .sum()
    }

    ff = session.putAttribute(ff, "package.size.start", sizeBytes.toString())

    // ------------------------------------------------------------
    // CLEAN UP MARKER FILE (.fetch_start)
    // ------------------------------------------------------------
    if (workDir) {
        File marker = new File(workDir, ".fetch_start")
        if (marker.exists()) {
            marker.delete()
        }
    }

    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {

    log.error("Fetch duration/size calculation failed: " + e.message, e)

    ff = session.putAttribute(ff, "fetch.duration", "-1")
    ff = session.putAttribute(ff, "package.size.start", "-1")
    ff = session.putAttribute(ff, "fetch.error", e.message)
    ff = setFailure(ff, e.message ?: "Fetch metrics calculation failed", e.toString())

    session.transfer(ff, REL_FAILURE)
}
