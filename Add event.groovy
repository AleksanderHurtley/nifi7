import java.nio.file.*
import java.nio.file.StandardOpenOption
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

def ff = session.get()
if (!ff) return

def isBlank = { v -> v == null || v.toString().trim().isEmpty() }

// ----------------------------------------------------------------------
// Attributes
// ----------------------------------------------------------------------
def eventsPathStr   = ff.getAttribute('events.payload.path')
def pkg             = ff.getAttribute('package.name')

def eventType       = ff.getAttribute('event.type')
def eventOutcomeRaw = ff.getAttribute('event.outcome')
def eventDetail     = ff.getAttribute('event.detail') ?: ""
def outcomeDetail   = ff.getAttribute('event.outcomeDetail') ?: ""

// Normalize outcome to "success"/"failure" where possible
def normalizeOutcome = { String o ->
    if (isBlank(o)) return "success"
    def x = o.trim().toLowerCase()
    if (x == "successful" || x == "succeeded") return "success"
    if (x == "failed" || x == "error") return "failure"
    return o.trim()
}
def eventOutcome = normalizeOutcome(eventOutcomeRaw)

// ----------------------------------------------------------------------
// Timestamp
// ----------------------------------------------------------------------
def providedDt = ff.getAttribute('event.datetime')
def eventDt = (!isBlank(providedDt))
        ? providedDt.trim()
        : OffsetDateTime.now(ZoneOffset.UTC)
            .truncatedTo(ChronoUnit.SECONDS)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

// ----------------------------------------------------------------------
// Agent defaults (flow identity) + optional agentVersion
// ----------------------------------------------------------------------
def agentName    = ff.getAttribute('agent.name') ?: "NiFi preservation ingest flow"
def agentType    = ff.getAttribute('agent.type') ?: "software"
def agentVersion = ff.getAttribute('agent.version') ?: "2.2.0"

// ----------------------------------------------------------------------
// Validate inputs
// ----------------------------------------------------------------------
def missing = []
[['events.payload.path', eventsPathStr], ['event.type', eventType], ['package.name', pkg]].each { pair ->
    if (isBlank(pair[1])) missing << pair[0]
}

if (!missing.isEmpty()) {
    ff = session.putAttribute(ff, 'events.append.status', 'FAIL')
    ff = session.putAttribute(ff, 'events.append.error', "Missing attributes: ${missing.join(', ')}")
    session.transfer(ff, REL_FAILURE)
    return
}

Path eventsPath = Paths.get(eventsPathStr.trim())

if (!Files.exists(eventsPath)) {
    ff = session.putAttribute(ff, 'events.append.status', 'FAIL')
    ff = session.putAttribute(ff, 'events.append.error', "Events file not found: ${eventsPath}")
    session.transfer(ff, REL_FAILURE)
    return
}

if (!Files.isRegularFile(eventsPath)) {
    ff = session.putAttribute(ff, 'events.append.status', 'FAIL')
    ff = session.putAttribute(ff, 'events.append.error', "Events path is not a file: ${eventsPath}")
    session.transfer(ff, REL_FAILURE)
    return
}

if (!Files.isWritable(eventsPath)) {
    ff = session.putAttribute(ff, 'events.append.status', 'FAIL')
    ff = session.putAttribute(ff, 'events.append.error', "Events file is not writable: ${eventsPath}")
    session.transfer(ff, REL_FAILURE)
    return
}

// ----------------------------------------------------------------------
// JSON helpers
// ----------------------------------------------------------------------
def jsonEscape = { String s ->
    if (s == null) return ""
    s.replace("\\", "\\\\")
     .replace("\"", "\\\"")
     .replace("\r", "\\r")
     .replace("\n", "\\n")
     .replace("\t", "\\t")
}

def jsonField = { String k, String v ->
    "\"${jsonEscape(k)}\":\"${jsonEscape(v)}\""
}

try {
    FileChannel ch = null
    FileLock lock = null
    try {
        ch = FileChannel.open(eventsPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
        lock = ch.lock()

        // Build agent JSON and OMIT agentVersion when blank
        def agentParts = []
        agentParts << jsonField("agentName", agentName)
        agentParts << jsonField("agentType", agentType)
        if (!isBlank(agentVersion)) agentParts << jsonField("agentVersion", agentVersion.trim())
        def agentJson = "{${agentParts.join(",")}}"

        def eventJsonParts = []
        eventJsonParts << jsonField("eventDateTime", eventDt)
        eventJsonParts << jsonField("eventType", eventType.trim())
        if (!isBlank(eventDetail))   eventJsonParts << jsonField("eventDetail", eventDetail.trim())
        eventJsonParts << jsonField("outcome", eventOutcome)
        if (!isBlank(outcomeDetail)) eventJsonParts << jsonField("outcomeDetail", outcomeDetail.trim())
        def eventJson = "{${eventJsonParts.join(",")}}"

        def record =
            "{${jsonField("packageId", pkg.trim())}," +
              "\"agent\":${agentJson}," +
              "\"event\":${eventJson}}" +
              "\n"

        ch.position(ch.size())
        ch.write(ByteBuffer.wrap(record.getBytes(StandardCharsets.UTF_8)))
    } finally {
        try { if (lock != null) lock.release() } catch (ignored) {}
        try { if (ch != null) ch.close() } catch (ignored) {}
    }

    ff = session.putAttribute(ff, 'events.append.status', 'OK')
    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, 'events.append.status', 'FAIL')
    ff = session.putAttribute(ff, 'events.append.error', e.toString())
    session.transfer(ff, REL_FAILURE)
}
