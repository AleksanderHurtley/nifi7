import java.nio.file.*
import java.time.OffsetDateTime
import groovy.xml.XmlSlurper

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = "creation.event.extract"
final int ERROR_DETAILS_MAX = 2048

def capDetails = { s ->
    if (s == null) return null
    String t = s.toString()
    (t.length() > ERROR_DETAILS_MAX) ? t.substring(0, ERROR_DETAILS_MAX) : t
}

def setFailure = { flowFile, String message, String details = null ->
    def out = session.putAttribute(flowFile, "error.stage", ERROR_STAGE)
    out = session.putAttribute(out, "error.message", message ?: "Creation event extraction failed")
    if (details != null && details.toString().trim()) {
        out = session.putAttribute(out, "error.details", capDetails(details))
    }
    return out
}

def isBlank = { v -> v == null || v.toString().trim().isEmpty() }

// ------------------------------------------------------------------
// Inputs
// ------------------------------------------------------------------
def deprMetsDirStr = ff.getAttribute('metadata.other.depr_mets.dir')

if (isBlank(deprMetsDirStr)) {
    ff = session.putAttribute(ff, 'creation.event.emit', 'false')
    ff = session.putAttribute(ff, 'creation.event.status', 'SKIPPED')
    ff = session.putAttribute(ff, 'creation.event.reason', 'metadata.other.depr_mets.dir attribute not set')
    session.transfer(ff, REL_SUCCESS)
    return
}

Path deprDir = Paths.get(deprMetsDirStr)
if (!Files.isDirectory(deprDir)) {
    ff = session.putAttribute(ff, 'creation.event.emit', 'false')
    ff = session.putAttribute(ff, 'creation.event.status', 'SKIPPED')
    ff = session.putAttribute(ff, 'creation.event.reason', "Deprecated METS dir not found: ${deprDir}")
    session.transfer(ff, REL_SUCCESS)
    return
}

try {
    // --------------------------------------------------------------
    // Collect METS_*.xml files
    // --------------------------------------------------------------
    def metsFiles = []
    Files.newDirectoryStream(deprDir, "METS_*.xml").each { Path p ->
        if (Files.isRegularFile(p)) metsFiles << p
    }

    if (metsFiles.isEmpty()) {
        ff = session.putAttribute(ff, 'creation.event.emit', 'false')
        ff = session.putAttribute(ff, 'creation.event.status', 'SKIPPED')
        ff = session.putAttribute(ff, 'creation.event.reason', "No METS_*.xml found in ${deprDir}")
        session.transfer(ff, REL_SUCCESS)
        return
    }

    // --------------------------------------------------------------
    // Parse: collect every dateTimeCreated; capture scanner/source info
    // from the first METS that exposes it. XmlSlurper is namespace-aware
    // by default, so node.name() returns the local name only.
    // --------------------------------------------------------------
    def textOrNull = { node ->
        if (node == null) return null
        def s = node.text()?.trim()
        return s ? s : null
    }

    def firstByLocalName = { root, String localName ->
        root.'**'.find { it.name() == localName }
    }

    def allByLocalName = { root, String localName ->
        root.'**'.findAll { it.name() == localName }
    }

    String earliestDt          = null
    String scannerModelName    = null   // e.g. "Scanity V3.2.3"
    String scannerManufacturer = null
    String scannerModelSerialNo= null
    String imageProducer       = null
    String captureDevice       = null
    String sourceXDimValue     = null
    String sourceXDimUnit      = null

    metsFiles.each { Path mets ->
        try {
            def root = new XmlSlurper().parse(mets.toFile())

            allByLocalName(root, 'GeneralCaptureInformation').each { gci ->
                def dt = textOrNull(firstByLocalName(gci, 'dateTimeCreated'))
                if (dt && (earliestDt == null || dt < earliestDt)) earliestDt = dt
                if (imageProducer == null) imageProducer = textOrNull(firstByLocalName(gci, 'imageProducer'))
                if (captureDevice == null) captureDevice = textOrNull(firstByLocalName(gci, 'captureDevice'))
            }

            if (scannerModelName == null) {
                def sc = firstByLocalName(root, 'ScannerCapture')
                if (sc) {
                    scannerManufacturer  = textOrNull(firstByLocalName(sc, 'scannerManufacturer'))
                    scannerModelName     = textOrNull(firstByLocalName(sc, 'scannerModelName'))
                    scannerModelSerialNo = textOrNull(firstByLocalName(sc, 'scannerModelSerialNo'))
                }
            }

            if (sourceXDimValue == null) {
                def sxd = firstByLocalName(root, 'SourceXDimension')
                if (sxd) {
                    sourceXDimValue = textOrNull(firstByLocalName(sxd, 'sourceXDimensionValue'))
                    sourceXDimUnit  = textOrNull(firstByLocalName(sxd, 'sourceXDimensionUnit'))
                }
            }
        } catch (Exception ignore) {
            // Tolerate per-file parse failures; keep scanning the rest.
        }
    }

    // --------------------------------------------------------------
    // Required fields: timestamp + scanner identity
    // --------------------------------------------------------------
    if (isBlank(earliestDt) || isBlank(scannerModelName)) {
        ff = session.putAttribute(ff, 'creation.event.emit', 'false')
        ff = session.putAttribute(ff, 'creation.event.status', 'SKIPPED')
        ff = session.putAttribute(ff, 'creation.event.reason',
            "No mix:GeneralCaptureInformation/dateTimeCreated or mix:ScannerCapture/scannerModelName found in ${metsFiles.size()} METS file(s)")
        session.transfer(ff, REL_SUCCESS)
        return
    }

    // Validate the timestamp is parseable; if not, fail-soft
    try {
        OffsetDateTime.parse(earliestDt)
    } catch (Exception e) {
        ff = session.putAttribute(ff, 'creation.event.emit', 'false')
        ff = session.putAttribute(ff, 'creation.event.status', 'SKIPPED')
        ff = session.putAttribute(ff, 'creation.event.reason', "Unparseable dateTimeCreated: ${earliestDt}")
        session.transfer(ff, REL_SUCCESS)
        return
    }

    // --------------------------------------------------------------
    // Parse "Scanity V3.2.3" -> agentName="Scanity", agentVersion="V3.2.3"
    // --------------------------------------------------------------
    def parts = scannerModelName.trim().split(/\s+/, 2)
    String agentName    = parts[0]
    String agentVersion = (parts.length > 1) ? parts[1] : ""

    // --------------------------------------------------------------
    // Build eventDetail (Norwegian)
    // --------------------------------------------------------------
    String filmGauge = (sourceXDimValue && sourceXDimUnit) ? "${sourceXDimValue} ${sourceXDimUnit}".trim() : null

    def detailParts = []
    detailParts << "Digitalisering av analog${filmGauge ? ' ' + filmGauge : ''} film til DPX-bildesekvenser ved bruk av filmskanner ${scannerModelName}"
    def scannerCtx = []
    if (!isBlank(scannerManufacturer))   scannerCtx << scannerManufacturer
    if (!isBlank(scannerModelSerialNo))  scannerCtx << "serienr. ${scannerModelSerialNo}"
    if (!scannerCtx.isEmpty()) {
        detailParts[0] = detailParts[0] + " (${scannerCtx.join(', ')})"
    }
    String detailHead = detailParts[0] + "."

    def tail = []
    if (!isBlank(imageProducer)) tail << "imageProducer: ${imageProducer}"
    if (!isBlank(captureDevice)) tail << "captureDevice: ${captureDevice}"
    String eventDetail = tail.isEmpty() ? detailHead : "${detailHead} " + tail.join('; ') + "."

    // --------------------------------------------------------------
    // Stage attributes for Add event.groovy
    // --------------------------------------------------------------
    ff = session.putAttribute(ff, 'event.type',     'creation')
    ff = session.putAttribute(ff, 'event.outcome',  'success')
    ff = session.putAttribute(ff, 'event.datetime', earliestDt)
    ff = session.putAttribute(ff, 'event.detail',   eventDetail)
    // event.outcomeDetail intentionally omitted; outcome=success is sufficient.

    ff = session.putAttribute(ff, 'agent.name',    agentName)
    ff = session.putAttribute(ff, 'agent.type',    'hardware')
    if (!isBlank(agentVersion)) ff = session.putAttribute(ff, 'agent.version', agentVersion)
    String agentNotes = isBlank(scannerManufacturer) ?
        "Filmskanner brukt til digitalisering av analog film til DPX-bildesekvenser." :
        "Filmskanner produsert av ${scannerManufacturer}. Brukes til digitalisering av analog film til DPX-bildesekvenser."
    ff = session.putAttribute(ff, 'agent.notes', agentNotes)

    ff = session.putAttribute(ff, 'creation.event.emit', 'true')
    ff = session.putAttribute(ff, 'creation.event.status', 'OK')
    ff = session.putAttribute(ff, 'creation.event.mets.count', metsFiles.size().toString())

    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = setFailure(ff, e.message ?: "Creation event extraction failed", e.toString())
    session.transfer(ff, REL_FAILURE)
}
