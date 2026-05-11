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
    // Collect final-reel METS files only (METS_*_NNNN.xml). The pre-production
    // "METS_*_pre.xml" is intentionally excluded — its dateTimeCreated reflects
    // a pre-conformance pass, not the final digitization.
    // --------------------------------------------------------------
    def metsFiles = []
    Files.newDirectoryStream(deprDir, "METS_*_[0-9][0-9][0-9][0-9].xml").each { Path p ->
        if (Files.isRegularFile(p)) metsFiles << p
    }

    if (metsFiles.isEmpty()) {
        ff = session.putAttribute(ff, 'creation.event.emit', 'false')
        ff = session.putAttribute(ff, 'creation.event.status', 'SKIPPED')
        ff = session.putAttribute(ff, 'creation.event.reason', "No METS_*_NNNN.xml (final-reel) found in ${deprDir}")
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

    // mix:SourceXDimension (film gauge) is intentionally NOT read — the value
    // is unreliable in source METS (e.g. recorded as 16 mm when actually 32 mm).
    // mix:captureDevice is also NOT read — the value "still from video" is a
    // MIX vocabulary phrase that confuses Norwegian readers.
    String earliestDt          = null
    String scannerModelName    = null   // e.g. "Scanity V3.2.3"
    String scannerManufacturer = null
    String scannerModelSerialNo= null
    String imageProducer       = null
    long   dpxTotal            = 0L     // sum of er:elementCount across all reels

    metsFiles.each { Path mets ->
        try {
            def root = new XmlSlurper().parse(mets.toFile())

            allByLocalName(root, 'GeneralCaptureInformation').each { gci ->
                def dt = textOrNull(firstByLocalName(gci, 'dateTimeCreated'))
                if (dt && (earliestDt == null || dt < earliestDt)) earliestDt = dt
                if (imageProducer == null) imageProducer = textOrNull(firstByLocalName(gci, 'imageProducer'))
            }

            if (scannerModelName == null) {
                def sc = firstByLocalName(root, 'ScannerCapture')
                if (sc) {
                    scannerManufacturer  = textOrNull(firstByLocalName(sc, 'scannerManufacturer'))
                    scannerModelName     = textOrNull(firstByLocalName(sc, 'scannerModelName'))
                    scannerModelSerialNo = textOrNull(firstByLocalName(sc, 'scannerModelSerialNo'))
                }
            }

            // er:elementCount inside er:elementRange — one count per reel.
            def ec = textOrNull(firstByLocalName(root, 'elementCount'))
            if (ec) {
                try { dpxTotal += Long.parseLong(ec) } catch (Exception ignore) {}
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
    // Parse "Scanity V3.2.3" -> agentName="Scanity", agentVersion="V3.2.3".
    // The physical-unit serial is appended to agentVersion so the agent block
    // uniquely identifies which specific scanner did the work:
    //   "V3.2.3 (serienr. 124)"
    // --------------------------------------------------------------
    def parts = scannerModelName.trim().split(/\s+/, 2)
    String agentName       = parts[0]
    String firmwareVersion = (parts.length > 1) ? parts[1] : ""

    String agentVersion
    if (!isBlank(firmwareVersion) && !isBlank(scannerModelSerialNo)) {
        agentVersion = "${firmwareVersion} (serienr. ${scannerModelSerialNo})"
    } else if (!isBlank(firmwareVersion)) {
        agentVersion = firmwareVersion
    } else if (!isBlank(scannerModelSerialNo)) {
        agentVersion = "serienr. ${scannerModelSerialNo}"
    } else {
        agentVersion = ""
    }

    // --------------------------------------------------------------
    // Build eventDetail (Norwegian). Scanner identity (model + version + serial)
    // is fully captured in the agent block, so eventDetail carries only the
    // per-event facts: operation and producer.
    // --------------------------------------------------------------
    def detailParts = ["Digitalisering av analog film til DPX-bildesekvenser."]
    if (!isBlank(imageProducer)) detailParts << "Produsent: ${imageProducer}."
    String eventDetail = detailParts.join(" ")

    // outcomeDetail: total DPX file count summed across all reels' METS files.
    String outcomeDetail = (dpxTotal > 0) ? "Resulterte i ${dpxTotal} DPX-filer." : null

    // --------------------------------------------------------------
    // Stage attributes for Add event.groovy
    // --------------------------------------------------------------
    ff = session.putAttribute(ff, 'event.type',     'creation')
    ff = session.putAttribute(ff, 'event.outcome',  'success')
    ff = session.putAttribute(ff, 'event.datetime', earliestDt)
    ff = session.putAttribute(ff, 'event.detail',   eventDetail)
    if (outcomeDetail) ff = session.putAttribute(ff, 'event.outcomeDetail', outcomeDetail)

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
