def ff = session.get()
if (!ff) return

final String ERROR_STAGE = "finalize.stats"
final int ERROR_DETAILS_MAX = 2048

def capDetails = { s ->
    if (s == null) return null
    String t = s.toString()
    (t.length() > ERROR_DETAILS_MAX) ? t.substring(0, ERROR_DETAILS_MAX) : t
}

def setFailure = { flowFile, String message, String details = null ->
    def out = session.putAttribute(flowFile, "error.stage", ERROR_STAGE)
    out = session.putAttribute(out, "error.message", message ?: "Finalize stats failed")
    if (details != null && details.toString().trim()) {
        out = session.putAttribute(out, "error.details", capDetails(details))
    }
    return out
}

try {
    long pipelineEnd = System.currentTimeMillis()
    ff = session.putAttribute(ff, "total.pipeline.end", pipelineEnd.toString())

    def startStr = ff.getAttribute("total.pipeline.start")
    if (startStr?.trim()) {
        try {
            long pipelineStart = Long.parseLong(startStr.trim())
            long duration = pipelineEnd - pipelineStart
            ff = session.putAttribute(ff, "total.pipeline.duration", duration.toString())
        } catch (Exception ignore) {
            ff = session.putAttribute(ff, "total.pipeline.duration", "")
        }
    }

    // package.size.end = total bytes of the output preservation package (metadata + representations)
    def outputSizeStr = ff.getAttribute("checksums.md5.totalBytes")
    if (outputSizeStr?.trim()) {
        ff = session.putAttribute(ff, "package.size.end", outputSizeStr.trim())
    }

    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    log.error("Finalize stats error for ${ff.getAttribute("package.name") ?: "UNKNOWN"}: ${e.message}", e)
    ff = setFailure(ff, e.message ?: "Finalize stats failed", e.toString())
    session.transfer(ff, REL_FAILURE)
}
