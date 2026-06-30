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

def getAttr = { String k ->
    def v = ff.getAttribute(k)
    (v && v.trim()) ? v.trim() : null
}

try {
    // ----------------------------------------------------------------
    // 1) Compute pipeline-total timings
    // ----------------------------------------------------------------
    long pipelineEnd = System.currentTimeMillis()
    ff = session.putAttribute(ff, "total.pipeline.end", pipelineEnd.toString())

    def startStr = getAttr("total.pipeline.start")
    if (startStr) {
        try {
            long pipelineStart = Long.parseLong(startStr)
            ff = session.putAttribute(ff, "total.pipeline.duration", (pipelineEnd - pipelineStart).toString())
        } catch (Exception ignore) {
            ff = session.putAttribute(ff, "total.pipeline.duration", "")
        }
    }

    // ----------------------------------------------------------------
    // 2) Derive package.size.end from checksum output total
    // ----------------------------------------------------------------
    def outputSizeStr = getAttr("checksums.md5.totalBytes")
    if (outputSizeStr) {
        ff = session.putAttribute(ff, "package.size.end", outputSizeStr)
    }

    // ----------------------------------------------------------------
    // 3) Validate all attributes that the PutSQL will write to the DB
    //
    //    REQUIRED_NUMERIC — set by pipeline scripts in this repo;
    //                       must be present and parseable as a number
    //    OPTIONAL_PRESENT — set outside this repo (eark.*);
    //                       log a warning if missing but do not fail
    // ----------------------------------------------------------------
    final List<String> REQUIRED_NUMERIC = [
        "fetch.start", "fetch.end", "fetch.duration",
        "checksum.start", "checksum.end", "checksum.durationMs",
        "rawcooked.start", "rawcooked.end", "rawcooked.durationMs",
        "rawcooked.total.input.bytes", "rawcooked.total.output.bytes",
        "rawcooked.total.compression_ratio",
        "total.pipeline.start", "total.pipeline.end", "total.pipeline.duration",
        "package.size.start", "package.size.end"
    ]

    final List<String> OPTIONAL_PRESENT = [
        "eark.start", "eark.end", "eark.duration"
    ]

    def missing = []
    def invalid = []
    def warnings = []

    REQUIRED_NUMERIC.each { attr ->
        def v = getAttr(attr)
        if (!v) {
            missing << attr
        } else {
            try {
                new BigDecimal(v)
            } catch (Exception ignore) {
                invalid << "${attr}=${v}"
            }
        }
    }

    OPTIONAL_PRESENT.each { attr ->
        if (!getAttr(attr)) {
            warnings << attr
        }
    }

    if (warnings) {
        log.warn("[${ff.getAttribute("package.name") ?: "UNKNOWN"}] finalize.stats: optional attributes missing (set outside repo): ${warnings.join(', ')}")
    }

    if (missing || invalid) {
        def parts = []
        if (missing) parts << "missing=[${missing.join(', ')}]"
        if (invalid) parts << "non-numeric=[${invalid.join('; ')}]"
        ff = setFailure(ff, "Stats validation failed: ${parts.join('; ')}")
        session.transfer(ff, REL_FAILURE)
        return
    }

    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    log.error("Finalize stats error for ${ff.getAttribute("package.name") ?: "UNKNOWN"}: ${e.message}", e)
    ff = setFailure(ff, e.message ?: "Finalize stats failed", e.toString())
    session.transfer(ff, REL_FAILURE)
}
