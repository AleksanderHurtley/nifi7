def ff = session.get()
if (!ff) return

try {
    // ------------------------------------------------------------
    // READ INPUT ATTRIBUTES
    // ------------------------------------------------------------
    def packageName = ff.getAttribute("package.name")
    def packagePath = ff.getAttribute("package.path")

    if (!packageName || !packagePath) {
        log.error("Missing package.name or package.path attributes")
        session.transfer(ff, REL_FAILURE)
        return
    }

    // ------------------------------------------------------------
    // BUILD PATHS
    // ------------------------------------------------------------
    def workBaseDir = "/fc1/work"
    def workDir     = "${workBaseDir}/${packageName}"

    // Payloads
    def payloadBaseDir         = "/fc1/payloads"
    def payloadDir             = "${payloadBaseDir}/${packageName}"
    def eventsPayloadPath      = "${payloadDir}/events.ndjson"
    def submissionPayloadPath  = "${payloadDir}/submission.json"

    // Transfer
    def transferBaseDir = "/fc1/transfer"
    def transferDir     = "${transferBaseDir}/${packageName}"

    def sourceDir      = packagePath.replaceAll("/+\$", "")   // trim trailing slashes
    def sourceAudioDir = "${sourceDir}/sound"

    // DPX
    def dpxUnpackDir    = "${workDir}/dpx"
    def dpxUnpackDirPre = "${workDir}/dpx_pre"
    def batchesDir      = "${workDir}/batches"
    def batchesDirPre   = "${workDir}/batches_pre"

    // Rep
    def repDir     = "${workDir}/representations/rep-${packageName}"
    def repDataDir = "${repDir}/data"

    def videoFilename   = "${packageName}.mkv"
    def videoOutputPath = "${repDataDir}/${videoFilename}"

    // Metadata
    def metadataExtractDir              = "${workDir}/metadata_extract"
    def metadataOtherDir                = "${repDir}/metadata/other"
    def metadataDescriptiveDir          = "${workDir}/metadata/descriptive"
    def metadataOtherDeprecatedMetsDir  = "${workDir}/metadata/other/deprecated_mets"
    def metadataOtherUnclassifiedDir    = "${workDir}/metadata/other/unclassified"

    // Preservation metadata
    def metadataPreservationDir    = "${repDir}/metadata/preservation"
    def metadataPreservationDpxDir = "${metadataPreservationDir}/dpx"
    def metadataPreservationMkvDir = "${metadataPreservationDir}/mkv"

    // ------------------------------------------------------------
    // SET ATTRIBUTES
    // ------------------------------------------------------------
    ff = session.putAttribute(ff, "work.base.dir", workBaseDir)
    ff = session.putAttribute(ff, "work.dir", workDir)

    // Payload attributes
    ff = session.putAttribute(ff, "payloads.base.dir", payloadBaseDir)
    ff = session.putAttribute(ff, "payloads.dir", payloadDir)
    ff = session.putAttribute(ff, "events.payload.path", eventsPayloadPath)
    ff = session.putAttribute(ff, "submission.payload.path", submissionPayloadPath)

    // Transfer attributes
    ff = session.putAttribute(ff, "transfer.base.dir", transferBaseDir)
    ff = session.putAttribute(ff, "transfer.dir", transferDir)

    ff = session.putAttribute(ff, "package.name", packageName)
    ff = session.putAttribute(ff, "package.path", sourceDir)

    ff = session.putAttribute(ff, "source.dir", sourceDir)
    ff = session.putAttribute(ff, "source.audio.dir", sourceAudioDir)

    ff = session.putAttribute(ff, "dpx.unpack.dir", dpxUnpackDir)
    ff = session.putAttribute(ff, "dpx.unpack.dir.pre", dpxUnpackDirPre)

    ff = session.putAttribute(ff, "batches.dir", batchesDir)
    ff = session.putAttribute(ff, "batches.dir.pre", batchesDirPre)

    ff = session.putAttribute(ff, "rep.dir", repDir)
    ff = session.putAttribute(ff, "rep.data.dir", repDataDir)

    ff = session.putAttribute(ff, "video.filename", videoFilename)
    ff = session.putAttribute(ff, "video.output.path", videoOutputPath)

    ff = session.putAttribute(ff, "metadata.extract.dir", metadataExtractDir)
    ff = session.putAttribute(ff, "metadata.other.dir", metadataOtherDir)
    ff = session.putAttribute(ff, "metadata.descriptive.dir", metadataDescriptiveDir)
    ff = session.putAttribute(ff, "metadata.other.depr_mets.dir", metadataOtherDeprecatedMetsDir)
    ff = session.putAttribute(ff, "metadata.other.unclassified.dir", metadataOtherUnclassifiedDir)

    ff = session.putAttribute(ff, "metadata.preservation.dir", metadataPreservationDir)
    ff = session.putAttribute(ff, "metadata.preservation.dpx.dir", metadataPreservationDpxDir)
    ff = session.putAttribute(ff, "metadata.preservation.mkv.dir", metadataPreservationMkvDir)

    ff = session.putAttribute(ff, "batch.pre.id", "batchPRE")

    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    log.error("Error configuring package paths", e)
    session.transfer(ff, REL_FAILURE)
}
