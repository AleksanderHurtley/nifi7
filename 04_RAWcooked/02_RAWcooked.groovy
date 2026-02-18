import groovy.json.JsonSlurper
import java.nio.file.*
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

def ff = session.get()
if (!ff) return

final String FFMPEG  = "/opt/nb-ffmpeg/bin/ffmpeg"
final String FFPROBE = "/opt/nb-ffmpeg/bin/ffprobe"

def getAttr = { String k ->
    def v = ff.getAttribute(k)
    (v && v.trim()) ? v.trim() : null
}

def runFirstLine = { List<String> cmd, Map<String,String> extraEnv = [:] ->
    try {
        def pb = new ProcessBuilder(cmd)
        pb.redirectErrorStream(true)
        if (extraEnv) pb.environment().putAll(extraEnv)
        def p = pb.start()
        def out = p.inputStream.getText("UTF-8")
        p.waitFor()
        def first = (out ?: "").readLines().find { it?.trim() }?.trim()
        return first ? (first.length() > 400 ? first.take(400) : first) : null
    } catch (Exception ignore) {
        return null
    }
}

def probeMkv = { String mkvPath ->
    try {
        def cmd = [
            FFPROBE,
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "format=format_name,format_long_name:stream=codec_name,codec_long_name,profile",
            "-of", "json",
            mkvPath
        ]
        def pb = new ProcessBuilder(cmd)
        pb.redirectErrorStream(true)
        def p = pb.start()
        def out = p.inputStream.getText("UTF-8")
        p.waitFor()

        def j = new JsonSlurper().parseText(out ?: "{}")

        def fmtName   = j?.format?.format_name ?: null
        def fmtLong   = j?.format?.format_long_name ?: null
        def codec     = (j?.streams && j.streams.size() > 0) ? (j.streams[0]?.codec_name ?: null) : null
        def codecLong = (j?.streams && j.streams.size() > 0) ? (j.streams[0]?.codec_long_name ?: null) : null
        def profile   = (j?.streams && j.streams.size() > 0) ? (j.streams[0]?.profile ?: null) : null

        return [
            format_name: fmtName,
            format_long: fmtLong,
            codec_name : codec,
            codec_long : codecLong,
            profile    : profile
        ]
    } catch (Exception ignore) {
        return null
    }
}

def isoUtcSecondsFromEpochMs = { long epochMs ->
    Instant.ofEpochMilli(epochMs)
        .atOffset(ZoneOffset.UTC)
        .truncatedTo(ChronoUnit.SECONDS)
        .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
}

def buildOutcomeDetail = { Closure adder ->
    def pairs = []
    def addPair = { String k, Object v ->
        if (v == null) return
        String s = v.toString().trim()
        if (!s) return
        pairs << "${k}=${s}"
    }
    adder(addPair)
    return pairs.join(";")
}

// --------------------------------------------------------------------
// Mark RAWcooked start
// --------------------------------------------------------------------
long rawStart = System.currentTimeMillis()
ff = session.putAttribute(ff, "rawcooked.start", rawStart.toString())

try {
    def packageName = getAttr("package.name")
    def batchesDir  = getAttr("batches.dir")
    def repDataDir  = getAttr("rep.data.dir")
    def workDir     = getAttr("work.dir")
    def batchPreId  = getAttr("batch.pre.id")

    def batchCountStr = getAttr("batches.count") ?: "0"
    int batchCount
    try {
        batchCount = Integer.parseInt(batchCountStr)
    } catch (Exception ex) {
        throw new RuntimeException("Invalid batches.count (not an integer): ${batchCountStr}")
    }

    if (!packageName || !batchesDir || !repDataDir || !workDir || batchCount <= 0 || !batchPreId) {
        throw new RuntimeException("Missing required attributes for RAWcooked batch processing.")
    }

    def batchesBase = new File(batchesDir)
    if (!batchesBase.isDirectory()) {
        throw new RuntimeException("Invalid batches.dir: ${batchesDir}")
    }

    // Ensure output dir exists
    Files.createDirectories(Paths.get(repDataDir))
    def repData = new File(repDataDir)
    if (!repData.isDirectory()) {
        throw new RuntimeException("Invalid rep.data.dir: ${repDataDir}")
    }

    // Ensure work dir exists
    Files.createDirectories(Paths.get(workDir))
    def work = new File(workDir)
    if (!work.isDirectory()) {
        throw new RuntimeException("Invalid work.dir: ${workDir}")
    }

    // Tool versions
    def rawcookedVersion = runFirstLine(["rawcooked", "--version"])
    def ffmpegVersion    = runFirstLine([FFMPEG, "-version"])
    def ffprobeVersion   = runFirstLine([FFPROBE, "-version"])

    if (rawcookedVersion) ff = session.putAttribute(ff, "tool.rawcooked.version", rawcookedVersion)
    if (ffmpegVersion)    ff = session.putAttribute(ff, "tool.ffmpeg.version", ffmpegVersion)
    if (ffprobeVersion)   ff = session.putAttribute(ff, "tool.ffprobe.version", ffprobeVersion)

    // List batch folders (PRE + batchNNNN)
    def batchFolders = batchesBase.listFiles({ f ->
        f.isDirectory() && (f.name == batchPreId || f.name ==~ /batch\d{4}/)
    } as FileFilter)

    if (!batchFolders || batchFolders.length == 0) {
        throw new RuntimeException("No batch folders (${batchPreId} or batchNNNN) found under: ${batchesDir}")
    }

    def batchSortKey = { String name ->
        if (name == batchPreId) return -1
        try { return (name.replace("batch", "") as Integer) } catch (Exception ignore) { return Integer.MAX_VALUE }
    }
    batchFolders = batchFolders.sort { a, b -> batchSortKey(a.name) <=> batchSortKey(b.name) }

    long totalInputBytes = 0
    long totalOutputBytes = 0
    def results = []
    def overallProbe = null

    batchFolders.each { batchFolder ->
        def batchName = batchFolder.name

        long inputBytes = 0
        Files.walk(batchFolder.toPath()).withCloseable { s ->
            inputBytes = s.filter { Files.isRegularFile(it) }
                          .mapToLong { Files.size(it) }
                          .sum()
        }

        def outputName = "${packageName}_${batchName}.mkv"
        def outputPath = new File(repDataDir, outputName).absolutePath

        File logFile = new File(workDir, "rawcooked_${batchName}.log")
        logFile.text = ""  // truncate

        def cmd = [
            "rawcooked",
            "--all",
            "--check",
            "-y",
            "-o", outputPath,
            batchFolder.absolutePath
        ]

        def pb = new ProcessBuilder(cmd)
        pb.environment().put("PATH", "/opt/nb-ffmpeg/bin:" + System.getenv("PATH"))

        // Logfile
        pb.redirectErrorStream(true)
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))

        long batchStart = System.currentTimeMillis()
        def proc = pb.start()
        int rc = proc.waitFor()
        long batchEnd = System.currentTimeMillis()
        long batchDurationMs = batchEnd - batchStart

        if (rc != 0) {
            throw new RuntimeException("RAWcooked failed for ${batchName} (exit=${rc}). Log=${logFile.absolutePath}")
        }

        File outFile = new File(outputPath)
        long outputBytes = outFile.exists() ? outFile.length() : 0
        double ratio = outputBytes > 0 ? (double) inputBytes / (double) outputBytes : 0.0

        if (overallProbe == null && outFile.exists()) {
            overallProbe = probeMkv(outputPath)
            if (overallProbe) {
                if (overallProbe.format_name) ff = session.putAttribute(ff, "rawcooked.output.container.format_name", overallProbe.format_name as String)
                if (overallProbe.format_long) ff = session.putAttribute(ff, "rawcooked.output.container.format_long", overallProbe.format_long as String)
                if (overallProbe.codec_name)  ff = session.putAttribute(ff, "rawcooked.output.video.codec_name", overallProbe.codec_name as String)
                if (overallProbe.codec_long)  ff = session.putAttribute(ff, "rawcooked.output.video.codec_long", overallProbe.codec_long as String)
                if (overallProbe.profile)     ff = session.putAttribute(ff, "rawcooked.output.video.profile", overallProbe.profile as String)
            }
        }

        totalInputBytes += inputBytes
        totalOutputBytes += outputBytes

        results << [
            batch        : batchName,
            mkv          : outputName,
            input_bytes  : inputBytes,
            output_bytes : outputBytes,
            duration_ms  : batchDurationMs,
            path         : batchFolder.absolutePath
        ]
    }

    double overallRatio = (totalOutputBytes > 0 ? (double) totalInputBytes / (double) totalOutputBytes : 0.0)

    long rawEnd = System.currentTimeMillis()
    long rawDurationMs = rawEnd - rawStart

    // Stat attributes
    ff = session.putAttribute(ff, "rawcooked.end", rawEnd.toString())
    ff = session.putAttribute(ff, "rawcooked.durationMs", rawDurationMs.toString())
    ff = session.putAttribute(ff, "rawcooked.total.input.bytes", totalInputBytes.toString())
    ff = session.putAttribute(ff, "rawcooked.total.output.bytes", totalOutputBytes.toString())
    ff = session.putAttribute(ff, "rawcooked.total.compression_ratio", overallRatio.toString())
    ff = session.putAttribute(ff, "rawcooked.batches.count", results.size().toString())
    ff = session.putAttribute(ff, "rawcooked.batches.names", results.collect { it.batch }.join(","))
    ff = session.putAttribute(ff, "rawcooked.outputs.names", results.collect { it.mkv }.join(","))
    ff = session.putAttribute(ff, "rawcooked.outputs.count", results.size().toString())

    // ----------------------------------------------------------------
    // Preservation event
    // ----------------------------------------------------------------
    ff = session.putAttribute(ff, "event.type", "migration")
    ff = session.putAttribute(ff, "event.outcome", "success")
    ff = session.putAttribute(ff, "event.datetime", isoUtcSecondsFromEpochMs(rawEnd))

    // Agent = RAWcooked
    ff = session.putAttribute(ff, "agent.name", "RAWcooked")
    ff = session.putAttribute(ff, "agent.type", "software")
    ff = session.putAttribute(ff, "agent.version", rawcookedVersion ?: "")

    ff = session.putAttribute(ff, "event.detail",
        "DPX image sequences converted to FFV1 video wrapped in a Matroska (MKV) container for preservation storage."
    )

    ff = session.putAttribute(ff, "event.outcomeDetail", buildOutcomeDetail { addPair ->
        addPair("command", "rawcooked --all --check -y")
        addPair("tool.ffmpeg", ffmpegVersion)
        addPair("tool.ffprobe", ffprobeVersion)
        addPair("container", overallProbe?.format_long ?: overallProbe?.format_name)
        addPair("videoCodec", overallProbe?.codec_name)
        addPair("videoProfile", overallProbe?.profile)
    })

    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    log.error("RAWcooked error for ${ff.getAttribute("package.name") ?: "UNKNOWN"}: ${e.message}", e)
    session.transfer(ff, REL_FAILURE)
}
