import org.apache.nifi.processor.io.StreamCallback

import java.nio.file.*
import java.nio.charset.StandardCharsets
import static java.nio.file.StandardOpenOption.*
import java.nio.file.StandardCopyOption

def ff = session.get()
if (!ff) return

try {
    // ------------------------------------------------------------
    // Read required attributes
    // ------------------------------------------------------------
    def tarPath   = ff.getAttribute("tar.path")
    def dpxDir    = ff.getAttribute("dpx.unpack.dir")
    def dpxDirPre = ff.getAttribute("dpx.unpack.dir.pre")
    def workDir   = ff.getAttribute("work.dir")

    if (!tarPath || !dpxDir || !workDir) {
        throw new RuntimeException("Missing required attributes (tar.path / dpx.unpack.dir / work.dir)")
    }
    if (!dpxDirPre) {
        // If not set yet, fall back to dpxDir
        dpxDirPre = dpxDir
    }

    // ------------------------------------------------------------
    // Decide target based on TAR filename
    // ------------------------------------------------------------
    def tarName = new File(tarPath).getName()
    def isPre = tarName.contains("_pre.tar")
    def targetDir = isPre ? dpxDirPre : dpxDir

    ff = session.putAttribute(ff, "tar.is.pre", String.valueOf(isPre))
    ff = session.putAttribute(ff, "dpx.unpack.dir.effective", targetDir)

    // ------------------------------------------------------------
    // Ensure target directory exists
    // ------------------------------------------------------------
    File unpack = new File(targetDir)
    unpack.mkdirs()
    if (!unpack.exists()) {
        throw new RuntimeException("Failed to create directory: ${targetDir}")
    }

    // ------------------------------------------------------------
    // Marker file
    // ------------------------------------------------------------
    Path marker = Paths.get(workDir.toString(), ".fetch_start")

    String fetchStartValue = null
    if (Files.exists(marker)) {
        fetchStartValue = Files.readString(marker, StandardCharsets.UTF_8).trim()
    }

    // If missing/invalid, initialize ONCE
    if (!(fetchStartValue?.isLong())) {
        long now = System.currentTimeMillis()
        fetchStartValue = now.toString()

        Path tmp = marker.resolveSibling(marker.fileName.toString() + ".tmp")
        Files.writeString(tmp, fetchStartValue, StandardCharsets.UTF_8, CREATE, TRUNCATE_EXISTING, WRITE)
        Files.move(tmp, marker, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    }

    ff = session.putAttribute(ff, "fetch.start", fetchStartValue)

    // ------------------------------------------------------------
    // Build TAR command
    // ------------------------------------------------------------
    List<String> cmd = [
        "bash",
        "-c",
        "tar -xf \"${tarPath}\" -C \"${targetDir}\""
    ].collect { it.toString() }

    def pb = new ProcessBuilder(cmd)
    pb.redirectErrorStream(true)

    def proc = pb.start()
    def output = proc.inputStream.text
    int rc = proc.waitFor()

    if (rc != 0) {
        ff = session.write(ff, { _, out ->
            out.write(("TAR ERROR\nExitCode: ${rc}\nTar: ${tarPath}\nTarget: ${targetDir}\n\n${output}\n").bytes)
        } as StreamCallback)

        ff = session.putAttribute(ff, "tar.exit.code", rc.toString())
        ff = session.putAttribute(ff, "tar.error", "true")
        session.transfer(ff, REL_FAILURE)
        return
    }

    ff = session.putAttribute(ff, "tar.exit.code", "0")
    ff = session.putAttribute(ff, "tar.error", "false")
    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, "tar.error", e.message ?: e.toString())
    session.transfer(ff, REL_FAILURE)
}
