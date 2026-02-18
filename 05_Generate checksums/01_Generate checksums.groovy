import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.Instant

def ff = session.get()
if (!ff) return

final String ALG = "MD5"
final String OUT_FILE_NAME = "checksums.md5"

def isoUtcSecondsFromEpochMs = { long epochMs ->
  Instant.ofEpochMilli(epochMs)
    .atOffset(ZoneOffset.UTC)
    .truncatedTo(ChronoUnit.SECONDS)
    .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
}

def getAttr = { String k ->
  def v = ff.getAttribute(k)
  (v && v.trim()) ? v.trim() : null
}

def fail = { String msg, String outcomeDetail ->
  long nowMs = System.currentTimeMillis()
  ff = session.putAttribute(ff, "checksums.md5.status", "FAIL")
  ff = session.putAttribute(ff, "checksums.md5.error", msg)

  session.transfer(ff, REL_FAILURE)
}

def pkg        = getAttr("package.name")
def workDirStr = getAttr("work.dir")

def missing = []
[
  ["package.name", pkg],
  ["work.dir", workDirStr]
].each { if (!it[1]) missing << it[0] }

if (!missing.isEmpty()) {
  fail("Missing attributes: ${missing.join(', ')}", "algorithm=${ALG};error=Missing required attributes")
  return
}

Path workDir = Paths.get(workDirStr).toAbsolutePath().normalize()
if (!Files.isDirectory(workDir)) {
  fail("work.dir not found: ${workDir}", "algorithm=${ALG};error=work.dir not found")
  return
}

Path outPath = workDir.resolve(OUT_FILE_NAME)
Path tmpOut  = workDir.resolve(OUT_FILE_NAME + ".tmp")

// Include directories under root
List<Path> includeDirs = [
  workDir.resolve("metadata"),
  workDir.resolve("representations")
].findAll { Files.isDirectory(it) }

if (includeDirs.isEmpty()) {
  fail("No include directories found under work.dir (expected metadata/ and representations/).",
       "algorithm=${ALG};error=no include dirs")
  return
}

// Junk filter
final Set<String> JUNK_FILE_NAMES = [
  ".DS_Store",
  "Thumbs.db",
  "desktop.ini"
] as Set

def isJunkPath = { Path p ->
  String name = p.fileName?.toString() ?: ""

  if (JUNK_FILE_NAMES.contains(name)) return true
  if (name.startsWith("._")) return true
  if (name.endsWith("~")) return true
  if (name.endsWith(".swp") || name.endsWith(".swo")) return true
  if (name.endsWith(".tmp")) return true

  return false
}

final Set<String> JUNK_DIR_NAMES = [
  ".Spotlight-V100",
  ".Trashes",
  ".fseventsd",
  "System Volume Information"
] as Set

def md5Hex = { Path p ->
  MessageDigest md = MessageDigest.getInstance(ALG)
  p.withInputStream { is ->
    byte[] buf = new byte[1024 * 1024]
    int r
    while ((r = is.read(buf)) != -1) {
      md.update(buf, 0, r)
    }
  }
  return md.digest().collect { String.format("%02x", it) }.join()
}

long startMs = System.currentTimeMillis()

try {
  List<Path> files = []

  includeDirs.each { base ->
    Files.walkFileTree(base, new SimpleFileVisitor<Path>() {
      @Override
      FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
        String dn = dir.fileName?.toString()
        if (dn && JUNK_DIR_NAMES.contains(dn)) {
          return FileVisitResult.SKIP_SUBTREE
        }
        return FileVisitResult.CONTINUE
      }

      @Override
      FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
        if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE

        Path rel = workDir.relativize(file)
        String relStr = rel.toString().replace("\\", "/")

        if (relStr == OUT_FILE_NAME || relStr == (OUT_FILE_NAME + ".tmp")) return FileVisitResult.CONTINUE
        if (isJunkPath(file)) return FileVisitResult.CONTINUE

        files.add(file)
        return FileVisitResult.CONTINUE
      }
    })
  }

  if (files.isEmpty()) {
    fail("No non-junk files found under metadata/ or representations/.",
         "algorithm=${ALG};error=no files after filtering")
    return
  }

  // Deterministic ordering independent of OS path separator
  files.sort { a, b ->
    workDir.relativize(a).toString().replace("\\","/") <=>
    workDir.relativize(b).toString().replace("\\","/")
  }

  long totalBytes = 0L
  int count = 0
  List<String> outLines = []

  files.each { Path f ->
    long size = Files.size(f)
    totalBytes += size

    String sum = md5Hex(f)
    String relPath = workDir.relativize(f).toString().replace("\\", "/")

    outLines << "${sum} *${relPath}"
    count++
  }

  String content = outLines.join("\n") + "\n"
  Files.write(tmpOut, content.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)

  try {
    Files.move(tmpOut, outPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  } catch (Exception ignore) {
    Files.move(tmpOut, outPath, StandardCopyOption.REPLACE_EXISTING)
  }

  long endMs = System.currentTimeMillis()
  long durationMs = endMs - startMs

  ff = session.putAttribute(ff, "checksums.md5.status", "OK")
  ff = session.putAttribute(ff, "checksums.md5.path", outPath.toString())
  ff = session.putAttribute(ff, "checksums.md5.scope", "metadata+representations")
  ff = session.putAttribute(ff, "checksums.md5.count", String.valueOf(count))
  ff = session.putAttribute(ff, "checksums.md5.totalBytes", String.valueOf(totalBytes))
  ff = session.putAttribute(ff, "checksums.md5.durationMs", String.valueOf(durationMs))

  session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
  long endMs = System.currentTimeMillis()
  long durationMs = endMs - startMs
  def msg = (e.message ?: "unknown").replaceAll(/[\\r\\n]+/, " ").take(300)

  ff = session.putAttribute(ff, "checksums.md5.status", "ERROR")
  ff = session.putAttribute(ff, "checksums.md5.durationMs", String.valueOf(durationMs))
  ff = session.putAttribute(ff, "checksums.md5.error", e.toString())
  
  session.transfer(ff, REL_FAILURE)
}
