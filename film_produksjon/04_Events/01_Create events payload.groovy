import groovy.json.JsonGenerator
import groovy.json.JsonSlurper
import groovy.xml.XmlSlurper
import java.nio.charset.StandardCharsets
import java.nio.file.*
import java.time.Instant
import java.time.OffsetDateTime
import java.time.temporal.ChronoUnit

def ff = session.get()
if (!ff) return

final String ERROR_STAGE = 'events.payload.build'
Path temporary = null

try {
    def required = { String name ->
        String value = ff.getAttribute(name)?.trim()
        if (!value) throw new IllegalArgumentException("Missing attribute: ${name}")
        value
    }
    def pathAttribute = { String name ->
        Path path = Paths.get(required(name))
        if (!path.isAbsolute()) throw new IllegalArgumentException("${name} must be absolute")
        path.normalize()
    }
    def requireFile = { Path path ->
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Expected a regular file: ${path}")
        }
    }
    def parseDate = { String text, String origin ->
        try {
            OffsetDateTime.parse(text).toInstant()
        } catch (Exception ignored) {
            throw new IllegalArgumentException("${origin} must be an ISO-8601 timestamp with timezone: ${text}")
        }
    }

    String pkg = required('package.name')
    if (!(pkg ==~ /digifilm_\d+_\d{8}/)) throw new IllegalArgumentException('Invalid package.name')
    String filename = "${pkg}.mov"
    Path sip = pathAttribute('transfer.dir')
    Path payloads = pathAttribute('payloads.dir')
    Path output = pathAttribute('events.payload.path')
    // Read the retained legacy METS inside the finished SIP, not the deleted work copy
    // and not the newly generated root METS.xml.
    Path sourceMets = sip.resolve("metadata/descriptive/deprecated_mets/${pkg}.xml")
    Path relativeMov = Paths.get("representations/primary_${pkg[-8..-1]}/data/${filename}")
    String fileRef = relativeMov.toString().replace('\\', '/')
    Path packagedMov = sip.resolve(relativeMov)

    if (sip.fileName.toString() != pkg || output != payloads.resolve('events.ndjson') ||
        payloads.startsWith(sip) || sip.startsWith(payloads)) {
        throw new IllegalArgumentException('Event, source METS, MOV, and SIP paths do not match the produksjon layout')
    }
    requireFile(sourceMets)
    requireFile(sip.resolve('METS.xml'))
    requireFile(packagedMov)
    requireFile(output) // Initialize creates this file; never silently choose a new location.

    if (required('fetch.status') != 'OK' || required('checksum.status') != 'OK' ||
        required('metadata.org.status') != 'OK' || required('metadata.org.review.required') != 'false' ||
        required('checksum.checked.count') != '1' || required('checksum.expected.count') != '1' ||
        required('eark.status') != 'OK') {
        throw new IllegalArgumentException('Successful metadata organization, MOV verification, and SIP creation are required')
    }
    String md5 = required('media.verified.md5')
    String size = required('media.verified.size')
    if (!(md5 ==~ /[a-f0-9]{32}/) || !(size ==~ /\d+/) || size.toLong() <= 0 ||
        md5 != required('media.expected.md5') || size != required('media.expected.size') ||
        required('media.expected.filename') != filename ||
        Files.size(packagedMov) != size.toLong()) {
        throw new IllegalArgumentException('MOV verification attributes or current file sizes are inconsistent')
    }

    // eark.end is captured immediately on EarkSIPGenerator success, not when this queued script runs.
    String earkEnd = required('eark.end')
    Instant sipTime
    try {
        sipTime = (earkEnd ==~ /\d+/)
            ? Instant.ofEpochMilli(earkEnd.toLong())
            : OffsetDateTime.parse(earkEnd).toInstant()
    } catch (Exception ignored) {
        throw new IllegalArgumentException("eark.end must be epoch milliseconds or an ISO-8601 timestamp with timezone: ${earkEnd}")
    }
    Instant transferTime = parseDate(required('transfer.event.datetime'), 'transfer.event.datetime')
    if (sipTime.isAfter(Instant.now()) || transferTime.isAfter(sipTime)) {
        throw new IllegalArgumentException('Transfer and SIP timestamps are out of order or in the future')
    }
    String nifiVersion = required('nifi.agent.version')
    String earkVersion = required('eark.agent.version')

    def mets = new XmlSlurper(false, true).parse(sourceMets.toFile())
    if (mets.name() != 'mets') throw new IOException('Source XML is not METS')
    def mediaFiles = mets.fileSec.depthFirst().findAll { it.name() == 'file' }
    if (mediaFiles.size() != 1) throw new IOException('Expected exactly one source METS media file')
    List<String> adminIds = mediaFiles[0].@ADMID.text().tokenize()
    def objects = mets.amdSec.techMD.findAll { adminIds.contains(it.@ID.text()) }.mdWrap.xmlData.object
    if (objects.size() != 1 || objects[0].originalName.text().trim() != filename) {
        throw new IOException('METS media ADMID must bind to the PREMIS object for this MOV')
    }
    String urn = "URN:NBN:no-nb_${pkg}"
    if (required('media.urn') != urn || mediaFiles[0].FLocat.findAll {
        it.attributes()['{http://www.w3.org/1999/xlink}href'] == urn
    }.size() != 1) throw new IOException('Source METS media identity does not match this package')

    def object = objects[0]
    def sourceMd5 = object.objectCharacteristics.fixity.findAll {
        it.messageDigestAlgorithm.text().trim().equalsIgnoreCase('MD5')
    }
    if (sourceMd5.size() != 1 || sourceMd5[0].messageDigest.text().trim().toLowerCase(Locale.ROOT) != md5 ||
        object.objectCharacteristics.size.text().trim() != size) {
        throw new IOException('Source METS no longer matches the verified MOV checksum and size')
    }
    def applications = object.creatingApplication
    if (applications.size() != 1 || !applications[0].creatingApplicationName.text().trim().equalsIgnoreCase('FFmpeg')) {
        throw new IOException('Expected one FFmpeg creatingApplication for the MOV; inspect unfamiliar historical metadata')
    }
    Instant creationTime = parseDate(applications[0].dateCreatedByApplication.text().trim(), 'PREMIS creation date')
    Set<String> objectIds = object.objectIdentifier.objectIdentifierValue.collect { it.text().trim() } as Set
    Set<String> eventIds = object.linkingEventIdentifier.linkingEventIdentifierValue.collect { it.text().trim() } as Set
    def historicalEvents = mets.amdSec.digiprovMD.mdWrap.xmlData.event.findAll { event ->
        eventIds.contains(event.eventIdentifier.eventIdentifierValue.text().trim()) &&
            event.linkingObjectIdentifier.linkingObjectIdentifierValue.any { objectIds.contains(it.text().trim()) } &&
            ['creation', 'derivation'].contains(event.eventType.text().trim().toLowerCase(Locale.ROOT))
    }
    if (historicalEvents.size() != 1) throw new IOException('Expected one linked historical creation/derivation event for this MOV')
    def historical = historicalEvents[0]
    if (parseDate(historical.eventDateTime.text().trim(), 'PREMIS event date') != creationTime ||
        !['success', 'successful', 'succeeded'].contains(historical.eventOutcomeInformation.eventOutcome.text().trim().toLowerCase(Locale.ROOT)) ||
        creationTime.isAfter(transferTime)) {
        throw new IOException('Historical creation date/outcome is missing, conflicting, or later than transfer')
    }

    // Match the published SubmitEventDto and bevaring uploader: flat fields, singular agentNote.
    // packageId is local bookkeeping and must be removed before each API POST.
    String historicalType = historical.eventType.text().trim()
    String recordedVersion = applications[0].creatingApplicationVersion.text().trim()
    def creationAgent = [agentName: 'FFmpeg', agentType: 'software',
        agentNote: 'Programvare for behandling og produksjon av lyd- og videofiler.']
    // A Lavf version identifies libavformat, not the complete FFmpeg build.
    if (recordedVersion) {
        def libraryVersion = recordedVersion =~ /(?i)Lavf(\d+\.\d+\.\d+)/
        if (libraryVersion.matches()) {
            creationAgent.agentVersion = "libavformat ${libraryVersion.group(1)} (FFmpeg versjon ikke dokumentert)".toString()
        } else if (recordedVersion.toLowerCase(Locale.ROOT).startsWith('lavf')) {
            creationAgent.agentVersion = "${recordedVersion} (FFmpeg versjon ikke dokumentert)".toString()
        } else {
            creationAgent.agentVersion = recordedVersion
        }
    }
    String historicalLabel = historicalType.equalsIgnoreCase('derivation') ? 'derivat' : 'opprettelse'
    String creationDetail = "Produksjonsfilen ${filename} ble opprettet med FFmpeg. Denne hendelsen er dokumentert i ettertid på grunnlag av de opprinnelige METS-metadataene. Opprinnelig hendelsestype: ${historicalLabel}"
    def records = [
        [packageId: pkg, agent: creationAgent,
         eventDateTime: creationTime.toString(), eventType: 'creation',
         eventDetail: creationDetail, fileRef: [relativePath: fileRef],
         outcome: 'success'],
        [packageId: pkg, agent: [agentName: 'Apache NiFi', agentType: 'software', agentVersion: nifiVersion,
            agentNote: 'Programvare for automatisering av dataflyter som muliggjør utforming og administrasjon av komplekse datapipelines.'],
         eventDateTime: transferTime.toString(), eventType: 'transfer',
         eventDetail: 'Pakke hentet fra Oracle HSM (SAM-FS) til lokalt arbeidsområde for rearkivering. MOV-filen ble kopiert uendret; filstørrelse og MD5-sjekksum ble kontrollert mot verdiene i opprinnelig METS-metadata med Java MessageDigest.',
         outcome: 'success', outcomeDetail: "Verifisert MOV: ${fileRef}; MD5=${md5}; størrelse=${size} byte."],
        [packageId: pkg, agent: [agentName: 'nifi-nb-eark-nar.EarkSIPGenerator', agentType: 'software', agentVersion: earkVersion,
            agentNote: 'Apache NiFi-prosessor utviklet av Nasjonalbiblioteket for opprettelse av E-ARK SIP. Bruker commons-ip, et Java-bibliotek for opprettelse og håndtering av E-ARK-informasjonspakker.'],
         eventDateTime: sipTime.truncatedTo(ChronoUnit.SECONDS).toString(), eventType: 'information package creation',
         eventDetail: 'Opprettet SIP i henhold til Nasjonalbibliotekets profil SIP 1.0.', outcome: 'success']
    ]

    // This builder owns these three records, not arbitrary existing history or another package's payload.
    Set<String> expectedTypes = records.collect { it.eventType } as Set
    Set<String> seenTypes = [] as Set
    Files.readAllLines(output, StandardCharsets.UTF_8).findAll { it.trim() }.each { line ->
        def existing = new JsonSlurper().parseText(line)
        if (!(existing instanceof Map) || existing.packageId != pkg || !expectedTypes.contains(existing.eventType) ||
            !seenTypes.add(existing.eventType)) {
            throw new IOException('Refusing to replace an events file containing unrelated or duplicate records')
        }
    }
    def json = new JsonGenerator.Options().disableUnicodeEscaping().build()
    String content = records.collect { json.toJson(it) }.join('\n') + '\n'
    temporary = Files.createTempFile(payloads, '.events-', '.tmp')
    Files.writeString(temporary, content, StandardCharsets.UTF_8)
    Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
    temporary = null
    ff = session.putAllAttributes(ff, [
        'events.payload.status': 'OK', 'events.payload.count': '3',
        'events.source.mets.path': sourceMets.toString(),
        'creation.event.datetime': creationTime.toString(),
        'sip.event.datetime': sipTime.truncatedTo(ChronoUnit.SECONDS).toString()
    ])
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    log.error('Produksjon event payload creation failed', e)
    ff = session.putAllAttributes(ff, [
        'events.payload.status': 'FAIL', 'events.payload.error': (e.message ?: 'Event payload creation failed').take(2048),
        'error.stage': ERROR_STAGE, 'error.message': (e.message ?: 'Event payload creation failed').take(2048),
        'error.details': e.toString().take(2048)
    ])
    session.transfer(ff, REL_FAILURE)
} finally {
    if (temporary != null) {
        try { Files.deleteIfExists(temporary) }
        catch (Exception cleanupError) { log.warn("Could not remove event temporary file ${temporary}: ${cleanupError.message}".toString()) }
    }
}
