def ff = session.get()
if (!ff) return

def readIds = { String attribute ->
    String value = ff.getAttribute(attribute)
    value ? value.split(',', -1).collect { it.trim() } : []
}

try {
    String expectedId = ff.getAttribute('catalog.expected.primary.priref')
    List candidates = readIds('catalog.dump.duplicate.candidate.prirefs')
    if (ff.getAttribute('catalog.dump.status') != 'FAIL' ||
        ff.getAttribute('catalog.dump.phase') != 'PRIMARY' ||
        ff.getAttribute('catalog.dump.duplicates.mode') != 'film_produksjon' ||
        ff.getAttribute('catalog.dump.package') != ff.getAttribute('package.name') ||
        (expectedId && !(expectedId ==~ /[0-9]+/)) || !candidates ||
        !candidates.every { it ==~ /[0-9]+/ } ||
        candidates.toSet().size() != candidates.size() || expectedId in candidates) {
        throw new IllegalArgumentException('Expected zero-byte candidates from the current package duplicate report')
    }

    List pending
    List completed
    if (!ff.getAttribute('catalog.delete.attempted')) {
        pending = candidates
        completed = []
    } else {
        pending = readIds('catalog.delete.pending.prirefs')
        completed = readIds('catalog.delete.completed.prirefs')
        String current = ff.getAttribute('catalog.delete.priref')
        if (ff.getAttribute('catalog.delete.attempted') != 'true' ||
            ff.getAttribute('catalog.delete.status') != 'DELETE' ||
            !pending || pending.first() != current ||
            completed + pending != candidates) {
            throw new IllegalArgumentException('Invalid duplicate deletion queue')
        }
        String statusCode = ff.getAttribute('invokehttp.status.code')
        if (statusCode != '204') {
            throw new IOException("Deletion of priref ${current} expected HTTP 204, got ${statusCode ?: 'no status'}")
        }
        completed.add(pending.remove(0))
    }

    ff = session.removeAttribute(ff, 'invokehttp.status.code')
    ff = session.putAllAttributes(ff, [
        'catalog.delete.attempted': 'true',
        'catalog.delete.status': pending ? 'DELETE' : 'COMPLETE',
        'catalog.delete.pending.prirefs': pending.join(','),
        'catalog.delete.completed.prirefs': completed.join(',')
    ])
    if (pending) {
        ff = session.putAttribute(ff, 'catalog.delete.priref', pending.first())
    } else {
        ff = session.removeAttribute(ff, 'catalog.delete.priref')
    }
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    ff = session.putAllAttributes(ff, [
        'error.stage': 'catalog.delete.advance',
        'error.message': (e.message ?: e.toString()).take(2048)
    ])
    log.error('Advancing catalog duplicate deletion failed', e)
    session.transfer(ff, REL_FAILURE)
}
