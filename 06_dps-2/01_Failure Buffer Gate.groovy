import org.apache.nifi.components.state.Scope

def ff = session.get()
if (!ff) return

// Switch to Scope.CLUSTER if this flow runs on a NiFi cluster.
final Scope STATE_SCOPE = Scope.LOCAL

def getAttr = { String k ->
    def v = ff.getAttribute(k)
    (v && v.trim()) ? v.trim() : null
}

try {
    def pkg = getAttr("package.name")
    if (!pkg) {
        ff = session.putAttribute(ff, "buffer.gate.status", "FAIL")
        ff = session.putAttribute(ff, "buffer.gate.error", "Missing required attribute: package.name")
        session.transfer(ff, REL_FAILURE)
        return
    }

    int capacity = 2
    def capAttr = getAttr("error.buffer.capacity")
    if (capAttr?.isInteger()) {
        capacity = Math.max(1, capAttr.toInteger())
    }

    def sm = context.stateManager
    def stateMap = sm.getState(STATE_SCOPE)
    def state = new HashMap<String, String>(stateMap.toMap())

    String existingSlot = null
    (1..capacity).each { i ->
        if (pkg == state["delivery.error.slot.${i}"]) {
            existingSlot = i.toString()
        }
    }

    String action
    String reason
    String slot = null

    if (existingSlot != null) {
        action = "retain"
        slot = existingSlot
        reason = "Package already retained in slot ${slot}"
    } else {
        String freeSlot = null
        (1..capacity).each { i ->
            if (!state["delivery.error.slot.${i}"]) {
                freeSlot = i.toString()
            }
        }

        if (freeSlot != null) {
            state["delivery.error.slot.${freeSlot}"] = pkg
            sm.setState(state, STATE_SCOPE)
            action = "retain"
            slot = freeSlot
            reason = "Retained in slot ${slot}/${capacity}"
        } else {
            action = "autocleanup"
            reason = "Failure margin full (${capacity} retained); package will be auto-cleaned"
        }
    }

    int count = 0
    (1..capacity).each { i ->
        if (state["delivery.error.slot.${i}"]) count++
    }

    ff = session.putAttribute(ff, "error.buffer.action", action)
    ff = session.putAttribute(ff, "error.buffer.reason", reason)
    ff = session.putAttribute(ff, "error.buffer.count", count.toString())
    ff = session.putAttribute(ff, "error.autocleaned", (action == "autocleanup").toString())

    if (slot != null) {
        ff = session.putAttribute(ff, "error.buffer.slot", slot)
    } else {
        ff = session.removeAttribute(ff, "error.buffer.slot")
    }

    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, "buffer.gate.status", "FAIL")
    ff = session.putAttribute(ff, "buffer.gate.error", e.toString())
    session.transfer(ff, REL_FAILURE)
}
