import org.apache.nifi.components.state.Scope

def ff = session.get()
if (!ff) return

final Scope STATE_SCOPE = Scope.LOCAL

def getAttr = { String k ->
    def v = ff.getAttribute(k)
    (v && v.trim()) ? v.trim() : null
}

def occupiedCount = { Map<String, String> st, int cap ->
    int c = 0
    (1..cap).each { i ->
        if (st["delivery.error.slot.${i}"]) c++
    }
    return c
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

    String op = (getAttr("error.buffer.op") ?: "acquire").toLowerCase()
    if (!(op in ["acquire", "release"])) {
        ff = session.putAttribute(ff, "buffer.gate.status", "FAIL")
        ff = session.putAttribute(ff, "buffer.gate.error", "Invalid error.buffer.op: ${op}. Expected acquire or release")
        session.transfer(ff, REL_FAILURE)
        return
    }

    def sm = context.stateManager
    def stateMap = sm.getState(STATE_SCOPE)
    def state = new HashMap<String, String>(stateMap.toMap())

    if (op == "release") {
        String manualAction = (getAttr("error.buffer.manual_action") ?: "retry").toLowerCase()
        if (!(manualAction in ["retry", "failure"])) {
            ff = session.putAttribute(ff, "buffer.gate.status", "FAIL")
            ff = session.putAttribute(ff, "buffer.gate.error", "Invalid error.buffer.manual_action: ${manualAction}. Expected retry or failure")
            session.transfer(ff, REL_FAILURE)
            return
        }

        def releasedSlots = []
        (1..capacity).each { i ->
            String key = "delivery.error.slot.${i}"
            if (pkg == state[key]) {
                state.remove(key)
                releasedSlots << i.toString()
            }
        }

        if (!releasedSlots.isEmpty()) {
            sm.setState(state, STATE_SCOPE)
        }

        ff = session.putAttribute(ff, "error.buffer.release.released", (!releasedSlots.isEmpty()).toString())
        ff = session.putAttribute(ff, "error.buffer.release.slots", releasedSlots.join(","))
        ff = session.putAttribute(ff, "error.buffer.count", occupiedCount(state, capacity).toString())
        ff = session.putAttribute(ff, "error.buffer.route", (manualAction == "retry") ? "retry_ready" : "failure_ready")
        ff = session.putAttribute(ff, "buffer.gate.status", "OK")
        ff = session.removeAttribute(ff, "buffer.gate.error")

        ff = session.removeAttribute(ff, "error.buffer.action")
        ff = session.removeAttribute(ff, "error.buffer.reason")
        ff = session.removeAttribute(ff, "error.buffer.slot")
        ff = session.removeAttribute(ff, "error.autocleaned")

        session.transfer(ff, REL_SUCCESS)
        return
    }

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

    int count = occupiedCount(state, capacity)

    ff = session.putAttribute(ff, "error.buffer.action", action)
    ff = session.putAttribute(ff, "error.buffer.reason", reason)
    ff = session.putAttribute(ff, "error.buffer.count", count.toString())
    ff = session.putAttribute(ff, "error.autocleaned", (action == "autocleanup").toString())
    ff = session.putAttribute(ff, "buffer.gate.status", "OK")
    ff = session.removeAttribute(ff, "buffer.gate.error")
    ff = session.removeAttribute(ff, "error.buffer.release.released")
    ff = session.removeAttribute(ff, "error.buffer.release.slots")
    ff = session.removeAttribute(ff, "error.buffer.route")

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
