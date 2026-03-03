// Deprecated: use 01_Failure Buffer Gate.groovy with error.buffer.op=release
// in the same ExecuteScript processor instance used for acquire.

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
        ff = session.putAttribute(ff, "buffer.release.status", "FAIL")
        ff = session.putAttribute(ff, "buffer.release.error", "Missing required attribute: package.name")
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
    ff = session.putAttribute(ff, "buffer.release.status", "OK")
    session.transfer(ff, REL_SUCCESS)

} catch (Exception e) {
    ff = session.putAttribute(ff, "buffer.release.status", "FAIL")
    ff = session.putAttribute(ff, "buffer.release.error", e.toString())
    session.transfer(ff, REL_FAILURE)
}
