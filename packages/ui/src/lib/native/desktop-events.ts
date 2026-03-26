import { invoke } from "@tauri-apps/api/core"
import { listen } from "@tauri-apps/api/event"
import type { WorkspaceEventPayload } from "../../../../server/src/api-types"
import type {
  DesktopEventActiveSessionTarget,
  DesktopEventsStartResult,
  DesktopEventTransportStartOptions,
  DesktopEventTransportStatusPayload,
} from "../event-transport-contract"
import type { WorkspaceEventConnection, WorkspaceEventTransportCallbacks } from "../event-transport"
import { getLogger } from "../logger"

const log = getLogger("sse")

interface WorkspaceEventBatchPayload {
  generation: number
  sequence: number
  emittedAt: number
  events: WorkspaceEventPayload[]
}

export async function connectTauriWorkspaceEvents(
  callbacks: WorkspaceEventTransportCallbacks,
  options: DesktopEventTransportStartOptions,
): Promise<WorkspaceEventConnection> {
  let closed = false
  let opened = false
  let expectedGeneration: number | null = null
  let terminalErrorRaised = false

  const matchesGeneration = (generation: number) => expectedGeneration === null || generation === expectedGeneration

  const unlistenBatch = await listen<WorkspaceEventBatchPayload>("desktop:event-batch", (event) => {
    if (closed) return
    const payload = event.payload
    if (!payload || !matchesGeneration(payload.generation)) return

    if (!opened) {
      opened = true
      callbacks.onOpen?.()
    }

    const events = payload.events ?? []
    if (events.length === 0) {
      return
    }

    callbacks.onBatch(events)
  })

  const unlistenStatus = await listen<DesktopEventTransportStatusPayload>("desktop:event-stream-status", (event) => {
    if (closed) return
    const payload = event.payload
    if (!payload || !matchesGeneration(payload.generation)) return

    if (payload.state === "connected" && !opened) {
      opened = true
      callbacks.onOpen?.()
    }

    if (payload.state === "unauthorized") {
      log.warn("Native desktop event transport is waiting for authentication", {
        reason: payload.reason,
        reconnectAttempt: payload.reconnectAttempt,
        nextDelayMs: payload.nextDelayMs,
        stats: payload.stats,
      })
    } else if (payload.state === "error") {
      log.warn("Native desktop event transport reported an error", {
        reason: payload.reason,
        reconnectAttempt: payload.reconnectAttempt,
        nextDelayMs: payload.nextDelayMs,
        statusCode: payload.statusCode,
        stats: payload.stats,
      })
    } else if ((payload.state === "disconnected" || payload.state === "stopped") && payload.stats) {
      log.info("Native desktop event transport stats", {
        state: payload.state,
        reconnectAttempt: payload.reconnectAttempt,
        stats: payload.stats,
      })
    }

    if (payload.terminal && !terminalErrorRaised && payload.state !== "stopped") {
      terminalErrorRaised = true
      callbacks.onError?.()
    }
  })

  try {
    const result = await invoke<DesktopEventsStartResult>("desktop_events_start", { request: options })
    if (!result?.started) {
      throw new Error(result?.reason ?? "desktop event transport unavailable")
    }
    expectedGeneration = result.generation ?? null
  } catch (error) {
    unlistenBatch()
    unlistenStatus()
    throw error
  }

  return {
    disconnect() {
      if (closed) {
        return
      }

      closed = true
      unlistenBatch()
      unlistenStatus()
      void invoke("desktop_events_stop").catch((error) => {
        log.warn("Failed to stop native desktop event transport", error)
      })
    },
  }
}

export async function setTauriDesktopActiveSession(target: DesktopEventActiveSessionTarget | null): Promise<void> {
  try {
    await invoke("desktop_events_set_active_session", {
      instanceId: target?.instanceId ?? null,
      sessionId: target?.sessionId ?? null,
    })
  } catch (error) {
    log.warn("Failed to update native desktop active session", error)
  }
}
