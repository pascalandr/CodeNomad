import type { WorkspaceEventPayload } from "../../../server/src/api-types"
import { serverApi } from "./api-client"
import {
  resolveDesktopEventTransportStartOptions,
  type DesktopEventTransportStartOptions,
} from "./event-transport-contract"
import { getLogger } from "./logger"
import { runtimeEnv } from "./runtime-env"
import { connectTauriWorkspaceEvents } from "./native/desktop-events"

const log = getLogger("sse")

export interface WorkspaceEventTransportCallbacks {
  onBatch: (events: WorkspaceEventPayload[]) => void
  onError?: () => void
  onOpen?: () => void
}

export interface WorkspaceEventConnection {
  disconnect: () => void
}

async function connectBrowserWorkspaceEvents(
  callbacks: WorkspaceEventTransportCallbacks,
): Promise<WorkspaceEventConnection> {
  const source = serverApi.connectEvents((event) => {
    callbacks.onBatch([event])
  }, callbacks.onError)
  source.onopen = () => callbacks.onOpen?.()
  return {
    disconnect() {
      source.close()
    },
  }
}

export async function connectWorkspaceEvents(
  callbacks: WorkspaceEventTransportCallbacks,
  options?: DesktopEventTransportStartOptions,
): Promise<WorkspaceEventConnection> {
  if (runtimeEnv.host === "tauri") {
    try {
      return await connectTauriWorkspaceEvents(
        callbacks,
        resolveDesktopEventTransportStartOptions(options),
      )
    } catch (error) {
      log.warn("Failed to start native desktop event transport, falling back to browser EventSource", error)
    }
  }

  return connectBrowserWorkspaceEvents(callbacks)
}

export type {
  DesktopEventsStartResult,
  DesktopEventTransportReconnectPolicy,
  DesktopEventTransportStartOptions,
  DesktopEventTransportState,
  DesktopEventTransportStatusPayload,
} from "./event-transport-contract"
