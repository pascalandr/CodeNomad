import { For, Show, createEffect, createMemo, createSignal, onCleanup, on, untrack, type Component, type Accessor } from "solid-js"
import MessagePreview from "./message-preview"
import { messageStoreBus } from "../stores/message-v2/bus"
import type { MessageRecord } from "../stores/message-v2/types"
import { getPartCharCount } from "../lib/token-utils"
import { getToolIcon } from "./tool-call/utils"
import { User as UserIcon, Bot as BotIcon, FoldVertical, ShieldAlert } from "lucide-solid"
import { useI18n } from "../lib/i18n"
import type { DeleteHoverState } from "../types/delete-hover"
import { projectMessageEntries } from "./message-render-projection"

export type TimelineSegmentType = "user" | "assistant" | "tool" | "compaction"

export interface TimelineSegment {
  id: string
  messageId: string
  type: TimelineSegmentType
  label: string
  tooltip: string
  shortLabel?: string
  variant?: "auto" | "manual"
  toolPartIds?: string[]
  partIds?: string[]
  partId?: string
  totalChars: number
}

interface MessageTimelineProps {
  segments: TimelineSegment[]
  onSegmentClick?: (segment: TimelineSegment) => void
  onToggleSelection?: (id: string) => void
  onLongPressSelection?: (segment: TimelineSegment) => void
  onSelectRange?: (id: string) => void
  onClearSelection?: () => void
  selectedIds?: Accessor<Set<string>>
  expandedMessageIds?: Accessor<Set<string>>
  // Optional: restrict histogram/xray overlay to only show for these message ids.
  // Used to hide ribs for messages before the last compaction.
  deletableMessageIds?: Accessor<Set<string>>
  activeSegmentId?: string | null
  instanceId: string
  sessionId: string
  showToolSegments?: boolean
  deleteHover?: () => DeleteHoverState
  onDeleteHoverChange?: (state: DeleteHoverState) => void
  onDeleteMessagesUpTo?: (messageId: string) => void | Promise<void>
  selectedMessageIds?: () => Set<string>
  onToggleSelectedMessage?: (messageId: string, selected: boolean) => void
}

const MAX_TOOLTIP_LENGTH = 220
const LONG_PRESS_MS = 500
const JITTER_THRESHOLD = 10
const ABSOLUTE_TOKEN_CAP = 10000

interface PendingSegment {
  type: TimelineSegmentType
  texts: string[]
  reasoningTexts: string[]
  partIds: string[]
  totalChars: number
  hasPrimaryText: boolean
}

function truncateText(value: string): string {
  if (value.length <= MAX_TOOLTIP_LENGTH) {
    return value
  }
  return `${value.slice(0, MAX_TOOLTIP_LENGTH - 1).trimEnd()}…`
}

function getToolTypeLabel(toolName: string, t: (key: string, params?: Record<string, unknown>) => string): string {
  if (toolName.trim().length > 0) {
    return toolName.trim().slice(0, 4)
  }
  return t("messageTimeline.tool.fallbackLabel").slice(0, 4)
}

function formatTextsTooltip(texts: string[], fallback: string): string {
  const combined = texts
    .map((text) => text.trim())
    .filter((text) => text.length > 0)
    .join("\n\n")
  if (combined.length > 0) {
    return truncateText(combined)
  }
  return fallback
}

function formatToolTooltip(
  titles: string[],
  t: (key: string, params?: Record<string, unknown>) => string,
): string {
  if (titles.length === 0) {
    return t("messageTimeline.tool.fallbackLabel")
  }
  return truncateText(`${t("messageTimeline.tool.fallbackLabel")}: ${titles.join(", ")}`)
}

export function buildTimelineSegments(
  instanceId: string,
  record: MessageRecord,
  t: (key: string, params?: Record<string, unknown>) => string,
): TimelineSegment[] {
  if (!record) return []
  const entries = projectMessageEntries(instanceId, record, t)
  if (entries.length === 0) {
    return []
  }

  const segmentLabel = (type: TimelineSegmentType) => {
    if (type === "user") return t("messageTimeline.segment.user.label")
    if (type === "assistant") return t("messageTimeline.segment.assistant.label")
    if (type === "compaction") return t("messageTimeline.segment.compaction.label")
    return t("messageTimeline.tool.fallbackLabel").slice(0, 4)
  }

  const result: TimelineSegment[] = []
  let segmentIndex = 0
  let pending: PendingSegment | null = null
  const flushPending = () => {
    if (!pending) return
    if (pending.type === "assistant" && !pending.hasPrimaryText) {
      pending = null
      return
    }
    const firstPartId = pending.partIds.find((partId) => partId.length > 0)
    const label = segmentLabel(pending.type)
    const shortLabel = undefined
    const tooltip = formatTextsTooltip(
      [...pending.texts, ...pending.reasoningTexts],
      pending.type === "user" ? t("messageTimeline.tooltip.userFallback") : t("messageTimeline.tooltip.assistantFallback"),
    )

    result.push({
      id: firstPartId ? `${record.id}:${pending.type}:${firstPartId}` : `${record.id}:${pending.type}:${segmentIndex}`,
      messageId: record.id,
      type: pending.type,
      label,
      tooltip,
      shortLabel,
      partIds: pending.partIds,
      totalChars: pending.totalChars,
    })
    segmentIndex += 1
    pending = null
  }

  const ensureSegment = (type: TimelineSegmentType): PendingSegment => {
    if (!pending || pending.type !== type) {
      flushPending()
      pending = {
        type,
        texts: [],
        reasoningTexts: [],
        partIds: [],
        totalChars: 0,
        hasPrimaryText: type !== "assistant",
      }
    }
    return pending!
  }


  const defaultContentType: TimelineSegmentType = record.role === "user" ? "user" : "assistant"

  for (const entry of entries) {
    if (entry.kind === "tool") {
      flushPending()
      result.push({
        id: entry.key,
        messageId: record.id,
        type: "tool",
        label: getToolTypeLabel(entry.toolName, t) || segmentLabel("tool"),
        tooltip: formatToolTooltip([entry.toolTitle], t),
        shortLabel: getToolIcon(entry.toolName || "tool"),
        toolPartIds: entry.partId ? [entry.partId] : undefined,
        totalChars: entry.totalChars,
      })
      segmentIndex += 1
      continue
    }

    if (entry.kind === "reasoning") {
      if (entry.text.trim().length === 0) continue
      const target = ensureSegment(defaultContentType)
      if (target) {
        target.reasoningTexts.push(entry.text)
        if (entry.partId) target.partIds.push(entry.partId)
        target.totalChars += entry.totalChars
      }
      continue
    }

    if (entry.kind === "compaction") {
      flushPending()
      result.push({
        id: entry.key,
        messageId: record.id,
        type: "compaction",
        label: segmentLabel("compaction"),
        tooltip: entry.auto ? t("messageTimeline.tooltip.compaction.auto") : t("messageTimeline.tooltip.compaction.manual"),
        variant: entry.auto ? "auto" : "manual",
        partId: entry.partId,
        totalChars: 0,
      })
      segmentIndex += 1
      continue
    }

    if (entry.kind === "step-finish") {
      continue
    }

    if (entry.kind !== "content") continue
    if (entry.texts.length === 0) continue

    const target = ensureSegment(defaultContentType)
    if (target) {
      target.texts.push(...entry.texts)
      target.hasPrimaryText = target.hasPrimaryText || entry.hasRenderableText || entry.texts.length > 0
      target.partIds.push(...entry.partIds)
      target.totalChars += entry.totalChars
    }
  }


  flushPending()

  return result
}

const MessageTimeline: Component<MessageTimelineProps> = (props) => {
  const { t } = useI18n()
  const buttonRefs = new Map<string, HTMLButtonElement>()
  const store = () => messageStoreBus.getOrCreate(props.instanceId)
  const [hoveredSegment, setHoveredSegment] = createSignal<TimelineSegment | null>(null)
  const [tooltipCoords, setTooltipCoords] = createSignal<{ top: number; left: number }>({ top: 0, left: 0 })
  const [hoverAnchorRect, setHoverAnchorRect] = createSignal<{ top: number; left: number; width: number; height: number } | null>(null)
  const [tooltipSize, setTooltipSize] = createSignal<{ width: number; height: number }>({ width: 360, height: 420 })
  const [tooltipElement, setTooltipElement] = createSignal<HTMLDivElement | null>(null)
  let hoverTimer: number | null = null
  let closeTimer: number | null = null
  const showTools = () => props.showToolSegments ?? true
  const deleteHover = () => props.deleteHover?.() ?? { kind: "none" as const }

  const isHistogramEligible = (segment: TimelineSegment): boolean => {
    const allowed = props.deletableMessageIds?.()
    if (!allowed) return true
    return allowed.has(segment.messageId)
  }

  const registerButtonRef = (segmentId: string, element: HTMLButtonElement | null) => {
    if (element) {
      buttonRefs.set(segmentId, element)
    } else {
      buttonRefs.delete(segmentId)
    }
  }

  const clearHoverTimer = () => {
    if (hoverTimer !== null && typeof window !== "undefined") {
      window.clearTimeout(hoverTimer)
      hoverTimer = null
    }
  }

  const clearCloseTimer = () => {
    if (closeTimer !== null && typeof window !== "undefined") {
      window.clearTimeout(closeTimer)
      closeTimer = null
    }
  }

  const scheduleClose = () => {
    if (typeof window === "undefined") return
    clearHoverTimer()
    clearCloseTimer()
    // Small delay so the pointer can travel from the segment to the tooltip.
    closeTimer = window.setTimeout(() => {
      closeTimer = null
      setHoveredSegment(null)
      setHoverAnchorRect(null)
    }, 160)
  }

  const handleMouseEnter = (segment: TimelineSegment, event: MouseEvent) => {
    // Suppress previews during long-press selection gestures.
    if (longPressTimer !== null) return

    if (typeof window === "undefined") return
    clearHoverTimer()
    clearCloseTimer()
    const target = event.currentTarget as HTMLButtonElement
    hoverTimer = window.setTimeout(() => {
      const rect = target.getBoundingClientRect()
      setHoverAnchorRect({ top: rect.top, left: rect.left, width: rect.width, height: rect.height })
      setHoveredSegment(segment)
    }, 200)
  }

  const handleMouseLeave = () => {
    scheduleClose()
  }

  createEffect(() => {
    if (typeof window === "undefined") return
    const anchor = hoverAnchorRect()
    const segment = hoveredSegment()
    if (!anchor || !segment) return
    const { width, height } = tooltipSize()
    const verticalGap = 16
    const horizontalGap = 16
    const preferredTop = anchor.top + anchor.height / 2 - height / 2
    const maxTop = window.innerHeight - height - verticalGap
    const clampedTop = Math.min(maxTop, Math.max(verticalGap, preferredTop))
    const preferredLeft = anchor.left - width - horizontalGap
    const clampedLeft = Math.max(horizontalGap, preferredLeft)
    setTooltipCoords({ top: clampedTop, left: clampedLeft })
  })

  onCleanup(() => {
    clearHoverTimer()
    clearCloseTimer()
  })

  // --- Selection & histogram rib state ---
  const isSelectionActive = createMemo(() => (props.selectedIds?.().size ?? 0) > 0)

  // Segments eligible for xray ribs. We intentionally exclude messages before
  // the last compaction (when provided by the parent) to avoid misleading token
  // weights for content that's no longer in context.
  const xraySegments = createMemo(() => {
    if (!isSelectionActive()) return [] as TimelineSegment[]
    return props.segments.filter((segment) => isHistogramEligible(segment))
  })

  // Stable layout offsets per badge (relative to scroll content), recomputed only
  // on activation, resize, or expansion — NOT on every scroll frame.
  const [badgeOffsets, setBadgeOffsets] = createSignal<Record<string, { layoutTop: number; height: number }>>({})
  const [windowWidth, setWindowWidth] = createSignal(typeof window !== "undefined" ? window.innerWidth : 1200)
  let scrollContainerRef: HTMLDivElement | undefined
  let xrayOverlayRef: HTMLDivElement | undefined

  // Full layout recomputation: reads every badge's getBoundingClientRect once,
  // then stores offsets relative to the scroll content so they survive scrolling.
  const computeBadgeLayout = () => {
    if (!isSelectionActive() || !scrollContainerRef) return
    const containerRect = scrollContainerRef.getBoundingClientRect()
    const scrollTop = scrollContainerRef.scrollTop
    const offsets: Record<string, { layoutTop: number; height: number }> = {}

    for (const [id, element] of buttonRefs.entries()) {
      if (!element) continue
      const rect = element.getBoundingClientRect()
      // Store position relative to scroll content (survives scrolling).
      offsets[id] = {
        layoutTop: rect.top - containerRect.top + scrollTop,
        height: rect.height,
      }
    }
    setBadgeOffsets(offsets)
    if (xrayOverlayRef) {
      xrayOverlayRef.style.setProperty("--xray-scroll-y", `${-scrollTop}px`)
    }

    if (typeof window !== "undefined") {
      setWindowWidth(window.innerWidth)
    }
  }

  const handleScroll = () => {
    if (!isSelectionActive()) return
    if (!scrollContainerRef || !xrayOverlayRef) return
    xrayOverlayRef.style.setProperty("--xray-scroll-y", `${-scrollContainerRef.scrollTop}px`)
  }

  createEffect(() => {
    if (isSelectionActive()) {
      computeBadgeLayout()
      if (typeof window !== "undefined") {
        // Deferred pass: tool segments become visible when selection activates,
        // but they may need a layout pass before getBoundingClientRect is accurate.
        requestAnimationFrame(computeBadgeLayout)
        window.addEventListener("resize", computeBadgeLayout)
        onCleanup(() => {
          window.removeEventListener("resize", computeBadgeLayout)
        })
      }
    }
  })

  // Re-compute badge layout after expansion changes (tools become visible in DOM)
  createEffect(() => {
    props.expandedMessageIds?.()
    if (isSelectionActive()) {
      requestAnimationFrame(computeBadgeLayout)
    }
  })

  const maxRibWidth = createMemo(() => Math.round(windowWidth() * 0.5))

  // Compute fresh char counts from the store. segment.totalChars can be stale for
  // tool parts whose output arrived after the timeline segment was first built.
  const liveSegmentChars = createMemo(() => {
    if (!isSelectionActive()) return {} as Record<string, number>
    const result: Record<string, number> = {}
    const resolvedStore = store()

    // Compute live char counts by reading only the parts that the segment
    // references (partIds/toolPartIds). This stays accurate for streamed tool
    // outputs without scanning every part in the message.
    for (const segment of xraySegments()) {
      const record = resolvedStore.getMessage(segment.messageId)
      if (!record) {
        result[segment.id] = segment.totalChars
        continue
      }

      const ids = [...(segment.partIds ?? []), ...(segment.toolPartIds ?? [])]
      let chars = 0
      for (const partId of ids) {
        const part = record.parts?.[partId]?.data
        if (!part) continue
        chars += getPartCharCount(part)
      }

      result[segment.id] = chars > 0 ? chars : segment.totalChars
    }

    return result
  })

  // Pre-compute aggregate tokens per message: O(n) once, O(1) per lookup.
  // Avoids the previous O(n²) pattern of iterating all segments inside each <For> item.
  const aggregateTokensByMessageId = createMemo(() => {
    const chars = liveSegmentChars()
    const result: Record<string, number> = {}
    for (const s of xraySegments()) {
      result[s.messageId] = (result[s.messageId] ?? 0) + (chars[s.id] ?? s.totalChars)
    }
    for (const id of Object.keys(result)) {
      result[id] = Math.max(Math.round(result[id] / 4), 1)
    }
    return result
  })

  const getSegmentTokens = (segment: TimelineSegment): number => {
    const isExpanded = props.expandedMessageIds?.().has(segment.messageId) ?? false
    // When tools are hidden (not expanded, not in selection mode), assistant/user
    // bars show aggregate tokens for the whole message.  When tools are visible
    // (expanded or selection mode active), each segment shows its own tokens to
    // avoid double-counting.
    if (!isExpanded && !isSelectionActive() && (segment.type === "assistant" || segment.type === "user")) {
      return aggregateTokensByMessageId()[segment.messageId] ?? 1
    }
    const chars = liveSegmentChars()[segment.id] ?? segment.totalChars
    return Math.max(Math.round(chars / 4), 1)
  }

  const getMessageAggregateTokens = (messageId: string): number => {
    return aggregateTokensByMessageId()[messageId] ?? 1
  }

  const formatTokenLabel = (tokens: number): string => {
    if (tokens >= 1000000) return `${(tokens / 1000000).toFixed(1)}M`
    if (tokens >= 1000) return `${(tokens / 1000).toFixed(1)}K`
    return String(tokens)
  }

  const maxTokens = createMemo(() => {
    let max = 0
    for (const s of xraySegments()) {
      const tokens = getSegmentTokens(s)
      if (tokens > max) max = tokens
    }
    return Math.max(max, 1)
  })

  // --- Long-press for mobile selection ---
  let longPressTimer: number | null = null
  let wasLongPress = false
  let pressStartPos = { x: 0, y: 0 }

  const handlePointerDown = (segment: TimelineSegment, event: PointerEvent) => {
    if (event.button !== 0) return
    wasLongPress = false
    pressStartPos = { x: event.clientX, y: event.clientY }

    clearHoverTimer()
    clearCloseTimer()

    if (longPressTimer !== null && typeof window !== "undefined") {
      window.clearTimeout(longPressTimer)
    }

    if (typeof window !== "undefined") {
      longPressTimer = window.setTimeout(() => {
        longPressTimer = null
        wasLongPress = true

        // Scroll anchoring: preserve visual position of the pressed badge.
        const btn = buttonRefs.get(segment.id)
        let anchorOffset: number | null = null
        if (btn && scrollContainerRef) {
          anchorOffset = btn.offsetTop - scrollContainerRef.scrollTop
        }

        if (props.onLongPressSelection) {
          props.onLongPressSelection(segment)
        } else {
          props.onToggleSelection?.(segment.id)
        }

        if (anchorOffset !== null && btn && scrollContainerRef) {
          const desired = btn.offsetTop - anchorOffset
          if (Math.abs(scrollContainerRef.scrollTop - desired) > 1) {
            scrollContainerRef.scrollTop = desired
          }
        }
      }, LONG_PRESS_MS)
    }
  }

  const handlePointerUp = () => {
    if (longPressTimer !== null && typeof window !== "undefined") {
      window.clearTimeout(longPressTimer)
      longPressTimer = null
    }
  }

  const handlePointerMove = (event: PointerEvent) => {
    if (longPressTimer !== null) {
      const dist = Math.sqrt(
        Math.pow(event.clientX - pressStartPos.x, 2) +
        Math.pow(event.clientY - pressStartPos.y, 2),
      )
      if (dist > JITTER_THRESHOLD) {
        if (typeof window !== "undefined") {
          window.clearTimeout(longPressTimer)
        }
        longPressTimer = null
      }
    }
  }

  const handleContextMenu = (event: MouseEvent) => {
    if (wasLongPress) {
      event.preventDefault()
    }
  }

  createEffect(on(() => props.activeSegmentId, (activeId) => {
    if (!activeId) return
    const element = buttonRefs.get(activeId)
    if (!element) return
    const timer = typeof window !== "undefined" ? window.setTimeout(() => {
      element.scrollIntoView({ block: "nearest", behavior: "smooth" })
    }, 120) : null
    onCleanup(() => {
      if (timer !== null && typeof window !== "undefined") {
        window.clearTimeout(timer)
      }
    })
  }))

  createEffect(() => {
    const element = tooltipElement()
    if (!element || typeof window === "undefined") return
    const updateSize = () => {
      const rect = element.getBoundingClientRect()
      setTooltipSize({ width: rect.width, height: rect.height })
    }
    updateSize()
    if (typeof ResizeObserver === "undefined") return
    const observer = new ResizeObserver(() => updateSize())
    observer.observe(element)
    onCleanup(() => observer.disconnect())
  })

  const previewData = createMemo(() => {
    const segment = hoveredSegment()
    if (!segment) return null
    const record = store().getMessage(segment.messageId)
    if (!record) return null
    return { messageId: segment.messageId }
  })

  // Pre-computed set of messageIds that have at least one tool segment.
  // Used by groupRole() inside <For> to avoid O(n) .some() per segment → O(1) .has().
  const messagesWithTools = createMemo(() => {
    const set = new Set<string>()
    for (const s of props.segments) {
      if (s.type === "tool") set.add(s.messageId)
    }
    return set
  })

  // Pre-computed index map for session message ordering.
  // Used by isDeleteHovered() to replace O(n) indexOf with O(1) Map.get().
  const messageIdToSessionIndex = createMemo(() => {
    const ids = store().getSessionMessageIds(props.sessionId)
    const map = new Map<string, number>()
    for (let i = 0; i < ids.length; i++) map.set(ids[i], i)
    return map
  })

  return (
    <div class="message-timeline-container">
      <div
        ref={scrollContainerRef}
        class={`message-timeline${isSelectionActive() ? " message-timeline--selection-active" : ""}`}
        role="navigation"
        aria-label={t("messageTimeline.ariaLabel")}
        onScroll={handleScroll}
      >
        <For each={props.segments}>
          {(segment, segIndex) => {
            onCleanup(() => buttonRefs.delete(segment.id))
            const isActive = () => props.activeSegmentId === segment.id
            const isSelected = () => props.selectedIds?.().has(segment.id)

            const isDeleteHovered = () => {
              const hover = deleteHover() as DeleteHoverState
              if (hover.kind === "message") {
                return hover.messageId === segment.messageId
              }

              if (hover.kind === "deleteUpTo") {
                const indexMap = messageIdToSessionIndex()
                const targetIndex = indexMap.get(hover.messageId)
                if (targetIndex === undefined) return false
                const segmentIndex = indexMap.get(segment.messageId)
                if (segmentIndex === undefined) return false
                return segmentIndex >= targetIndex
              }

              return false
            }

            const isDeleteSelected = () => {
              const selected = props.selectedMessageIds?.()
              if (!selected) return false
              return selected.has(segment.messageId)
            }

            const hasActivePermission = () => {
              if (segment.type !== "tool") return false
              const partIds = segment.toolPartIds ?? []
              if (partIds.length === 0) return false
              for (const partId of partIds) {
                const permissionState = store().getPermissionState(segment.messageId, partId)
                if (permissionState?.active) return true
              }
              return false
            }

            const isExpanded = () => props.expandedMessageIds?.().has(segment.messageId) ?? false
            const isHidden = () =>
              segment.type === "tool" &&
              !(showTools() || isExpanded() || isSelectionActive() || isActive() || hasActivePermission() || isDeleteHovered() || isDeleteSelected())

            // Group visual indicators: tools belong to the same message as their
            // assistant.  Uses messageId for correctness (not positional adjacency).
            const groupRole = (): "child" | "parent" | "none" => {
              if (segment.type === "tool") return "child"
              if (segment.type === "assistant" && messagesWithTools().has(segment.messageId)) return "parent"
              return "none"
            }
            const isGroupStart = () => {
              if (segment.type !== "tool") return false
              const idx = segIndex()
              const prev = idx > 0 ? props.segments[idx - 1] : null
              // First tool in the message's run: either nothing before, or previous
              // segment is from a different message or is not a tool.
              return !prev || prev.type !== "tool" || prev.messageId !== segment.messageId
            }

             const shortLabelContent = () => {
               if (segment.type === "tool") {
                 if (hasActivePermission()) {
                   return <ShieldAlert class="message-timeline-icon" aria-hidden="true" />
                 }
                 return segment.shortLabel ?? getToolIcon("tool")
               }
               if (segment.type === "compaction") {
                 return <FoldVertical class="message-timeline-icon" aria-hidden="true" />
               }
               if (segment.type === "user") {
                 return <UserIcon class="message-timeline-icon" aria-hidden="true" />
               }
               return <BotIcon class="message-timeline-icon" aria-hidden="true" />
             }

            return (
              <button
                  ref={(el) => registerButtonRef(segment.id, el)}
                  type="button"
                  data-variant={segment.variant}
                  class={`message-timeline-segment message-timeline-${segment.type} ${hasActivePermission() ? "message-timeline-segment-permission" : ""} ${segment.type === "compaction" ? `message-timeline-compaction-${segment.variant ?? "manual"}` : ""} ${isActive() ? "message-timeline-segment-active" : ""} ${isHidden() ? "message-timeline-segment-hidden" : ""} ${isSelected() ? "message-timeline-segment-selected" : ""} ${isDeleteSelected() ? "message-timeline-segment-delete-selected" : ""} ${groupRole() !== "none" ? `message-timeline-group-${groupRole()}` : ""} ${isGroupStart() ? "message-timeline-group-start" : ""}`}

                  data-delete-hover={isDeleteHovered() || isDeleteSelected() || isSelected() ? "true" : undefined}

                 aria-current={isActive() ? "true" : undefined}
                 aria-hidden={isHidden() ? "true" : undefined}
                 onClick={(event) => {
                   if (wasLongPress) {
                     wasLongPress = false
                     return
                   }

                   // Capture scroll anchor before selection changes may toggle
                   // tool segment visibility, which shifts timeline layout.
                   const btn = buttonRefs.get(segment.id)
                   let anchorOffset: number | null = null
                   if (btn && scrollContainerRef) {
                     anchorOffset = btn.offsetTop - scrollContainerRef.scrollTop
                   }

                   const isMultiSelectActive = (props.selectedIds?.().size ?? 0) > 0

                   if (event.shiftKey) {
                     props.onSelectRange?.(segment.id)
                   } else if (event.ctrlKey || event.metaKey) {
                     props.onToggleSelection?.(segment.id)
                   } else if (isMultiSelectActive) {
                     // In selection mode, plain click scrolls to the message
                     // instead of clearing. Selection is cleared by clicking
                     // anywhere inside the chat container or pressing Esc.
                     props.onSegmentClick?.(segment)
                   } else {
                     props.onSegmentClick?.(segment)
                   }

                   // Restore scroll anchor: keep the clicked badge at the same
                   // visual position after hidden tools appear or disappear.
                   if (anchorOffset !== null && btn && scrollContainerRef) {
                     const desired = btn.offsetTop - anchorOffset
                     if (Math.abs(scrollContainerRef.scrollTop - desired) > 1) {
                       scrollContainerRef.scrollTop = desired
                     }
                   }
                 }}
                onPointerDown={(e) => handlePointerDown(segment, e)}
                 onPointerUp={handlePointerUp}
                 onPointerCancel={handlePointerUp}
                 onPointerMove={handlePointerMove}
                 onContextMenu={handleContextMenu}
                 onMouseEnter={(event) => handleMouseEnter(segment, event)}
                 onMouseLeave={handleMouseLeave}
              >
                <span class="message-timeline-label message-timeline-label-full">{segment.label}</span>
                <span class="message-timeline-label message-timeline-label-short">{shortLabelContent()}</span>
              </button>
            )
          }}
        </For>
        <Show when={previewData()}>
          {(data) => {
            onCleanup(() => setTooltipElement(null))
            return (
              <div
                ref={(element) => setTooltipElement(element)}
                class="message-timeline-tooltip"
                style={{ top: `${tooltipCoords().top}px`, left: `${tooltipCoords().left}px` }}
                onMouseEnter={() => clearCloseTimer()}
                onMouseLeave={() => scheduleClose()}
              >
                <MessagePreview
                  messageId={data().messageId}
                  instanceId={props.instanceId}
                  sessionId={props.sessionId}
                  store={store}
                  deleteHover={props.deleteHover}
                  onDeleteHoverChange={props.onDeleteHoverChange}
                  onDeleteMessagesUpTo={props.onDeleteMessagesUpTo}
                  selectedMessageIds={props.selectedMessageIds}
                />
              </div>
            )
          }}
        </Show>
      </div>

      <Show when={isSelectionActive()}>
        <div
          ref={(el) => {
            xrayOverlayRef = el
            if (xrayOverlayRef && scrollContainerRef) {
              xrayOverlayRef.style.setProperty("--xray-scroll-y", `${-scrollContainerRef.scrollTop}px`)
            }
          }}
          class="message-timeline-xray-overlay"
          style={{ "--max-rib-width": `${maxRibWidth()}px` }}
        >
          <div class="message-timeline-xray-overlay-inner">
          <For each={xraySegments()}>
            {(segment) => {
              const pos = () => {
                const offset = badgeOffsets()[segment.id]
                if (!offset) return null
                return { top: offset.layoutTop + offset.height / 2 }
              }
              const tokens = () => getSegmentTokens(segment)
              const relativeWeight = () => tokens() / maxTokens()
              const absoluteWeight = () => Math.min(tokens() / ABSOLUTE_TOKEN_CAP, 1.0)
              const isOverflow = () => tokens() > ABSOLUTE_TOKEN_CAP
              const isParent = segment.type === "assistant" || segment.type === "user"
              const displayTokens = () =>
                isParent ? getMessageAggregateTokens(segment.messageId) : tokens()
              return (
                <Show when={pos()}>
                  <div
                    class="message-timeline-xray-rib"
                    style={{
                      top: `${pos()!.top}px`,
                      left: "var(--xray-overhang)",
                    }}
                  >
                    <span class="message-timeline-xray-token-label">
                      {formatTokenLabel(displayTokens())}
                    </span>
                    <div
                      class="message-timeline-relative-bar"
                      style={{ "--segment-weight": relativeWeight() }}
                    />
                    <div
                      class={`message-timeline-absolute-bar${isOverflow() ? " message-timeline-absolute-bar-overflow" : ""}`}
                      style={{ "--segment-weight": absoluteWeight() }}
                    />
                  </div>
                </Show>
              )
            }}
          </For>
          </div>
        </div>
      </Show>
    </div>
  )
}

export default MessageTimeline
