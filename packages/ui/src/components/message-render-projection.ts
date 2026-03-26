import { buildRecordDisplayData } from "../stores/message-v2/record-display-cache"
import type { MessageRecord } from "../stores/message-v2/types"
import { getPartCharCount } from "../lib/token-utils"
import type { ClientPart } from "../types/message"
import { partHasRenderableText } from "../types/message"

type ToolCallPart = Extract<ClientPart, { type: "tool" }>
type Translate = (key: string, params?: Record<string, unknown>) => string

export type MessageProjectionEntry =
  | {
      kind: "content"
      key: string
      startPartId: string
      partIds: string[]
      texts: string[]
      totalChars: number
      hasRenderableText: boolean
    }
  | {
      kind: "tool"
      key: string
      partId: string
      part: ToolCallPart
      toolName: string
      toolTitle: string
      totalChars: number
    }
  | {
      kind: "reasoning"
      key: string
      partId: string
      part: ClientPart
      text: string
      totalChars: number
    }
  | {
      kind: "compaction"
      key: string
      partId: string
      part: ClientPart
      auto: boolean
      totalChars: number
    }
  | {
      kind: "step-finish"
      key: string
      partId: string
      part: ClientPart
      totalChars: number
    }

export function isSupportedPartType(part: unknown): boolean {
  const type = (part as any)?.type
  return !(typeof type === "string" && type === "patch")
}

export function collectReasoningText(part: ClientPart): string {
  const stringifySegment = (segment: unknown): string => {
    if (typeof segment === "string") return segment
    if (segment && typeof segment === "object") {
      const obj = segment as { text?: unknown; value?: unknown; content?: unknown[] }
      const parts: string[] = []
      if (typeof obj.text === "string") parts.push(obj.text)
      if (typeof obj.value === "string") parts.push(obj.value)
      if (Array.isArray(obj.content)) parts.push(obj.content.map((entry) => stringifySegment(entry)).join("\n"))
      return parts.filter(Boolean).join("\n")
    }
    return ""
  }

  if (typeof (part as any)?.text === "string") return (part as any).text
  if (Array.isArray((part as any)?.content)) {
    return (part as any).content.map((entry: unknown) => stringifySegment(entry)).join("\n")
  }
  return ""
}

export function reasoningHasRenderableContent(part: ClientPart): boolean {
  if (!part || part.type !== "reasoning") return false
  return collectReasoningText(part).trim().length > 0
}

export function collectTextFromPart(part: ClientPart, t: Translate): string {
  if (!part) return ""
  if (typeof (part as any).text === "string") return (part as any).text as string
  if (part.type === "reasoning") return collectReasoningText(part)
  if (Array.isArray((part as any)?.content)) {
    return ((part as any).content as unknown[])
      .map((entry) => (typeof entry === "string" ? entry : ""))
      .filter(Boolean)
      .join("\n")
  }
  if (part.type === "file") {
    const filename = (part as any)?.filename
    return typeof filename === "string" && filename.length > 0
      ? t("messageTimeline.text.filePrefix", { filename })
      : t("messageTimeline.text.attachment")
  }
  return ""
}

export function getToolTitle(part: ToolCallPart, t: Translate): string {
  const metadata = (((part as unknown as { state?: { metadata?: unknown } })?.state?.metadata) || {}) as { title?: unknown }
  const title = typeof metadata.title === "string" && metadata.title.length > 0 ? metadata.title : undefined
  if (title) return title
  if (typeof part.tool === "string" && part.tool.length > 0) return part.tool
  return t("messageTimeline.tool.fallbackLabel")
}

export function projectMessageEntries(
  instanceId: string,
  record: MessageRecord,
  t: Translate,
): MessageProjectionEntry[] {
  if (!record) return []
  const { orderedParts } = buildRecordDisplayData(instanceId, record)
  if (!orderedParts || orderedParts.length === 0) return []

  const entries: MessageProjectionEntry[] = []
  let pendingParts: ClientPart[] = []

  const flushContent = () => {
    if (pendingParts.length === 0) return
    const startPartId = typeof (pendingParts[0] as any)?.id === "string" ? ((pendingParts[0] as any).id as string) : ""
    if (!startPartId) {
      pendingParts = []
      return
    }

    const partIds: string[] = []
    const texts: string[] = []
    let totalChars = 0
    let hasRenderableText = false

    for (const part of pendingParts) {
      const partId = typeof (part as any)?.id === "string" ? ((part as any).id as string) : ""
      if (partId) partIds.push(partId)
      const text = collectTextFromPart(part, t)
      if (text.trim().length > 0) texts.push(text)
      if (partHasRenderableText(part)) hasRenderableText = true
      totalChars += getPartCharCount(part)
    }

    entries.push({
      kind: "content",
      key: `${record.id}:content:${startPartId}`,
      startPartId,
      partIds,
      texts,
      totalChars,
      hasRenderableText,
    })
    pendingParts = []
  }

  orderedParts.forEach((part, partIndex) => {
    if (!isSupportedPartType(part)) return

    if (part.type === "tool") {
      flushContent()
      const partId = part.id
      if (!partId) return
      entries.push({
        kind: "tool",
        key: `${record.id}:${partId}`,
        partId,
        part,
        toolName: typeof part.tool === "string" ? part.tool : "",
        toolTitle: getToolTitle(part, t),
        totalChars: getPartCharCount(part),
      })
      return
    }

    if (part.type === "compaction") {
      flushContent()
      const partId = part.id ?? ""
      entries.push({
        kind: "compaction",
        key: `${record.id}:${partId || partIndex}:compaction`,
        partId,
        part,
        auto: Boolean((part as any)?.auto),
        totalChars: 0,
      })
      return
    }

    if (part.type === "step-start") {
      flushContent()
      return
    }

    if (part.type === "step-finish") {
      flushContent()
      entries.push({
        kind: "step-finish",
        key: `${record.id}:${part.id ?? partIndex}:step-finish`,
        partId: part.id ?? "",
        part,
        totalChars: 0,
      })
      return
    }

    if (part.type === "reasoning") {
      flushContent()
      if (reasoningHasRenderableContent(part)) {
        entries.push({
          kind: "reasoning",
          key: `${record.id}:${part.id ?? partIndex}:reasoning`,
          partId: part.id ?? "",
          part,
          text: collectReasoningText(part),
          totalChars: getPartCharCount(part),
        })
      }
      return
    }

    pendingParts.push(part)
  })

  flushContent()
  return entries
}

export function buildMessageProjectionStructureKey(record: MessageRecord | undefined): string {
  if (!record) return ""

  const tokens: string[] = []
  let pendingContentStart = ""

  const flushContent = () => {
    if (!pendingContentStart) return
    tokens.push(`content:${pendingContentStart}`)
    pendingContentStart = ""
  }

  for (const partId of record.partIds) {
    const part = record.parts[partId]?.data
    if (!part || !isSupportedPartType(part)) continue

    if (part.type === "tool") {
      flushContent()
      tokens.push(`tool:${partId}`)
      continue
    }

    if (part.type === "compaction") {
      flushContent()
      tokens.push(`compaction:${partId}:${Boolean((part as any)?.auto) ? 1 : 0}`)
      continue
    }

    if (part.type === "step-start" || part.type === "step-finish") {
      flushContent()
      continue
    }

    if (part.type === "reasoning") {
      flushContent()
      tokens.push(`reasoning:${partId}`)
      continue
    }

    if (!pendingContentStart) {
      pendingContentStart = partId
    }
  }

  flushContent()
  return tokens.join("|")
}
