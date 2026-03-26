import type { MessageStatus } from "../stores/message-v2/types"
import type { ClientPart } from "../types/message"

export type AssistantStreamRenderMode =
  | "streaming_preview"
  | "stabilizing_preview"
  | "complete_rich"

export interface AssistantStreamRenderDecision {
  mode: AssistantStreamRenderMode
  text: string
}

interface ResolveAssistantStreamRenderDecisionInput {
  messageType?: "user" | "assistant"
  messageStatus?: MessageStatus
  part: ClientPart
  previewText?: string
}

export function resolveAssistantStreamRenderDecision(
  input: ResolveAssistantStreamRenderDecisionInput,
): AssistantStreamRenderDecision {
  const canonicalText =
    input.part?.type === "text" && typeof input.part.text === "string" ? input.part.text : ""
  const previewText = input.previewText ?? ""
  const hasPreview = previewText.length > 0

  if (input.messageType !== "assistant" || input.part?.type !== "text") {
    return {
      mode: "complete_rich",
      text: canonicalText,
    }
  }

  if (input.messageStatus === "streaming") {
    if (hasPreview) {
      return {
        mode: "streaming_preview",
        text: previewText,
      }
    }
  }

  if (hasPreview && canonicalText.length >= previewText.length) {
    return {
      mode: "complete_rich",
      text: canonicalText,
    }
  }

  if ((input.messageStatus === "complete" || input.messageStatus === "error") && canonicalText.length > 0) {
    return {
      mode: "complete_rich",
      text: canonicalText,
    }
  }

  if (hasPreview) {
    return {
      mode: "stabilizing_preview",
      text: previewText,
    }
  }

  return {
    mode: "complete_rich",
    text: canonicalText,
  }
}
