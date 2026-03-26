import { createEffect, onCleanup } from "solid-js"

interface FastStreamingTextProps {
  text: string
  class?: string
  dir?: "auto" | "ltr" | "rtl"
  onRendered?: () => void
}

export default function FastStreamingText(props: FastStreamingTextProps) {
  let element: HTMLDivElement | undefined
  let textNode: Text | null = null
  let previousText = ""
  let pendingFrame: number | null = null

  const notifyRendered = () => {
    if (!props.onRendered || typeof requestAnimationFrame !== "function") {
      return
    }

    if (pendingFrame !== null) {
      cancelAnimationFrame(pendingFrame)
    }

    pendingFrame = requestAnimationFrame(() => {
      pendingFrame = null
      props.onRendered?.()
    })
  }

  createEffect(() => {
    const nextText = props.text ?? ""
    if (!element) {
      previousText = nextText
      return
    }

    if (!textNode) {
      textNode = document.createTextNode(nextText)
      element.replaceChildren(textNode)
      previousText = nextText
      notifyRendered()
      return
    }

    if (nextText === previousText) {
      return
    }

    if (nextText.startsWith(previousText)) {
      textNode.appendData(nextText.slice(previousText.length))
    } else {
      textNode.data = nextText
    }

    previousText = nextText
    notifyRendered()
  })

  onCleanup(() => {
    if (pendingFrame !== null) {
      cancelAnimationFrame(pendingFrame)
    }
  })

  return <div ref={element} class={props.class} dir={props.dir ?? "auto"} />
}
