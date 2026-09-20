import { createHmac, randomUUID } from 'node:crypto'

import WebSocket from 'ws'

import { merge } from '@moeru/std'
import { ofetch } from 'ofetch'

type AliyunNlsRegion = 'cn-shanghai' | 'cn-shanghai-internal' | 'cn-beijing' | 'cn-beijing-internal' | 'cn-shenzhen' | 'cn-shenzhen-internal'

interface AliyunNlsCredentials {
  accessKeyId: string
  accessKeySecret: string
  appKey: string
  region: AliyunNlsRegion
}

interface AliyunNlsToken {
  token: string
  expiresAt: number
}

interface AliyunNlsStartPayload {
  format?: 'pcm' | 'wav' | 'opus' | 'speex' | 'amr' | 'mp3' | 'aac'
  sample_rate?: 8000 | 16000
  enable_intermediate_result?: boolean
  enable_punctuation_prediction?: boolean
  enable_inverse_text_normalization?: boolean
  enable_words?: boolean
  max_sentence_silence?: number
}

interface AliyunNlsServerEvent {
  header?: {
    name?: string
  }
  payload?: {
    result?: string
  }
}

interface CreateAliyunNlsStreamResponseOptions {
  audioStream: ReadableStream<Uint8Array>
  credentials: AliyunNlsCredentials
  createToken?: (credentials: AliyunNlsCredentials) => Promise<AliyunNlsToken>
  emitHops?: boolean
  readyElapsedMs?: number
  sessionOptions?: AliyunNlsStartPayload
  websocketBaseURL?: string
}

const encoder = new TextEncoder()
const DEFAULT_SESSION_OPTIONS: AliyunNlsStartPayload = {
  format: 'pcm',
  sample_rate: 16000,
  enable_intermediate_result: true,
  enable_punctuation_prediction: true,
  enable_words: true,
}

function nlsMetaEndpointFromRegion(region: AliyunNlsRegion): URL {
  return new URL(`http://nls-meta.${region}.aliyuncs.com`)
}

function nlsWebSocketEndpointFromRegion(region: AliyunNlsRegion): URL {
  const websocketURL = new URL('/ws/v1', 'https://example.com')

  switch (region) {
    case 'cn-shanghai':
    case 'cn-beijing':
    case 'cn-shenzhen':
      websocketURL.protocol = 'wss:'
      websocketURL.hostname = `nls-gateway-${region}.aliyuncs.com`
      break
    case 'cn-shanghai-internal':
    case 'cn-beijing-internal':
    case 'cn-shenzhen-internal':
      websocketURL.protocol = 'wss:'
      websocketURL.hostname = `nls-gateway-${region}-internal.aliyuncs.com:80`
      break
  }

  return websocketURL
}

function canonicalizeQuery(params: Record<string, string>): string {
  return Object.keys(params)
    .sort()
    .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`)
    .join('&')
}

function createStringToSign(method: string, path: string, canonicalQuery: string): string {
  return `${method}&${encodeURIComponent(path)}&${encodeURIComponent(canonicalQuery)}`
}

function signStringToBase64(stringToSign: string, accessKeySecret: string): string {
  return createHmac('sha1', `${accessKeySecret}&`).update(stringToSign).digest('base64')
}

function aliyunTimestamp(date: Date): string {
  return date.toISOString().replace(/\.\d{3}Z$/, 'Z')
}

async function createAliyunNlsToken(credentials: AliyunNlsCredentials): Promise<AliyunNlsToken> {
  const params: Record<string, string> = {
    AccessKeyId: credentials.accessKeyId,
    Action: 'CreateToken',
    Format: 'JSON',
    RegionId: credentials.region,
    SignatureMethod: 'HMAC-SHA1',
    SignatureNonce: randomUUID(),
    SignatureVersion: '1.0',
    Timestamp: aliyunTimestamp(new Date()),
    Version: '2019-02-28',
  }
  const canonicalQuery = canonicalizeQuery(params)
  const signature = encodeURIComponent(signStringToBase64(createStringToSign('POST', '/', canonicalQuery), credentials.accessKeySecret))
  const endpoint = nlsMetaEndpointFromRegion(credentials.region).toString().replace(/\/$/, '')
  const response = await ofetch<{
    Token?: { ExpireTime?: number, Id?: string }
    Message?: string
  }>(`${endpoint}/?Signature=${signature}&${canonicalQuery}`, { method: 'POST' })

  if (typeof response.Token?.Id === 'string' && typeof response.Token?.ExpireTime === 'number')
    return { token: response.Token.Id, expiresAt: response.Token.ExpireTime * 1000 }

  throw new Error(`Failed to create Aliyun NLS token: ${response.Message || 'unknown error'}`)
}

function sse(payload: { delta: string, type: 'transcript.text.delta' | 'transcript.text.done' }): Uint8Array {
  return encoder.encode(`data: ${JSON.stringify(payload)}\n\n`)
}

function sseHop(name: string, data: Record<string, unknown>): Uint8Array {
  return encoder.encode(`data: ${JSON.stringify({ type: 'airi.debug.hop', name, ...data })}\n\n`)
}

function emitHop(
  controller: ReadableStreamDefaultController<Uint8Array> | undefined,
  emitHops: boolean,
  name: string,
  data: Record<string, unknown>,
) {
  if (!emitHops || !controller)
    return
  try {
    controller.enqueue(sseHop(name, data))
  }
  catch {
    // The client may already have closed the SSE body.
  }
}

function createClientEvent(credentials: AliyunNlsCredentials, name: 'StartTranscription' | 'StopTranscription', sessionId: string, payload?: AliyunNlsStartPayload) {
  return JSON.stringify({
    header: {
      appkey: credentials.appKey,
      message_id: randomUUID().replaceAll('-', ''),
      task_id: sessionId,
      namespace: 'SpeechTranscriber',
      name,
    },
    payload,
  })
}

async function writeAudioToUpstream(
  audioStream: ReadableStream<Uint8Array>,
  ws: WebSocket,
  credentials: AliyunNlsCredentials,
  sessionId: string,
  t0: number,
  controller: ReadableStreamDefaultController<Uint8Array>,
  emitHops: boolean,
) {
  const reader = audioStream.getReader()
  let chunks = 0
  let bytes = 0
  let firstChunkElapsedMs: number | null = null
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done)
        break
      if (value) {
        if (chunks === 0) {
          firstChunkElapsedMs = Date.now() - t0
          emitHop(controller, emitHops, 'nls_audio_first_chunk', { elapsedMs: firstChunkElapsedMs, byteLength: value.byteLength })
        }
        chunks++
        bytes += value.byteLength
        ws.send(value, { binary: true })
      }
    }
  }
  finally {
    ws.send(createClientEvent(credentials, 'StopTranscription', sessionId))
    emitHop(controller, emitHops, 'nls_audio_stop', { elapsedMs: Date.now() - t0, chunks, bytes, firstChunkElapsedMs })
  }
}

/**
 * Streams client microphone PCM through Aliyun NLS and returns xsai-compatible SSE transcript deltas.
 *
 * Use when:
 * - AIRI owns the Aliyun NLS credentials server-side.
 * - The browser uploads a realtime audio `ReadableStream` and expects transcript deltas.
 *
 * Expects:
 * - `audioStream` contains 16 kHz PCM chunks by default, matching the Hearing worklet output.
 *
 * Returns:
 * - A `text/event-stream` response consumable by the shared `streamTranscription` adapter.
 */
export function createAliyunNlsStreamResponse(options: CreateAliyunNlsStreamResponseOptions): Response {
  const emitHops = options.emitHops === true
  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      const t0 = Date.now()
      if (options.readyElapsedMs != null)
        emitHop(controller, emitHops, 'asr_stream_ready', { elapsedMs: options.readyElapsedMs, region: options.credentials.region })
      const createToken = options.createToken ?? createAliyunNlsToken
      const token = await createToken(options.credentials)
      emitHop(controller, emitHops, 'nls_token_created', { elapsedMs: Date.now() - t0, region: options.credentials.region })
      const sessionId = randomUUID().replaceAll('-', '')
      const upstreamURL = new URL(options.websocketBaseURL ?? nlsWebSocketEndpointFromRegion(options.credentials.region))
      upstreamURL.searchParams.set('token', token.token)

      const ws = new WebSocket(upstreamURL)
      let loggedIntermediate = false

      ws.on('open', () => {
        emitHop(controller, emitHops, 'nls_ws_open', { elapsedMs: Date.now() - t0, region: options.credentials.region })
        ws.send(createClientEvent(options.credentials, 'StartTranscription', sessionId, merge(DEFAULT_SESSION_OPTIONS, options.sessionOptions)))
      })

      ws.on('message', (data) => {
        const event = JSON.parse(data.toString()) as AliyunNlsServerEvent
        const eventName = event.header?.name
        const hopData = { elapsedMs: Date.now() - t0, eventName, hasResult: typeof event.payload?.result === 'string', resultLen: event.payload?.result?.length ?? 0 }
        if (eventName === 'TranscriptionResultChanged') {
          if (!loggedIntermediate) {
            loggedIntermediate = true
            emitHop(controller, emitHops, 'nls_event', hopData)
          }
        }
        else {
          emitHop(controller, emitHops, 'nls_event', hopData)
        }
        switch (eventName) {
          case 'TranscriptionStarted':
            void writeAudioToUpstream(options.audioStream, ws, options.credentials, sessionId, t0, controller, emitHops)
            break
          case 'SentenceEnd': {
            const text = event.payload?.result ? `${event.payload.result}\n` : ''
            if (text)
              controller.enqueue(sse({ delta: text, type: 'transcript.text.delta' }))
            controller.enqueue(sse({ delta: '', type: 'transcript.text.done' }))
            break
          }
          case 'TranscriptionCompleted':
            controller.close()
            ws.close(1000, 'completed')
            break
        }
      })

      ws.on('error', (error) => {
        emitHop(controller, emitHops, 'nls_ws_error', { elapsedMs: Date.now() - t0, message: error instanceof Error ? error.message : String(error) })
        controller.error(error)
      })

      ws.on('close', () => {
        try {
          controller.close()
        }
        catch {}
      })
    },
    cancel() {
      // The upstream websocket is closed by its own completion/error handlers.
    },
  })

  return new Response(body, {
    headers: {
      'Cache-Control': 'no-cache',
      'Content-Type': 'text/event-stream',
    },
  })
}
