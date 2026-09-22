import type { RequestOptions } from '../request'
import request from '../request'

export interface EgressBinding {
  resourceGroup: string
  nic: string
  ipConfiguration: string
  sourceIp: string
  dedicated: boolean
}
export interface EgressInstance {
  provider: string
  name: string
  subscription?: string
  resourceGroup?: string
  location?: string
  host?: string
  port?: number
  username?: string
  password?: string
  passwordSet?: boolean
  maxConcurrent?: number
  intervalSeconds?: number
  bindings: Record<string, Partial<EgressBinding>>
}

export type ProbeProfile = 'codex_core' | 'minimal_compat'

export interface FetcherConfig {
  accountId: string
  enabled: boolean
  models: string[]
  proxyId: string | null
  dynamicEgress?: { instance: string, family: string } | null
  schedule?: { startMinute: number, endMinute: number } | null
  probeProfile: ProbeProfile
  adaptiveConcurrency: boolean
  refreshIntervalMinutes: number
  stateTtlMinutes: number
  revision: number
}
export interface FetcherValue {
  accountId: string
  model: string
  value: string
  acquiredAt: number
  lastSeenAt: number
  expiresAt: number
  source: string
  proxyUrl: string | null
}
export interface FetcherAttempt {
  accountId: string
  model: string
  configRevision: number
  attemptedAt: number
  nextAttemptAt: number
  failures: number
  paused: boolean
  status: string
  message: string
  byteLength: number | null
  durationMs: number
  inputTokens: number | null
  outputTokens: number | null
  exitIp?: string | null
  searchConcurrency?: number | null
}
export interface ProbeRecord {
  id: string
  batchId: string
  accountId: string
  model: string
  configRevision: number
  profile: ProbeProfile
  startedAt: number
  durationMs: number
  outcome: string
  httpStatus: number | null
  httpVersion: string | null
  byteLength: number | null
  repeated: boolean
  endpoint: string | null
  responsesLite: boolean
  compressed: boolean
  egressInstance: string | null
  leaseId: string | null
  exitIp: string | null
  freshConnection: boolean
}
export interface FetcherSnapshot {
  configs: FetcherConfig[]
  values: FetcherValue[]
  attempts: FetcherAttempt[]
  recentProbes?: ProbeRecord[]
  running: [string, string] | null
  runningRequests?: [string, string][]
  dynamicEgress?: {
    revision: number
    available: boolean
    message: string
    instances: Array<EgressInstance & { id: string, families: string[] }>
    history: Array<{ id: string, instance: string, family: string, state: string, ip: string | null, created: number, message: string, provider?: string }>
  }
}
export function configureDynamicEgress(data: { instances: Record<string, EgressInstance>, revision: number }) {
  return request<void>({
    url: '/api/admin/turn-state-fetcher/egress',
    method: 'POST',
    data,
  })
}
export function getFetcher(options: RequestOptions = {}) {
  return request<FetcherSnapshot>({
    url: '/api/admin/turn-state-fetcher',
    method: 'GET',
    ...options,
  })
}
export function configureFetcher(data: FetcherConfig) {
  return request<void>({
    url: '/api/admin/turn-state-fetcher/configure',
    method: 'POST',
    data,
  })
}
export function runFetcher(data: { accountId: string, model: string }) {
  return request<void>({
    url: '/api/admin/turn-state-fetcher/run',
    method: 'POST',
    data,
  })
}
