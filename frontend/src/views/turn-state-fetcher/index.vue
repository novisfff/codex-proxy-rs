<script setup lang="ts">
import type { Account, OutboundProxyRecord } from '@/api'
import type { EgressInstance, FetcherConfig, FetcherSnapshot } from '@/api/modules/turn-state-fetcher'
import { Copy, Pencil, RefreshCw } from '@lucide/vue'
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { getAccountDetail, getAccountModels, getAccounts, getProxies } from '@/api'
import { configureDynamicEgress, configureFetcher, getFetcher, runFetcher } from '@/api/modules/turn-state-fetcher'
import BaseButton from '@/components/base/BaseButton.vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseCheckbox from '@/components/base/BaseCheckbox.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BasePageHeader from '@/components/base/BasePageHeader.vue'
import BaseSelect from '@/components/base/BaseSelect.vue'
import BaseSwitch from '@/components/base/BaseSwitch.vue'
import { toast } from '@/components/base/BaseToast'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { useCopyText } from '@/composables/useCopyText'
import { formatDateTime } from '@/utils/date'

const accounts = ref<Account[]>([])
const proxies = ref<OutboundProxyRecord[]>([])
const snapshot = ref<FetcherSnapshot>({ configs: [], values: [], attempts: [], running: null })
const loading = ref(true)
const error = ref(false)
const now = ref(Date.now())
const search = ref('')
const tab = ref('accounts')
const dynamicInstance = ref('')
const dynamicFamily = ref('ipv4')
const instanceOpen = ref(false)
const instanceId = ref('')
const originalInstanceId = ref('')
const instanceRevision = ref(0)
const instanceForm = ref<EgressInstance>({ provider: 'azure', name: '', subscription: '', resourceGroup: '', location: '', bindings: {} })
const editingFamilies = ref<string[]>([])
const instanceFields = [
  { key: 'name', label: '名称' },
  { key: 'subscription', label: 'Azure 订阅 ID' },
  { key: 'resourceGroup', label: '公网 IP 资源组' },
  { key: 'location', label: 'Azure 区域' },
] as const
const bindingFields = [
  { key: 'resourceGroup', label: '网卡资源组' },
  { key: 'nic', label: '专用网卡名称' },
  { key: 'ipConfiguration', label: 'IP 配置名称' },
  { key: 'sourceIp', label: '私网源 IP' },
] as const
const runningRequests = computed(() => snapshot.value.runningRequests ?? (snapshot.value.running ? [snapshot.value.running] : []))
const instancesEditable = computed(() => !!snapshot.value.dynamicEgress?.available && !snapshot.value.configs.some(config => config.enabled && config.dynamicEgress) && !runningRequests.value.length)
const dynamicOptions = computed(() => snapshot.value.dynamicEgress?.instances.map(instance => ({ label: instance.name, value: instance.id })) ?? [])
const familyOptions = computed(() => (snapshot.value.dynamicEgress?.instances.find(instance => instance.id === dynamicInstance.value)?.families ?? []).map(family => ({ label: family === 'ipv4' ? 'IPv4' : 'IPv6', value: family })))
const editOpen = ref(false)
const form = ref<FetcherConfig>({ accountId: '', enabled: false, models: [], proxyId: null, probeProfile: 'minimal_compat', adaptiveConcurrency: true, revision: 0 })
const scheduleMode = ref('all')
const scheduleStart = ref('09:00')
const scheduleEnd = ref('01:00')
const proxyChoice = ref('')
const catalog = ref<Array<{ id: string, label: string }>>([])
const customModel = ref('')
const mode = ref('')
const formReady = ref(false)
const formError = ref(false)
const action = useAsyncAction()
const { loading: saving } = action
const copyText = useCopyText()
let timer: ReturnType<typeof setTimeout> | undefined
const controller = new AbortController()
let formGeneration = 0
let refreshGeneration = 0

const visible = computed(() => accounts.value.filter(a => `${a.name} ${a.email ?? ''}`.toLowerCase().includes(search.value.toLowerCase())))
const modelOptions = computed(() => [...new Map([...catalog.value, ...form.value.models.map(id => ({ id, label: id }))].map(m => [m.id, m])).values()])
const proxyOptions = computed(() => [
  { label: '直连 · 不使用代理', value: 'direct' },
  { label: '动态出口 · Azure / SOCKS5', value: 'dynamic' },
  ...proxies.value.map(p => ({ label: `${p.name} · ${p.endpoint}${p.lastTest?.success ? '' : '（未通过测试）'}`, value: p.id, disabled: !p.lastTest?.success })),
])

function editInstance(id = '') {
  const existing = snapshot.value.dynamicEgress?.instances.find(instance => instance.id === id)
  instanceId.value = id
  originalInstanceId.value = id
  instanceRevision.value = snapshot.value.dynamicEgress?.revision ?? 0
  instanceForm.value = existing ? { ...instanceConfig(existing), password: '', passwordSet: existing.passwordSet, bindings: JSON.parse(JSON.stringify(existing.bindings)) } : { provider: 'azure', name: '', subscription: '', resourceGroup: '', location: '', host: '', port: 1080, username: '', password: '', bindings: {} }
  editingFamilies.value = Object.keys(instanceForm.value.bindings)
  for (const family of ['ipv4', 'ipv6']) {
    instanceForm.value.bindings[family] ??= { resourceGroup: '', nic: '', ipConfiguration: '', sourceIp: '', dedicated: true }
  }
  instanceOpen.value = true
}
watch(() => instanceForm.value.provider, (provider) => {
  if (!originalInstanceId.value) {
    instanceForm.value.maxConcurrent = provider === 'socks5' ? 5 : 1
    instanceForm.value.intervalSeconds = provider === 'socks5' ? 1 : 10
  }
})

const profileOptions = [
  { label: '极简兼容（实验）', value: 'minimal_compat' },
  { label: '完整 Codex 请求', value: 'codex_core' },
]
function profileLabel(profile?: string) {
  return profile === 'minimal_compat' ? '极简兼容' : '完整 Codex'
}
const outcomeLabels: Record<string, string> = { captured: '已获取', non_target: '非目标长度', repeated: '重复旧值', failed: '未获取', cooldown: '冷却', paused: '需要处理', cancelled: '已取消', stale: '结果已失效' }
const historyAccount = ref('')
const historyModel = ref('')
const historyAccountOptions = computed(() => [{ label: '全部账号', value: '' }, ...accounts.value.map(a => ({ label: a.name, value: a.id }))])
const recentProbes = computed(() => (snapshot.value.recentProbes ?? []).filter(p => (!historyAccount.value || p.accountId === historyAccount.value) && (!historyModel.value.trim() || p.model.includes(historyModel.value.trim()))))
const probeSummary = computed(() => profileOptions.map(({ value, label }) => {
  const probes = recentProbes.value.filter(p => p.profile === value)
  const responses = probes.filter(p => p.httpStatus !== null)
  const count = (length: number) => responses.filter(p => p.httpStatus === 200 && p.byteLength === length).length
  return { label, total: probes.length, responses: responses.length, good: count(292), other: count(312) }
}))
function accountName(id: string) {
  return accounts.value.find(a => a.id === id)?.name ?? id
}

function instanceConfig(instance: EgressInstance): EgressInstance {
  const scheduling = { maxConcurrent: instance.provider === 'azure' ? 1 : instance.maxConcurrent ?? 1, intervalSeconds: instance.intervalSeconds ?? 10 }
  const config = ['socks5', 'novaproxy'].includes(instance.provider)
    ? { provider: 'socks5', name: instance.name, host: instance.host?.trim(), port: instance.port, username: instance.username, ...(instance.password ? { password: instance.password } : {}), bindings: { ipv4: {} } }
    : { provider: 'azure', name: instance.name, subscription: instance.subscription, resourceGroup: instance.resourceGroup, location: instance.location, bindings: instance.bindings }
  return { ...config, ...scheduling }
}
function isSocks5(accountId: string) {
  const id = config(accountId).dynamicEgress?.instance
  return snapshot.value.dynamicEgress?.instances.some(instance => instance.id === id && ['socks5', 'novaproxy'].includes(instance.provider))
}
function toggleFamily(family: string, enabled: boolean) {
  editingFamilies.value = enabled ? [...new Set([...editingFamilies.value, family])] : editingFamilies.value.filter(value => value !== family)
}
async function saveInstance(remove = false) {
  const proxy = instanceForm.value
  if (!remove && (!Number.isInteger(proxy.maxConcurrent ?? 1) || (proxy.maxConcurrent ?? 1) < 1 || (proxy.maxConcurrent ?? 1) > 16 || !Number.isInteger(proxy.intervalSeconds ?? 10) || (proxy.intervalSeconds ?? 10) < 0 || (proxy.intervalSeconds ?? 10) > 3600)) {
    toast.warning('并发数需为 1–16 的整数，尝试间隔需为 0–3600 秒的整数')
    return
  }
  const validSocks5 = !!proxy.host?.trim() && Number.isInteger(proxy.port) && (proxy.port ?? 0) >= 1 && (proxy.port ?? 0) <= 65535 && !!proxy.username && (!!proxy.password || proxy.passwordSet)
  if (!remove && (!/^[\w-]{1,64}$/.test(instanceId.value) || (proxy.provider === 'azure' ? !editingFamilies.value.length : !validSocks5))) {
    toast.warning(proxy.provider === 'socks5' ? '填写有效的实例 ID、SOCKS5 地址、端口、用户名和密码' : '填写实例 ID 并至少选择一种地址类型')
    return
  }
  await action.run(async () => {
    const instances: Record<string, EgressInstance> = {}
    for (const instance of snapshot.value.dynamicEgress?.instances ?? []) {
      if (instance.id !== originalInstanceId.value)
        instances[instance.id] = instanceConfig(instance)
    }
    if (!remove)
      instances[instanceId.value] = instanceConfig({ ...instanceForm.value, bindings: Object.fromEntries(editingFamilies.value.map(family => [family, instanceForm.value.bindings[family]!])) })
    await configureDynamicEgress({ instances, revision: instanceRevision.value })
    instanceForm.value.password = ''
    instanceOpen.value = false
    await refresh()
    toast.success(remove ? '实例已移除' : '实例已保存')
  })
}

function config(accountId: string): FetcherConfig {
  return snapshot.value.configs.find(c => c.accountId === accountId) ?? { accountId, enabled: false, models: [], proxyId: null, probeProfile: 'minimal_compat', adaptiveConcurrency: true, revision: 0 }
}
function timeLabel(minute: number) {
  return `${String(Math.floor(minute / 60)).padStart(2, '0')}:${String(minute % 60).padStart(2, '0')}`
}
function timeMinute(value: string) {
  if (!/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(value))
    return null
  const [hour, minute] = value.split(':').map(Number)
  return hour! * 60 + minute!
}
function scheduleLabel(selected: FetcherConfig) {
  const schedule = selected.schedule
  return schedule ? `${timeLabel(schedule.startMinute)}–${schedule.startMinute > schedule.endMinute ? '次日 ' : ''}${timeLabel(schedule.endMinute)}（北京时间）` : '全天'
}
function inSchedule(selected: FetcherConfig) {
  const schedule = selected.schedule
  if (!schedule)
    return true
  const minute = Math.floor((now.value + 8 * 3600000) % 86400000 / 60000)
  return schedule.startMinute < schedule.endMinute
    ? minute >= schedule.startMinute && minute < schedule.endMinute
    : minute >= schedule.startMinute || minute < schedule.endMinute
}
function rows(accountId: string) {
  const selected = config(accountId)
  return selected.models.map((model) => {
    const value = snapshot.value.values.find(v => v.accountId === accountId && v.model === model)
    const attempt = snapshot.value.attempts.find(a => a.accountId === accountId && a.model === model && a.configRevision === selected.revision)
    const running = runningRequests.value.filter(([account, currentModel]) => account === accountId && currentModel === model).length
    const expired = !!value && value.expiresAt <= now.value
    const cooling = attempt?.status === 'cooldown' && attempt.nextAttemptAt > now.value
    const status = !selected.enabled ? '已暂停' : !inSchedule(selected) ? '时段外 · 等待开启' : running ? `获取中 · ${running}` : attempt?.paused ? '需要处理' : cooling ? '限流冷却' : !value ? '等待获取' : expired ? '已过期' : value.expiresAt - now.value <= 1200000 ? '待续期' : '有效'
    return { model, value, attempt, running, expired, cooling, status }
  })
}
function date(value?: number) {
  return value ? formatDateTime(new Date(value).toISOString()) : '—'
}
function remaining(expires: number) {
  const seconds = Math.max(0, Math.floor((expires - now.value) / 1000))
  return seconds ? `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒` : '已过期，不再携带'
}
function proxyName(id: string | null) {
  return id ? proxies.value.find(p => p.id === id)?.name ?? '代理不可用' : '直连'
}

async function refresh() {
  const generation = ++refreshGeneration
  clearTimeout(timer)
  try {
    const data = await getFetcher({ silent: true, signal: controller.signal })
    if (!controller.signal.aborted && generation === refreshGeneration) {
      snapshot.value = data
      now.value = Date.now()
      error.value = false
    }
  }
  catch {
    if (!controller.signal.aborted && generation === refreshGeneration)
      error.value = true
  }
  finally {
    if (generation === refreshGeneration)
      loading.value = false
    if (!controller.signal.aborted && generation === refreshGeneration)
      timer = setTimeout(refresh, 5000)
  }
}
async function load() {
  try {
    const loaded: Account[] = []
    for (let page = 1; ; page++) {
      const result = await getAccounts({ page, pageSize: 100, provider: 'openai' }, { signal: controller.signal })
      loaded.push(...result.items)
      if (page >= result.page.totalPages)
        break
    }
    accounts.value = loaded
    const loadedProxies: OutboundProxyRecord[] = []
    for (let page = 1; ; page++) {
      const result = await getProxies({ page, pageSize: 100 }, { signal: controller.signal })
      loadedProxies.push(...result.items)
      if (page >= result.page.totalPages)
        break
    }
    proxies.value = loadedProxies
    await refresh()
  }
  catch {
    error.value = true
    loading.value = false
  }
}
async function edit(account: Account) {
  const generation = ++formGeneration
  form.value = { ...config(account.id), probeProfile: config(account.id).probeProfile ?? 'minimal_compat', adaptiveConcurrency: config(account.id).adaptiveConcurrency ?? true, models: [...config(account.id).models] }
  scheduleMode.value = form.value.schedule ? 'custom' : 'all'
  scheduleStart.value = timeLabel(form.value.schedule?.startMinute ?? 540)
  scheduleEnd.value = timeLabel(form.value.schedule?.endMinute ?? 60)
  proxyChoice.value = form.value.revision ? form.value.proxyId ?? 'direct' : ''
  if (form.value.dynamicEgress)
    proxyChoice.value = 'dynamic'
  dynamicInstance.value = form.value.dynamicEgress?.instance ?? ''
  dynamicFamily.value = form.value.dynamicEgress?.family ?? 'ipv4'
  customModel.value = ''
  catalog.value = []
  mode.value = ''
  formReady.value = false
  formError.value = false
  editOpen.value = true
  try {
    const [detail, models] = await Promise.all([getAccountDetail({ accountId: account.id }), getAccountModels({ accountId: account.id }, { silent: true }).catch(() => null)])
    if (generation !== formGeneration)
      return
    catalog.value = models?.models ?? []
    if (!models)
      toast.warning('模型目录暂不可用，可以手动输入上游模型名')
    mode.value = detail.credentialConfiguration?.codex_turn_state.mode ?? 'default'
    formReady.value = true
  }
  catch {
    if (generation === formGeneration) {
      formError.value = true
      toast.error('账号配置读取失败，请关闭后重试')
    }
  }
}
function toggleModel(id: string, checked: boolean) {
  form.value.models = checked ? [...new Set([...form.value.models, id])] : form.value.models.filter(m => m !== id)
}
function addModel() {
  const model = customModel.value.trim()
  if (model && model.length <= 128) {
    toggleModel(model, true)
    customModel.value = ''
  }
}
async function save() {
  const startMinute = timeMinute(scheduleStart.value)
  const endMinute = timeMinute(scheduleEnd.value)
  if (scheduleMode.value === 'custom' && (startMinute === null || endMinute === null || startMinute === endMinute)) {
    toast.warning('请填写有效且不同的开始、结束时间；全天探测请选择“全天”')
    return
  }
  form.value.schedule = scheduleMode.value === 'custom' ? { startMinute: startMinute!, endMinute: endMinute! } : null
  if (!formReady.value || !proxyChoice.value) {
    toast.warning('请选择获取专用代理或直连')
    return
  }
  if (form.value.models.length > 32 || (form.value.enabled && !form.value.models.length)) {
    toast.warning('启用时请选择 1～32 个模型')
    return
  }
  await action.run(async () => {
    if (proxyChoice.value === 'dynamic' && (!dynamicInstance.value || !familyOptions.value.some(option => option.value === dynamicFamily.value))) {
      toast.warning('请选择可用的动态出口实例和地址类型')
      return
    }
    await configureFetcher({ ...form.value, proxyId: ['direct', 'dynamic'].includes(proxyChoice.value) ? null : proxyChoice.value, dynamicEgress: proxyChoice.value === 'dynamic' ? { instance: dynamicInstance.value, family: dynamicFamily.value } : null })
    editOpen.value = false
    await refresh()
    toast.success('获取器配置已保存')
  })
}
async function pause(accountId: string) {
  await action.run(async () => {
    await configureFetcher({ ...config(accountId), enabled: false })
    await refresh()
  })
}
async function run(accountId: string, model: string) {
  await action.run(async () => {
    await runFetcher({ accountId, model })
    await refresh()
    toast.success('已排队，后台将按出口策略获取')
  })
}
onMounted(load)
onBeforeUnmount(() => {
  controller.abort()
  clearTimeout(timer)
  formGeneration++
})
</script>

<template>
  <div class="flex flex-col gap-5">
    <BasePageHeader title="292 获取器" description="为每个账号、每个模型维护有效的 Turn State，使用独立出口提前续期。">
      <template #actions>
        <BaseButton :disabled="loading" @click="load">
          <RefreshCw class="size-4" />刷新
        </BaseButton>
      </template>
    </BasePageHeader>
    <div class="flex gap-2" aria-label="获取器页面">
      <BaseButton :variant="tab === 'accounts' ? 'primary' : 'secondary'" @click="tab = 'accounts'">
        账号与模型
      </BaseButton>
      <BaseButton :variant="tab === 'egress' ? 'primary' : 'secondary'" @click="tab = 'egress'">
        动态出口
      </BaseButton>
      <BaseButton :variant="tab === 'history' ? 'primary' : 'secondary'" @click="tab = 'history'">
        探测记录
      </BaseButton>
    </div>
    <BaseCard v-if="tab === 'history'" title="最近探测" description="保留全站最近 200 条尝试。按账号、模型筛选后对照请求模式；不同时间和出口的结果不能直接视为等量实验。">
      <div class="grid gap-3 sm:grid-cols-2">
        <BaseSelect v-model="historyAccount" :options="historyAccountOptions" aria-label="筛选探测账号" />
        <BaseInput v-model="historyModel" placeholder="筛选模型" aria-label="筛选探测模型" />
      </div>
      <div class="my-4 grid gap-3 sm:grid-cols-2">
        <div v-for="summary in probeSummary" :key="summary.label" class="rounded-cp bg-cp-fill-quaternary p-3 text-cp-sm">
          <strong>{{ summary.label }}</strong>
          <p>{{ summary.total }} 次尝试 · {{ summary.responses }} 次收到 HTTP 响应</p>
          <p>HTTP 200：292 × {{ summary.good }} · 312 × {{ summary.other }}</p>
        </div>
      </div>
      <p v-if="!recentProbes.length" class="text-cp-sm text-cp-text-secondary">
        暂无匹配的探测记录。
      </p>
      <div class="grid gap-3">
        <section v-for="probe in recentProbes" :key="probe.id" class="min-w-0 rounded-cp bg-cp-fill-quaternary p-3 text-cp-sm">
          <div class="flex flex-wrap gap-2">
            <strong class="break-all">{{ accountName(probe.accountId) }} · {{ probe.model }}</strong>
            <span>{{ profileLabel(probe.profile) }}</span>
            <span :class="probe.outcome === 'captured' ? 'text-cp-success' : 'text-cp-text-secondary'">{{ outcomeLabels[probe.outcome] ?? probe.outcome }}</span>
          </div>
          <p class="text-cp-text-secondary">
            {{ date(probe.startedAt) }} · HTTP {{ probe.httpStatus ?? '未收到' }} · {{ probe.byteLength ?? '无' }} 字节 · {{ probe.durationMs }} ms<span v-if="probe.repeated"> · 重复票据</span>
          </p>
          <details class="text-cp-xs text-cp-text-secondary">
            <summary class="cursor-pointer">
              连接与批次详情
            </summary>
            <div class="mt-2 grid gap-1 break-all">
              <span>批次：{{ probe.batchId }} · 配置版本：{{ probe.configRevision }}</span>
              <span>请求：{{ probe.endpoint ?? '未发送' }} · {{ probe.httpVersion ?? '协议未知' }} · Lite {{ probe.responsesLite ? '开' : '关' }} · {{ probe.compressed ? 'zstd' : '普通 JSON' }}</span>
              <span>出口：{{ probe.egressInstance ?? '静态 / 直连' }} · IP {{ probe.exitIp ?? '未验证' }} · {{ probe.freshConnection ? '独立连接' : '允许连接复用 / 尚未连接' }}</span>
              <span>租约：{{ probe.leaseId ?? '无' }} · 尝试：{{ probe.id }}</span>
            </div>
          </details>
        </section>
      </div>
    </BaseCard>
    <BaseCard v-if="tab === 'egress'" title="专用动态出口" description="Azure 独占 IP；SOCKS5H sticky 代理。仅用于 292 获取器。">
      <p :class="snapshot.dynamicEgress?.available ? 'text-cp-success' : 'text-cp-warning'">
        {{ snapshot.dynamicEgress?.available ? '出口服务已就绪' : snapshot.dynamicEgress?.message || '尚未配置出口服务' }}
      </p>
      <p class="text-cp-sm text-cp-text-secondary">
        Azure 获取到 292 时保留 IP 并优先复用，未获取到时换 IP，新申请避免连续重复。SOCKS5H 使用用户名 SID 维持 sticky 会话，实际出口未验证。
      </p>
      <BaseButton :disabled="!instancesEditable || saving" @click="editInstance()">
        添加出口实例
      </BaseButton>
      <p class="text-cp-xs text-cp-text-secondary">
        修改实例前，请暂停使用动态出口的获取账号，并等待当前任务释放。保存配置不会立即申请公网 IP。
      </p>
      <div v-for="instance in snapshot.dynamicEgress?.instances" :key="instance.id" class="my-3 flex flex-wrap gap-3">
        <strong>{{ instance.name }}</strong><span>{{ instance.families.join(' / ') }}</span>
        <span>并发 {{ instance.maxConcurrent ?? 1 }} · 间隔 {{ instance.intervalSeconds ?? 10 }} 秒</span>
        <BaseButton size="sm" :disabled="!instancesEditable || saving" @click="editInstance(instance.id)">
          编辑
        </BaseButton>
      </div>
      <div class="mt-4 grid gap-3">
        <section v-for="entry in snapshot.dynamicEgress?.history" :key="entry.id" class="min-w-0 rounded-cp bg-cp-fill-quaternary p-3 text-cp-sm">
          <div class="flex flex-wrap gap-3">
            <strong class="break-all">{{ entry.ip || (['socks5', 'novaproxy'].includes(entry.provider ?? '') ? '代理出口 · IP 未验证' : '等待分配') }}</strong><span>{{ entry.family }}</span><span>{{ ({ provisioning: '正在分配', ready: '等待请求', connected: '请求进行中', released: '已释放', failed: '失败' } as Record<string, string>)[entry.state] || entry.state }}</span>
          </div>
          <p class="text-cp-text-secondary">
            {{ date(entry.created * 1000) }} · {{ entry.instance }}
          </p><p v-if="entry.message" class="text-cp-error">
            {{ entry.message }}
          </p>
        </section>
      </div>
    </BaseCard>
    <template v-if="tab === 'accounts'">
      <BaseCard>
        <p class="m-0 text-cp-sm text-cp-text-secondary">
          预计有效 60 分钟，剩余 20 分钟开始续期。动态 SOCKS5 支持并发搜索；普通请求返回新值时自动顺延。相同值不会延长有效期。
        </p>
        <BaseInput v-model="search" class="mt-4 max-w-md" placeholder="搜索账号名称或邮箱" aria-label="搜索获取器账号" />
      </BaseCard>
      <p v-if="error" role="alert" class="text-cp-error">
        读取失败，当前显示可能不是最新状态。请点击刷新重试。
      </p>
      <p v-if="loading" role="status" class="text-cp-text-secondary">
        正在读取获取器…
      </p>
      <BaseCard v-for="account in visible" :key="account.id" :title="account.name" :description="account.email ?? undefined">
        <div class="flex flex-wrap items-center justify-between gap-3">
          <span class="text-cp-sm text-cp-text-secondary">{{ config(account.id).enabled ? '自动获取已开启' : '自动获取未开启' }} · 专用出口：{{ config(account.id).dynamicEgress ? `${config(account.id).dynamicEgress?.instance} / ${config(account.id).dynamicEgress?.family}` : proxyName(config(account.id).proxyId) }}</span>
          <div class="flex gap-2">
            <BaseButton v-if="config(account.id).enabled" :disabled="saving" @click="pause(account.id)">
              暂停
            </BaseButton>
            <BaseButton :disabled="saving || error" @click="edit(account)">
              <Pencil class="size-4" />配置
            </BaseButton>
          </div>
        </div>
        <p v-if="!config(account.id).models.length" class="text-cp-sm text-cp-text-tertiary">
          尚未选择模型。配置后可自动获取，也可手动触发一次。
        </p>
        <p class="text-cp-sm text-cp-text-secondary">
          探测时段：{{ scheduleLabel(config(account.id)) }} · {{ profileLabel(config(account.id).probeProfile) }} · {{ config(account.id).adaptiveConcurrency ? '自适应并发' : '固定并发' }}
        </p>
        <div class="mt-4 grid gap-4">
          <section v-for="row in rows(account.id)" :key="row.model" class="min-w-0 rounded-cp bg-cp-fill-quaternary p-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div class="flex min-w-0 flex-wrap items-center gap-3">
                <strong class="break-all text-cp-text">{{ row.model }}</strong><span :class="row.status === '有效' ? 'text-cp-success' : row.expired || row.attempt?.paused ? 'text-cp-error' : 'text-cp-text-secondary'">{{ row.status }}</span>
              </div>
              <BaseButton size="sm" :disabled="saving || error || !!row.running || !config(account.id).enabled || !inSchedule(config(account.id)) || row.attempt?.paused || (!!row.attempt?.failures && row.attempt.nextAttemptAt > now)" @click="run(account.id, row.model)">
                立即获取
              </BaseButton>
            </div>
            <div v-if="row.value" class="mt-3 grid gap-2 text-cp-sm text-cp-text-secondary">
              <div class="flex flex-wrap items-center gap-2">
                <span>292 字节 · {{ row.value.source === 'fetcher' ? '获取器' : '正常请求' }} · {{ remaining(row.value.expiresAt) }}</span><BaseIconButton :label="`复制 ${row.model} 的 Turn State`" @click="copyText(row.value.value, { successText: 'Turn State 已复制' })">
                  <Copy class="size-4" />
                </BaseIconButton>
              </div>
              <div v-if="row.value.proxyUrl" class="flex min-w-0 flex-wrap items-center gap-2">
                <span class="break-all">探测代理（含认证信息）：{{ row.value.proxyUrl }}</span><BaseIconButton :label="`复制 ${row.model} 的探测代理`" @click="copyText(row.value.proxyUrl, { successText: '探测代理已复制' })">
                  <Copy class="size-4" />
                </BaseIconButton>
              </div>
              <span v-else class="text-cp-text-tertiary">探测代理：未绑定（该值来自正常请求或旧版本数据）</span>
              <span>首次获取：{{ date(row.value.acquiredAt) }} · 最近收到：{{ date(row.value.lastSeenAt) }}</span>
              <span>预计到期：{{ date(row.value.expiresAt) }}</span>
              <details>
                <summary class="cursor-pointer">
                  查看当前值
                </summary><pre class="max-h-32 overflow-auto whitespace-pre-wrap break-all font-mono text-cp-xs">{{ row.value.value }}</pre>
              </details>
            </div>
            <p v-else class="text-cp-sm text-cp-text-tertiary">
              尚无有效值，正常自动请求暂不携带此头。
            </p>
            <div v-if="row.attempt" class="mt-3 grid gap-1 text-cp-xs text-cp-text-secondary">
              <span>最近尝试：{{ date(row.attempt.attemptedAt) }} · {{ row.attempt.message }}</span>
              <span v-if="row.attempt.exitIp" class="break-all">本次出口 IP：{{ row.attempt.exitIp }}</span>
              <span v-else-if="isSocks5(account.id)">本次出口 IP：未验证（SOCKS5）</span>
              <span>返回长度：{{ row.attempt.byteLength ?? '未返回' }} · 耗时：{{ row.attempt.durationMs }} ms · 输入 / 输出 token：{{ row.attempt.inputTokens ?? '未知' }} / {{ row.attempt.outputTokens ?? '未知' }}</span>
              <span v-if="config(account.id).adaptiveConcurrency && isSocks5(account.id)">搜索目标并发：{{ row.attempt.searchConcurrency ?? 3 }}（仍受出口和账号上限约束）</span>
              <span v-if="config(account.id).enabled && !row.attempt.paused">下次检查：{{ date(Math.max(row.attempt.nextAttemptAt, row.value && row.attempt.status !== 'queued' ? row.value.expiresAt - 1200000 : 0)) }}</span>
            </div>
          </section>
        </div>
      </BaseCard>
      <p v-if="!loading && !visible.length" class="text-cp-text-secondary">
        没有匹配的 OpenAI 账号。
      </p>
    </template>
    <BaseModal v-model="instanceOpen" title="动态出口实例" size="md" :dismissible="!saving">
      <div class="grid gap-4">
        <BaseFormItem label="出口供应商">
          <BaseSelect v-model="instanceForm.provider" :options="[{ label: 'Azure', value: 'azure' }, { label: 'SOCKS5H（sticky）', value: 'socks5' }]" :disabled="saving || !!originalInstanceId" />
        </BaseFormItem>
        <BaseFormItem label="实例 ID">
          <BaseInput v-model="instanceId" :disabled="saving || !!originalInstanceId" :placeholder="instanceForm.provider === 'socks5' ? 'socks5-main' : 'azure-main'" />
        </BaseFormItem>
        <div class="grid grid-cols-2 gap-4">
          <BaseFormItem :label="instanceForm.provider === 'azure' ? '最大并发数（Azure 固定 1）' : '最大并发数'">
            <BaseInput :model-value="String(instanceForm.provider === 'azure' ? 1 : instanceForm.maxConcurrent ?? 5)" type="number" min="1" max="16" :disabled="saving || instanceForm.provider === 'azure'" @update:model-value="instanceForm.maxConcurrent = Number($event)" />
          </BaseFormItem>
          <BaseFormItem label="尝试间隔（秒）">
            <BaseInput :model-value="String(instanceForm.intervalSeconds ?? (instanceForm.provider === 'socks5' ? 1 : 10))" type="number" min="0" max="3600" :disabled="saving" @update:model-value="instanceForm.intervalSeconds = Number($event)" />
          </BaseFormItem>
        </div>
        <template v-if="instanceForm.provider === 'socks5'">
          <BaseFormItem label="名称">
            <BaseInput v-model="instanceForm.name" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="代理地址">
            <BaseInput v-model="instanceForm.host" placeholder="代理域名或 IP，如 us.novproxy.io" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="端口">
            <BaseInput :model-value="String(instanceForm.port ?? '')" type="number" min="1" max="65535" :disabled="saving" @update:model-value="instanceForm.port = Number($event)" />
          </BaseFormItem>
          <BaseFormItem label="代理用户名模板">
            <BaseInput v-model="instanceForm.username" placeholder="例如 r_xxx-sid-__CPR_292_SID__-ttl-1440m" autocomplete="off" :disabled="saving" />
            <p class="text-cp-xs text-cp-text-secondary">
              使用 `__CPR_292_SID__` 占位符；每次 292 探测会替换成新的 12 位小写字母和数字 SID，并保持供应商 sticky 会话。
            </p>
          </BaseFormItem>
          <BaseFormItem :label="instanceForm.passwordSet ? '代理密码（已配置，留空保留）' : '代理密码'">
            <BaseInput v-model="instanceForm.password" type="password" autocomplete="new-password" :disabled="saving" />
          </BaseFormItem>
          <p class="text-cp-sm text-cp-warning">
            每次建立独立 SOCKS5H 连接；出口由用户名 SID 维持 sticky 会话，实际 IP 未验证。
          </p>
        </template>
        <template v-else>
          <BaseFormItem v-for="field in instanceFields" :key="field.key" :label="field.label">
            <BaseInput v-model="instanceForm[field.key]" :disabled="saving" />
          </BaseFormItem>
          <section v-for="family in ['ipv4', 'ipv6']" :key="family" class="grid gap-3">
            <BaseCheckbox :label="`启用 ${family === 'ipv4' ? 'IPv4' : 'IPv6'}`" show-label :model-value="editingFamilies.includes(family)" :disabled="saving" @update:model-value="toggleFamily(family, $event)" />
            <template v-if="editingFamilies.includes(family) && instanceForm.bindings[family]">
              <BaseFormItem v-for="field in bindingFields" :key="field.key" :label="field.label">
                <BaseInput v-model="instanceForm.bindings[family]![field.key]" :disabled="saving" />
              </BaseFormItem>
            </template>
          </section>
          <p class="text-cp-sm text-cp-warning">
            必须使用已准备好的专用 IP 配置，且没有其他服务使用。IPv4 不允许使用主 IP 配置；操作系统需预先配置私网源 IP。公网 IP 会产生 Azure 费用。
          </p>
        </template>
      </div>
      <template #footer>
        <BaseButton v-if="originalInstanceId" :disabled="saving" @click="saveInstance(true)">
          移除实例
        </BaseButton>
        <BaseButton :disabled="saving" @click="instanceOpen = false">
          取消
        </BaseButton>
        <BaseButton variant="primary" :loading="saving" @click="saveInstance()">
          保存实例
        </BaseButton>
      </template>
    </BaseModal>
    <BaseModal v-model="editOpen" title="配置 292 获取器" size="md" :dismissible="!saving">
      <p v-if="formError" role="alert" class="text-cp-error">
        账号配置读取失败，请关闭后重新打开。
      </p>
      <div class="grid gap-5">
        <div class="flex items-center justify-between">
          <span class="text-cp-text">自动获取</span><BaseSwitch v-model="form.enabled" label="开启自动获取" :disabled="saving" />
        </div>
        <BaseFormItem label="探测请求模式">
          <BaseSelect v-model="form.probeProfile" :options="profileOptions" :disabled="saving" />
          <p class="text-cp-xs text-cp-text-secondary">
            极简兼容用于 OAuth 账号的对照探测，可随时切回完整模式。API Key 账号沿用公开接口格式。模式不改变业务请求。
          </p>
        </BaseFormItem>
        <div class="flex items-center justify-between gap-3">
          <span>自适应并发（动态 SOCKS5）</span><BaseSwitch v-model="form.adaptiveConcurrency" label="启用自适应并发" :disabled="saving" />
        </div>
        <p class="text-cp-xs text-cp-text-secondary">
          从 3 路开始，收到 312 后升至 5、8 路，始终受出口实例和账号上限约束。429/503 冷却后从 1 路恢复；命中后取消其他搜索。
        </p>
        <BaseFormItem label="探测时段">
          <BaseSelect v-model="scheduleMode" :options="[{ label: '全天', value: 'all' }, { label: '固定时段', value: 'custom' }]" :disabled="saving" />
        </BaseFormItem>
        <div v-if="scheduleMode === 'custom'" class="grid gap-3 sm:grid-cols-2">
          <BaseFormItem label="开始时间（北京时间）">
            <BaseInput v-model="scheduleStart" type="time" aria-label="探测开始时间" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="结束时间（北京时间）">
            <BaseInput v-model="scheduleEnd" type="time" aria-label="探测结束时间" :disabled="saving" />
          </BaseFormItem>
        </div>
        <p class="text-cp-xs text-cp-text-secondary">
          每天按北京时间执行，支持跨午夜（如 09:00–次日 01:00）。时段外停止探测，已有有效 Turn State 仍可正常使用。
        </p>
        <BaseFormItem label="获取专用代理">
          <BaseSelect v-model="proxyChoice" :options="proxyOptions" placeholder="请选择代理或直连" :disabled="saving" /><p class="text-cp-xs text-cp-text-secondary">
            只用于获取任务，不修改账号的业务代理。代理失败时不会回退到直连。
          </p>
        </BaseFormItem>
        <template v-if="proxyChoice === 'dynamic'">
          <BaseFormItem label="动态出口实例">
            <BaseSelect v-model="dynamicInstance" :options="dynamicOptions" placeholder="选择专用出口" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="地址类型">
            <BaseSelect v-model="dynamicFamily" :options="familyOptions" :disabled="saving" />
          </BaseFormItem>
          <p class="text-cp-xs text-cp-text-secondary">
            Azure 等待新 IP 就绪；SOCKS5H 每次新建代理连接并使用远程 DNS。仅请求官方 OpenAI，不使用账号自定义网关或业务代理。
          </p>
        </template>
        <BaseFormItem label="获取模型（最多 32 个）">
          <p v-if="!formReady && !formError" role="status" class="text-cp-text-secondary">
            正在读取账号配置…
          </p>
          <div class="grid max-h-60 gap-3 overflow-auto sm:grid-cols-2">
            <BaseCheckbox v-for="item in modelOptions" :key="item.id" :label="item.label" show-label :model-value="form.models.includes(item.id)" :disabled="saving" @update:model-value="toggleModel(item.id, $event)" />
          </div>
          <div class="mt-3 flex gap-2">
            <BaseInput v-model="customModel" placeholder="手动输入上游模型名" aria-label="上游模型名" :maxlength="128" :disabled="saving" /><BaseButton :disabled="saving || !customModel.trim()" @click="addModel">
              添加
            </BaseButton>
          </div>
        </BaseFormItem>
        <p v-if="mode && mode !== 'auto'" class="text-cp-sm text-cp-warning">
          此账号的请求头模式不是“自动”。获取的值会保存，但不会用于正常请求；请在账号编辑中切换模式。
        </p>
        <p class="text-cp-xs text-cp-text-secondary">
          使用无历史的简短请求，不携带 Turn State。动态出口按实例的并发数和启动间隔搜索，其他出口未获得有效新值时等待 10 秒重试；429/503 至少冷却 30 秒并遵循 Retry-After（最多 1 小时），账号或模型错误需处理。
        </p>
      </div>
      <template #footer>
        <BaseButton :disabled="saving" @click="editOpen = false">
          取消
        </BaseButton><BaseButton variant="primary" :loading="saving" :disabled="!formReady" @click="save">
          保存配置
        </BaseButton>
      </template>
    </BaseModal>
  </div>
</template>
