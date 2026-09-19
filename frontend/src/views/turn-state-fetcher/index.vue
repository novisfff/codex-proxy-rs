<script setup lang="ts">
import type { Account, OutboundProxyRecord } from '@/api'
import type { EgressInstance, FetcherConfig, FetcherSnapshot } from '@/api/modules/turn-state-fetcher'
import { Copy, Pencil, RefreshCw } from '@lucide/vue'
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
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
const instancesEditable = computed(() => !!snapshot.value.dynamicEgress?.available && !snapshot.value.configs.some(config => config.enabled && config.dynamicEgress) && !snapshot.value.running)
const dynamicOptions = computed(() => snapshot.value.dynamicEgress?.instances.map(instance => ({ label: instance.name, value: instance.id })) ?? [])
const familyOptions = computed(() => (snapshot.value.dynamicEgress?.instances.find(instance => instance.id === dynamicInstance.value)?.families ?? []).map(family => ({ label: family === 'ipv4' ? 'IPv4' : 'IPv6', value: family })))
const editOpen = ref(false)
const form = ref<FetcherConfig>({ accountId: '', enabled: false, models: [], proxyId: null, revision: 0 })
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
  { label: '动态出口 · Azure / NovaProxy', value: 'dynamic' },
  ...proxies.value.map(p => ({ label: `${p.name} · ${p.endpoint}${p.lastTest?.success ? '' : '（未通过测试）'}`, value: p.id, disabled: !p.lastTest?.success })),
])

function editInstance(id = '') {
  const existing = snapshot.value.dynamicEgress?.instances.find(instance => instance.id === id)
  instanceId.value = id
  originalInstanceId.value = id
  instanceRevision.value = snapshot.value.dynamicEgress?.revision ?? 0
  instanceForm.value = existing ? { ...instanceConfig(existing), bindings: JSON.parse(JSON.stringify(existing.bindings)) } : { provider: 'azure', name: '', subscription: '', resourceGroup: '', location: '', credentialRef: '', bindings: {} }
  editingFamilies.value = Object.keys(instanceForm.value.bindings)
  for (const family of ['ipv4', 'ipv6']) {
    instanceForm.value.bindings[family] ??= { resourceGroup: '', nic: '', ipConfiguration: '', sourceIp: '', dedicated: true }
  }
  instanceOpen.value = true
}
function instanceConfig(instance: EgressInstance): EgressInstance {
  return instance.provider === 'novaproxy'
    ? { provider: 'novaproxy', name: instance.name, credentialRef: instance.credentialRef, bindings: { ipv4: {} } }
    : { provider: 'azure', name: instance.name, subscription: instance.subscription, resourceGroup: instance.resourceGroup, location: instance.location, bindings: instance.bindings }
}
function isNova(accountId: string) {
  const id = config(accountId).dynamicEgress?.instance
  return snapshot.value.dynamicEgress?.instances.some(instance => instance.id === id && instance.provider === 'novaproxy')
}
function toggleFamily(family: string, enabled: boolean) {
  editingFamilies.value = enabled ? [...new Set([...editingFamilies.value, family])] : editingFamilies.value.filter(value => value !== family)
}
async function saveInstance(remove = false) {
  if (!remove && (!/^[\w-]{1,64}$/.test(instanceId.value) || (instanceForm.value.provider === 'azure' ? !editingFamilies.value.length : !/^[\w-]{1,64}$/.test(instanceForm.value.credentialRef ?? '')))) {
    toast.warning(instanceForm.value.provider === 'novaproxy' ? '填写有效的实例 ID 和服务器凭据名称' : '填写实例 ID 并至少选择一种地址类型')
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
    instanceOpen.value = false
    await refresh()
    toast.success(remove ? '实例已移除' : '实例已保存')
  })
}

function config(accountId: string): FetcherConfig {
  return snapshot.value.configs.find(c => c.accountId === accountId) ?? { accountId, enabled: false, models: [], proxyId: null, revision: 0 }
}
function rows(accountId: string) {
  const selected = config(accountId)
  return selected.models.map((model) => {
    const value = snapshot.value.values.find(v => v.accountId === accountId && v.model === model)
    const attempt = snapshot.value.attempts.find(a => a.accountId === accountId && a.model === model && a.configRevision === selected.revision)
    const running = snapshot.value.running?.[0] === accountId && snapshot.value.running?.[1] === model
    const expired = !!value && value.expiresAt <= now.value
    const status = !selected.enabled ? '已暂停' : running ? '获取中' : attempt?.paused ? '需要处理' : !value ? '等待获取' : expired ? '已过期' : value.expiresAt - now.value <= 1200000 ? '待续期' : '有效'
    return { model, value, attempt, running, expired, status }
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
  form.value = { ...config(account.id), models: [...config(account.id).models] }
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
    toast.success('已排队，后台将依次获取')
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
    </div>
    <BaseCard v-if="tab === 'egress'" title="专用动态出口" description="Azure 独占 IP；NovaProxy Rotating 住宅代理。仅用于 292 获取器。">
      <p :class="snapshot.dynamicEgress?.available ? 'text-cp-success' : 'text-cp-warning'">
        {{ snapshot.dynamicEgress?.available ? '出口服务已就绪' : snapshot.dynamicEgress?.message || '尚未配置出口服务' }}
      </p>
      <p class="text-cp-sm text-cp-text-secondary">
        Azure 出口校验并保持 24 小时不重复。NovaProxy 每次新建连接，实际出口未验证，可能重复。连接失败不会回退到其他出口。
      </p>
      <BaseButton :disabled="!instancesEditable || saving" @click="editInstance()">
        添加出口实例
      </BaseButton>
      <p class="text-cp-xs text-cp-text-secondary">
        修改实例前，请暂停使用动态出口的获取账号，并等待当前任务释放。保存配置不会立即申请公网 IP。
      </p>
      <div v-for="instance in snapshot.dynamicEgress?.instances" :key="instance.id" class="my-3 flex flex-wrap gap-3">
        <strong>{{ instance.name }}</strong><span>{{ instance.families.join(' / ') }}</span>
        <BaseButton size="sm" :disabled="!instancesEditable || saving" @click="editInstance(instance.id)">
          编辑
        </BaseButton>
      </div>
      <div class="mt-4 grid gap-3">
        <section v-for="entry in snapshot.dynamicEgress?.history" :key="entry.id" class="min-w-0 rounded-cp bg-cp-fill-quaternary p-3 text-cp-sm">
          <div class="flex flex-wrap gap-3">
            <strong class="break-all">{{ entry.ip || (entry.provider === 'novaproxy' ? '轮换出口 · IP 未验证' : '等待分配') }}</strong><span>{{ entry.family }}</span><span>{{ ({ provisioning: '正在分配', ready: '等待请求', connected: '请求进行中', released: '已释放', failed: '失败' } as Record<string, string>)[entry.state] || entry.state }}</span>
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
          预计有效 60 分钟，剩余 20 分钟开始续期。全站一次获取一个任务；普通请求返回新值时自动顺延。相同值不会延长有效期。
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
        <div class="mt-4 grid gap-4">
          <section v-for="row in rows(account.id)" :key="row.model" class="min-w-0 rounded-cp bg-cp-fill-quaternary p-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
              <div class="flex min-w-0 flex-wrap items-center gap-3">
                <strong class="break-all text-cp-text">{{ row.model }}</strong><span :class="row.status === '有效' ? 'text-cp-success' : row.expired || row.attempt?.paused ? 'text-cp-error' : 'text-cp-text-secondary'">{{ row.status }}</span>
              </div>
              <BaseButton size="sm" :disabled="saving || error || row.running || !config(account.id).enabled || row.attempt?.paused || (!!row.attempt?.failures && row.attempt.nextAttemptAt > now)" @click="run(account.id, row.model)">
                立即获取
              </BaseButton>
            </div>
            <div v-if="row.value" class="mt-3 grid gap-2 text-cp-sm text-cp-text-secondary">
              <div class="flex flex-wrap items-center gap-2">
                <span>292 字节 · {{ row.value.source === 'fetcher' ? '获取器' : '正常请求' }} · {{ remaining(row.value.expiresAt) }}</span><BaseIconButton :label="`复制 ${row.model} 的 Turn State`" @click="copyText(row.value.value, { successText: 'Turn State 已复制' })">
                  <Copy class="size-4" />
                </BaseIconButton>
              </div>
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
              <span v-else-if="isNova(account.id)">本次出口 IP：未验证（NovaProxy Rotating）</span>
              <span>返回长度：{{ row.attempt.byteLength ?? '未返回' }} · 耗时：{{ row.attempt.durationMs }} ms · 输入 / 输出 token：{{ row.attempt.inputTokens ?? '未知' }} / {{ row.attempt.outputTokens ?? '未知' }}</span>
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
          <BaseSelect v-model="instanceForm.provider" :options="[{ label: 'Azure', value: 'azure' }, { label: 'NovaProxy Rotating', value: 'novaproxy' }]" :disabled="saving || !!originalInstanceId" />
        </BaseFormItem>
        <BaseFormItem label="实例 ID">
          <BaseInput v-model="instanceId" :disabled="saving || !!originalInstanceId" placeholder="azure-main" />
        </BaseFormItem>
        <template v-if="instanceForm.provider === 'novaproxy'">
          <BaseFormItem label="名称">
            <BaseInput v-model="instanceForm.name" :disabled="saving" />
          </BaseFormItem>
          <BaseFormItem label="服务器凭据名称">
            <BaseInput v-model="instanceForm.credentialRef" placeholder="nova-us" :disabled="saving" />
          </BaseFormItem>
          <p class="text-cp-sm text-cp-warning">
            Residential Premium · IPv4 · 每次独立连接；不保证出口不重复，实际 IP 未验证。
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
            Azure 等待新 IP 就绪；NovaProxy 每次新建代理连接。仅请求官方 OpenAI，不使用账号自定义网关或业务代理。
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
          使用无历史的简短请求，不携带 Turn State。未获得有效新值时等待 10 秒重试；连接错误保留退避，账号或模型错误需处理。
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
