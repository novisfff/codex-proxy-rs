import type { Ref } from 'vue'
import type { AccountModelAccess, ApiKeyConfiguration, getAccounts } from '@/api'
import type { CodexTurnStateConfig } from '@/api/modules/settings'

import { computed, ref, shallowRef, watch } from 'vue'
import { getAccountDetail, updateAccount, updateAccountApiKey, updateAccountOpenAiBaseUrl } from '@/api'
import { toast } from '@/components/base/BaseToast'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { useRequestState } from '@/composables/useRequestState'
import { accountModelAccessError } from '../utils/modelAccess'
import { concurrencyLimitInput, parseAccountSchedulingForm } from '../utils/schedulingForm'
import { apiKeyAccountError, emptyApiKeyAccountForm, upstreamBaseUrlError } from '../utils/upstreamApiKey'

type AccountRow = Awaited<ReturnType<typeof getAccounts>>['items'][number]

export function useAccountEditor(options: {
  accounts: Ref<AccountRow[]>
  reloadAccounts: () => Promise<unknown>
  reloadGroups: () => Promise<unknown>
}) {
  const showEditModal = shallowRef(false)
  const editingAccountId = shallowRef<string | null>(null)
  const notes = shallowRef('')
  const schedulingEnabled = shallowRef(true)
  const concurrencyLimit = shallowRef('')
  const weight = shallowRef('1')
  const modelAccess = ref<AccountModelAccess | undefined>()
  const proxyMode = shallowRef('preserve')
  const proxyId = shallowRef('')
  const selectedGroupIds = ref<string[]>([])
  const saveAction = useAsyncAction()
  const saving = saveAction.loading
  const apiKey = ref(emptyApiKeyAccountForm())
  const configurationRequest = useRequestState()
  const configurationLoading = configurationRequest.loading
  const configurationReady = shallowRef(false)
  const savedConfiguration = shallowRef<ApiKeyConfiguration>()
  const openaiBaseUrl = shallowRef('')
  const savedOpenaiBaseUrl = shallowRef('')
  const codexTurnState = ref<CodexTurnStateConfig>({ mode: 'default', value: '' })
  const savedTurnState = shallowRef('')

  async function loadConfiguration(accountId: string) {
    const requestId = configurationRequest.start()
    try {
      const detail = await getAccountDetail({ accountId }, { signal: configurationRequest.signal })
      if (!configurationRequest.isCurrent(requestId))
        return
      if (!detail.credentialConfiguration)
        throw new Error('该账号没有上游设置')
      codexTurnState.value = { ...detail.credentialConfiguration.codex_turn_state }
      savedTurnState.value = JSON.stringify(codexTurnState.value)
      if ('base_url' in detail.credentialConfiguration) {
        apiKey.value = { ...emptyApiKeyAccountForm(), ...detail.credentialConfiguration }
        savedConfiguration.value = detail.credentialConfiguration
      }
      else {
        openaiBaseUrl.value = detail.credentialConfiguration.openai_base_url ?? ''
        savedOpenaiBaseUrl.value = openaiBaseUrl.value
      }
      configurationReady.value = true
    }
    catch (error) {
      configurationRequest.fail(requestId, error)
    }
    finally {
      configurationRequest.finish(requestId)
    }
  }

  const editingAccount = computed(() => {
    const accountId = editingAccountId.value
    return accountId
      ? options.accounts.value.find(account => account.id === accountId) ?? null
      : null
  })

  function open(account: AccountRow) {
    configurationRequest.invalidate()
    editingAccountId.value = account.id
    notes.value = account.notes ?? ''
    proxyMode.value = 'preserve'
    proxyId.value = ''
    schedulingEnabled.value = account.enabled
    concurrencyLimit.value = concurrencyLimitInput(account.concurrencyLimit)
    weight.value = String(account.weight)
    modelAccess.value = { ...account.modelAccess, models: [...account.modelAccess.models] }
    selectedGroupIds.value = account.groups.map(group => group.id)
    apiKey.value = emptyApiKeyAccountForm()
    openaiBaseUrl.value = ''
    savedOpenaiBaseUrl.value = ''
    codexTurnState.value = { mode: 'default', value: '' }
    savedTurnState.value = ''
    savedConfiguration.value = undefined
    configurationReady.value = false
    showEditModal.value = true
    if (account.provider === 'openai')
      void loadConfiguration(account.id)
  }

  async function save() {
    const accountId = editingAccountId.value
    if (!accountId || saving.value)
      return
    const isApiKey = editingAccount.value?.authenticationKind === 'api_key'
    const isOpenAi = editingAccount.value?.provider === 'openai'
    if (isOpenAi && !configurationReady.value)
      return
    const turnState = codexTurnState.value
    if (isOpenAi && (turnState.value.length > 8192 || /[^\x21-\x7E]/.test(turnState.value)
      || (turnState.mode === 'manual' && !turnState.value))) {
      toast.warning('Turn State 应为不含空格的 ASCII 字符串，最长 8192 字节；手动模式不能为空')
      return
    }
    if (isOpenAi && !isApiKey && openaiBaseUrl.value.trim()) {
      const error = upstreamBaseUrlError(openaiBaseUrl.value.trim())
      if (error) {
        toast.warning(error)
        return
      }
    }
    if (isApiKey) {
      if (!configurationReady.value)
        return
      const error = apiKeyAccountError(apiKey.value, true)
      if (error) {
        toast.warning(error)
        return
      }
    }
    const modelError = accountModelAccessError(modelAccess.value)
    if (modelError) {
      toast.warning(modelError)
      return
    }
    const scheduling = parseAccountSchedulingForm(concurrencyLimit.value, weight.value)
    if (proxyMode.value === 'proxy' && !proxyId.value.trim()) {
      toast.warning('请选择已通过测试的代理')
      return
    }
    if (!scheduling.valid) {
      toast.warning(scheduling.message)
      return
    }

    await saveAction.run(async () => {
      const settings = {
        accountId,
        notes: notes.value,
        outboundProxyId: proxyMode.value === 'preserve' ? undefined : proxyMode.value === 'direct' ? '' : proxyId.value.trim(),
        enabled: schedulingEnabled.value,
        concurrencyLimit: scheduling.values.concurrencyLimit,
        weight: scheduling.values.weight,
        modelAccess: modelAccess.value,
        groupIds: [...new Set(selectedGroupIds.value)],
      }
      const connectionChanged = isApiKey && (
        apiKey.value.apiKey !== ''
        || apiKey.value.base_url.trim() !== savedConfiguration.value?.base_url
        || apiKey.value.transport !== savedConfiguration.value?.transport
        || JSON.stringify(codexTurnState.value) !== savedTurnState.value
      )
      if (connectionChanged) {
        await updateAccountApiKey({ accountId, baseUrl: apiKey.value.base_url.trim(), transport: apiKey.value.transport, apiKey: apiKey.value.apiKey || undefined, codexTurnState: { ...codexTurnState.value }, settings })
      }
      else if (isOpenAi && !isApiKey && (openaiBaseUrl.value.trim() !== savedOpenaiBaseUrl.value || JSON.stringify(codexTurnState.value) !== savedTurnState.value)) {
        await updateAccountOpenAiBaseUrl({ accountId, openaiBaseUrl: openaiBaseUrl.value.trim(), codexTurnState: { ...codexTurnState.value }, settings })
      }
      else {
        await updateAccount(settings)
      }
      showEditModal.value = false
      await Promise.all([options.reloadAccounts(), options.reloadGroups()])
      toast.success('账号已更新')
    })
  }

  watch([showEditModal, saving], ([open, isSaving]) => {
    if (open || isSaving)
      return
    configurationRequest.invalidate()
    apiKey.value = emptyApiKeyAccountForm()
    savedConfiguration.value = undefined
    configurationReady.value = false
    editingAccountId.value = null
    notes.value = ''
    proxyMode.value = 'preserve'
    proxyId.value = ''
    schedulingEnabled.value = true
    concurrencyLimit.value = ''
    weight.value = '1'
    modelAccess.value = undefined
    selectedGroupIds.value = []
  })

  return {
    codexTurnState,
    openaiBaseUrl,
    apiKey,
    configurationLoading,
    configurationReady,
    showEditModal,
    editingAccount,
    notes,
    schedulingEnabled,
    concurrencyLimit,
    weight,
    modelAccess,
    proxyMode,
    proxyId,
    selectedGroupIds,
    saving,
    open,
    save,
  }
}
