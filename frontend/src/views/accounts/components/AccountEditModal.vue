<script setup lang="ts">
import type { AccountRow } from '../constants'
import type { ApiKeyAccountForm } from '../utils/upstreamApiKey'
import type { AccountGroup, AccountModelAccess } from '@/api'

import BaseButton from '@/components/base/BaseButton.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseTextarea from '@/components/base/BaseTextarea.vue'
import ProviderIconGroup from '@/components/ProviderIconGroup.vue'
import AccountApiKeyFields from './AccountApiKeyFields.vue'
import AccountIdentityCell from './AccountIdentityCell.vue'
import AccountPlanBadge from './AccountPlanBadge.vue'
import AccountSettingsFields from './AccountSettingsFields.vue'

defineProps<{
  account: AccountRow | null
  groups: AccountGroup[]
  groupsLoading: boolean
  saving: boolean
  configurationLoading: boolean
  configurationReady: boolean
}>()

const emit = defineEmits<{
  save: []
}>()

const open = defineModel<boolean>({ required: true })
const apiKey = defineModel<ApiKeyAccountForm>('apiKey', { required: true })
const openaiBaseUrl = defineModel<string>('openaiBaseUrl', { required: true })
const notes = defineModel<string>('notes', { required: true })
const enabled = defineModel<boolean>('enabled', { required: true })
const concurrencyLimit = defineModel<string>('concurrencyLimit', { required: true })
const modelAccess = defineModel<AccountModelAccess | undefined>('modelAccess', { required: true })
const weight = defineModel<string>('weight', { required: true })
const proxyMode = defineModel<string>('proxyMode', { required: true })
const proxyId = defineModel<string>('proxyId', { required: true })
const selectedGroupIds = defineModel<string[]>('selectedGroupIds', { required: true })
</script>

<template>
  <BaseModal
    v-model="open"
    title="编辑账号"
    size="md-wide"
    :dismissible="!saving"
  >
    <div v-if="account" class="grid gap-5">
      <div
        class="flex flex-wrap items-center justify-between gap-4 rounded-cp bg-cp-fill-quaternary px-4 py-3.5"
      >
        <AccountIdentityCell
          class="min-w-0 flex-1"
          :account="account"
          size="lg"
        />
        <div class="flex shrink-0 items-center gap-3">
          <AccountPlanBadge :authentication-kind="account.authenticationKind" :plan-type="account.planType" :plan-type-display="account.planTypeDisplay" size="sm" />
          <ProviderIconGroup
            :provider="account.provider"
            :authentication-kind="account.authenticationKind"
          />
        </div>
      </div>

      <section v-if="account.provider === 'openai'" class="grid gap-4">
        <h3 class="m-0 text-cp font-heavy text-cp-text">
          上游连接
        </h3>
        <p v-if="configurationLoading" role="status" class="m-0 text-cp-sm text-cp-text-secondary">
          正在读取上游设置…
        </p>
        <p v-else-if="!configurationReady" role="alert" class="m-0 text-cp-sm text-cp-error">
          上游设置读取失败，请关闭后重试
        </p>
        <AccountApiKeyFields v-else-if="account.authenticationKind === 'api_key'" v-model="apiKey" editing :disabled="saving" />
        <BaseFormItem v-else label="OpenAI 网关地址（openai_base_url）">
          <BaseInput v-model="openaiBaseUrl" type="url" placeholder="留空使用系统默认地址" :disabled="saving" autocomplete="off" />
          <p class="mb-0 mt-2 text-cp-xs text-cp-text-secondary">
            填写 HTTPS 网关前缀，请求会自动追加 /codex/responses 等路径。仅影响此账号的模型请求，留空恢复系统默认地址。
          </p>
        </BaseFormItem>
      </section>

      <AccountSettingsFields
        v-model:enabled="enabled"
        v-model:concurrency-limit="concurrencyLimit"
        v-model:weight="weight"
        v-model:model-access="modelAccess"
        v-model:selected-group-ids="selectedGroupIds"
        v-model:proxy-mode="proxyMode"
        v-model:proxy-id="proxyId"
        :groups="groups"
        :groups-loading="groupsLoading"
        :disabled="saving"
        :endpoint="account.outboundProxyEndpoint"
        :account-id="account.id"
      />

      <BaseFormItem label="备注">
        <BaseTextarea
          v-model="notes"
          :rows="3"
          :maxlength="500"
          placeholder="最多 500 字，留空可清除备注。"
          :disabled="saving"
        />
      </BaseFormItem>
    </div>

    <template #footer>
      <BaseButton variant="secondary" :disabled="saving" @click="open = false">
        取消
      </BaseButton>
      <BaseButton
        variant="primary"
        :loading="saving"
        :disabled="!account || groupsLoading || (account.provider === 'openai' && !configurationReady)"
        @click="emit('save')"
      >
        保存更改
      </BaseButton>
    </template>
  </BaseModal>
</template>
