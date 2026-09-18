<script setup lang="ts">
import type { AutomaticTurnState, CodexTurnStateConfig } from '@/api/modules/settings'
import { Copy, RefreshCw } from '@lucide/vue'
import { onBeforeUnmount, ref, watch } from 'vue'
import { getAutomaticTurnState } from '@/api/modules/settings'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseForm from '@/components/base/BaseForm/index.vue'
import BaseIconButton from '@/components/base/BaseIconButton.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'
import { useCopyText } from '@/composables/useCopyText'
import { formatDateTime } from '@/utils/date'

defineProps<{ disabled: boolean }>()
const model = defineModel<CodexTurnStateConfig>({ required: true })
const current = ref<AutomaticTurnState[]>([])
const loading = ref(false)
const error = ref(false)
const loaded = ref(false)
const copyText = useCopyText()
let timer: ReturnType<typeof setTimeout> | undefined
let controller: AbortController | undefined

async function refresh() {
  clearTimeout(timer)
  controller?.abort()
  const pending = new AbortController()
  controller = pending
  loading.value = true
  try {
    const result = await getAutomaticTurnState({ silent: true, signal: pending.signal })
    if (!pending.signal.aborted) {
      current.value = result
      error.value = false
      loaded.value = true
    }
  }
  catch {
    if (!pending.signal.aborted) {
      error.value = true
      current.value = []
    }
  }
  finally {
    if (!pending.signal.aborted) {
      loading.value = false
      timer = setTimeout(refresh, 5000)
    }
  }
}

watch(() => model.value.mode, (mode) => {
  clearTimeout(timer)
  controller?.abort()
  current.value = []
  loaded.value = false
  error.value = false
  if (mode === 'auto')
    void refresh()
}, { immediate: true })

onBeforeUnmount(() => {
  clearTimeout(timer)
  controller?.abort()
})

function setMode(mode: string) {
  if (mode === 'default' || mode === 'manual' || mode === 'auto')
    model.value = { ...model.value, mode }
}
</script>

<template>
  <BaseCard title="X-Codex-Turn-State">
    <BaseForm class="max-w-6xl">
      <BaseFormItem label="请求头模式">
        <BaseSegmented
          :model-value="model.mode"
          label="Turn State 模式"
          :disabled="disabled"
          :options="[
            { label: '默认', value: 'default' },
            { label: '手动', value: 'manual' },
            { label: '自动', value: 'auto' },
          ]"
          @update:model-value="setMode"
        />
      </BaseFormItem>
      <BaseFormItem v-if="model.mode === 'manual'" label="请求头值">
        <BaseInput
          :model-value="model.value"
          type="password"
          autocomplete="off"
          :maxlength="8192"
          :disabled="disabled"
          @update:model-value="model = { ...model, value: $event }"
        />
      </BaseFormItem>
      <BaseFormItem v-if="model.mode === 'auto'" label="自动模式使用值">
        <div class="min-w-0 space-y-2">
          <div class="flex flex-wrap items-center gap-2">
            <BaseIconButton label="刷新自动请求头" :disabled="loading" @click="refresh">
              <RefreshCw class="size-4" />
            </BaseIconButton>
          </div>
          <p v-if="error" role="status" class="text-cp-sm text-cp-text-secondary">
            读取失败，正在重试；也可点击刷新。
          </p>
          <div v-else-if="current.length" class="max-h-96 space-y-4 overflow-auto">
            <div v-for="entry in current" :key="JSON.stringify([entry.accountId, entry.model])" class="space-y-2 rounded-lg bg-cp-input-bg p-3">
              <div class="flex flex-wrap items-center gap-2 text-cp-sm text-cp-text-secondary">
                <span class="break-all">账号：{{ entry.accountId }}</span>
                <span class="break-all">模型：{{ entry.model }}</span>
                <BaseIconButton
                  :label="`复制 ${entry.accountId} / ${entry.model} 的 X-Codex-Turn-State`"
                  @click="copyText(entry.value, { successText: 'X-Codex-Turn-State 已复制' })"
                >
                  <Copy class="size-4" />
                </BaseIconButton>
              </div>
              <p class="text-cp-xs text-cp-text-secondary">
                292 字节 · 获取时间：{{ formatDateTime(entry.acquiredAt) }}
              </p>
              <pre class="max-h-40 overflow-auto whitespace-pre-wrap break-all font-mono text-cp-sm text-cp-text">{{ entry.value }}</pre>
            </div>
          </div>
          <p v-else class="text-cp-sm text-cp-text-secondary">
            {{ loaded ? '尚未获取到 292 字节值，自动模式暂不携带此请求头。' : '正在读取…' }}
          </p>
          <p class="text-cp-xs text-cp-text-tertiary">
            每个账号、每个上游模型分别使用最新的 292 字节值，不区分思考强度。未命中时不携带此头；显示每 5 秒刷新，模式修改需保存后生效。
          </p>
        </div>
      </BaseFormItem>
    </BaseForm>
  </BaseCard>
</template>
