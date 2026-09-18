<script setup lang="ts">
import type { CodexTurnStateConfig } from '@/api/modules/settings'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseForm from '@/components/base/BaseForm/index.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseSegmented from '@/components/base/BaseSegmented.vue'

defineProps<{ disabled: boolean }>()
const model = defineModel<CodexTurnStateConfig>({ required: true })

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
    </BaseForm>
  </BaseCard>
</template>
