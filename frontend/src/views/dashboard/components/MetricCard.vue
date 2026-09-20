<script setup lang="ts">
import type { EChartsOption } from 'echarts'
import type { MetricCardView, MetricTone } from '../composables/useDashboard'

import { computed } from 'vue'
import BaseCard from '@/components/base/BaseCard.vue'
import BaseMotionIcon from '@/components/base/BaseMotionIcon.vue'

import BaseChart from '@/components/charts/BaseChart.vue'
import { useThemeColor } from '@/composables/useThemeColor'
import { metricToneIconClasses, metricToneValueClasses } from '../constants'
import AnimatedMetricValue from './AnimatedMetricValue.vue'

const props = defineProps<{
  metric: MetricCardView
}>()

const trendToneClasses: Record<MetricTone, string> = {
  normal: 'bg-cp-text-tertiary',
  info: 'bg-cp-info-text',
  success: 'bg-cp-success-text',
  warning: 'bg-cp-warning-text',
  danger: 'bg-cp-error-text',
}

const themeColor = useThemeColor()
const sparklineColorTokens: Record<MetricTone, { token: string, fallback: string }> = {
  normal: { token: '--cp-color-blue-solid', fallback: '#5983F4' },
  info: { token: '--cp-color-blue-solid', fallback: '#5983F4' },
  success: { token: '--cp-color-green-solid', fallback: '#12B981' },
  warning: { token: '--cp-color-orange-solid', fallback: '#F59E0B' },
  danger: { token: '--cp-color-error', fallback: '#EF4444' },
}

const sparklineOption = computed<EChartsOption | null>(() => {
  const values = (props.metric.sparkline?.values ?? []) as number[]
  if (values.length < 2)
    return null

  const colorToken = sparklineColorTokens[props.metric.sparkline?.tone ?? 'normal']
  const color = themeColor(colorToken.token, colorToken.fallback)
  return {
    animation: false,
    grid: { left: 0, right: 0, top: 4, bottom: 4 },
    xAxis: { type: 'category', show: false, data: values.map((_, index) => index) },
    yAxis: { type: 'value', show: false, min: 'dataMin', max: 'dataMax' },
    series: [
      {
        type: 'line',
        data: values,
        smooth: true,
        symbol: 'none',
        lineStyle: { color, width: 1.75, opacity: 0.9 },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: `${color}18` },
              { offset: 1, color: `${color}00` },
            ],
          },
        },
      },
    ],
    tooltip: { show: false },
  }
})
</script>

<template>
  <BaseCard as="article" padding="compact" class="relative min-h-38.5 w-full">
    <div class="flex items-start gap-3">
      <BaseMotionIcon
        class="inline-flex size-8.5 shrink-0 items-center justify-center rounded-cp-lg"
        :class="metricToneIconClasses[metric.tone]"
      >
        <component :is="metric.icon" :size="18" />
      </BaseMotionIcon>
      <span class="mt-1 text-cp leading-[1.15] font-emphasis text-cp-text-secondary">{{ metric.title }}</span>
    </div>

    <div class="mt-3.25 flex h-7.75 items-end gap-2">
      <strong class="font-mono text-[28px] leading-[1.05] font-heavy tabular-nums text-cp-text">
        <AnimatedMetricValue :value="metric.value" :raw-value="metric.valueRaw" :formatter="metric.valueFormatter" />
      </strong>
      <i
        v-if="metric.trend && metric.trend.direction !== 'flat'"
        class="mb-1.25 block size-3"
        :class="[
          trendToneClasses[metric.trend.tone],
          metric.trend.direction === 'up'
            ? '[clip-path:polygon(50%_0,100%_58%,66%_58%,66%_100%,34%_100%,34%_58%,0_58%)]'
            : '[clip-path:polygon(34%_0,66%_0,66%_42%,100%_42%,50%_100%,0_42%,34%_42%)]',
        ]"
      />
    </div>

    <div v-if="sparklineOption" class="pointer-events-none absolute top-7 right-6 h-16 w-[42%]">
      <BaseChart :option="sparklineOption" :height="64" />
    </div>

    <div class="mt-3 flex min-h-7.5 w-full flex-wrap items-center justify-between gap-x-3 gap-y-2 rounded-cp-lg bg-cp-fill-alter/70 px-3 py-2">
      <span v-for="detail in metric.details" :key="detail.label" class="inline-flex min-w-0 items-baseline gap-2">
        <span class="shrink-0 text-cp-xs leading-none font-emphasis text-cp-text-quaternary">{{ detail.label }}</span>
        <b class="min-w-0 truncate font-mono text-xs leading-none font-bold tabular-nums" :class="detail.tone ? metricToneValueClasses[detail.tone] : undefined">{{ detail.value }}</b>
      </span>
    </div>
  </BaseCard>
</template>
