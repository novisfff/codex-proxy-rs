import type { ApiKeyConfiguration } from '@/api'

export interface ApiKeyAccountForm extends Omit<ApiKeyConfiguration, 'codex_turn_state'> {
  name: string
  apiKey: string
}

export function emptyApiKeyAccountForm(): ApiKeyAccountForm {
  return { name: '', base_url: '', apiKey: '', transport: 'http' }
}

export function upstreamBaseUrlError(value: string): string | undefined {
  if (value.length > 2048)
    return '上游 API 地址不能超过 2048 个字符'
  if ([...value].some(character => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127))
    return '上游 API 地址不能包含控制字符'
  try {
    const url = new URL(value)
    const loopback = url.hostname === 'localhost' || url.hostname === '[::1]' || /^127(?:\.\d{1,3}){3}$/.test(url.hostname)
    if ((url.protocol !== 'https:' && !(url.protocol === 'http:' && loopback)) || url.username || url.password || url.search || url.hash)
      return '请输入不含认证、查询参数或片段的 HTTPS 地址'
  }
  catch {
    return '请输入完整的上游 API 地址'
  }
  return undefined
}

export function apiKeyAccountError(form: ApiKeyAccountForm, editing = false): string | undefined {
  if (!editing && !form.name.trim())
    return '请输入账号名称'
  const urlError = upstreamBaseUrlError(form.base_url.trim())
  if (urlError)
    return urlError
  if (!editing && !form.apiKey)
    return '请输入 API Key'
  if (form.apiKey && (!/^[\x21-\x7E]+$/.test(form.apiKey) || form.apiKey.length > 16384))
    return 'API Key 不能包含空格或控制字符'
  return undefined
}
