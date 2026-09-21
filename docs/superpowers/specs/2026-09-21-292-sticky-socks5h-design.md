# 292 探测器 SOCKS5H sticky 出口设计

**日期：** 2026-09-21  
**状态：** 已获用户确认，待编写实现计划

## 背景

292 获取器目前可以通过动态出口服务建立本地 HTTP CONNECT 隧道，再由出口服务连接上游 SOCKS5 代理。上游连接已经把 OpenAI 目标域名作为 SOCKS5 域名地址类型发送，因此具备 SOCKS5H 的远程 DNS 语义，但代理用户名目前不会按探测生成 sticky session，成功结果也不会关联本次探测使用的上游代理地址。

本次变更需要让每次 292 探测使用独立的 sticky session，同时把成功获取 292 所用的完整代理地址与 292 一起保存并在管理端展示、复制。

## 目标

- 292 获取器的动态 SOCKS5 出口按 SOCKS5H 语义使用远程 DNS。
- 支持在代理用户名中配置固定占位符 `__CPR_292_SID__`。
- 每次新探测租约生成一个新的 12 位、小写字母和数字组成的 SID，并替换用户名中的占位符。
- 有效 292 与实际使用的完整上游代理地址建立一对一关联。
- 管理端分别展示和复制 292 字符串及代理地址。
- 旧的 Turn State JSON、Azure 出口和未使用占位符的旧 SOCKS5 配置保持可读取。

## 非目标

- 本次不实现 SOCKS5 代理出口 IP 的独立验证。
- 本次不改变 Azure 动态出口的分配、保留或清理逻辑。
- 本次不让普通业务请求携带或展示探测代理地址。
- 本次不把代理地址写入探测历史、日志、错误消息或动态出口状态列表。
- 本次不引入新的数据库迁移；代理地址作为现有 Turn State JSON 的可选字段保存。

## 方案选择

### 推荐方案：扩展现有动态出口租约链路

沿用现有架构：Rust 获取器向 Python 出口服务申请租约，Rust 通过出口服务返回的本地 HTTP 代理完成探测，Python 再通过上游 SOCKS5 CONNECT 建立隧道。只在上游 SOCKS5 租约创建时生成 SID，并把本次租约实际使用的上游地址返回给 Rust。

优点：

- 不重复实现租约、并发、释放和故障恢复。
- 现有 SOCKS5H 的域名转发行为可以直接复用。
- Azure 路径和已有部署可以继续工作。
- 代理地址只沿控制面租约响应传递，不需要扩展动态出口状态接口。

不采用另起专用 SOCKS5H 服务的原因：会重复现有租约和隧道逻辑，增加部署与迁移边界，且不能改善当前 SOCKS5 CONNECT 的协议语义。

## 详细设计

### 1. SOCKS5H sticky session

- 在 `socks5.py` 增加固定占位符常量 `__CPR_292_SID__`。
- `Service.acquire()` 为每个新租约复制实例凭据，不修改共享实例配置。
- 如果用户名包含占位符，则使用安全随机源生成 12 个 `[a-z0-9]` 字符，并替换占位符。一个租约只生成一次；控制面因相同租约 ID 重试时复用同一组凭据。
- 若旧配置不含占位符，继续使用原用户名，保持旧配置可用；新配置页面将明确要求使用该占位符以启用 sticky session。
- `Socks5Proxy.connect()` 继续通过 SOCKS5 域名地址类型发送 `api.openai.com` 或 `chatgpt.com`，不在出口服务本地解析目标域名。
- 本次实际使用的地址以 `socks5h://` URL 表示。用户名和密码按 URL 规则转义；IPv6 主机使用方括号。

### 2. 动态出口租约响应

动态出口 `/v1/leases` 的 `ready` 响应新增可选字段：

```json
{
  "proxyUrl": "http://127.0.0.1:19081",
  "upstreamProxyUrl": "socks5h://<实际用户名>:<实际密码>@<主机>:<端口>"
}
```

- `proxyUrl` 仍是 Rust 连接本地出口服务的内部地址。
- `upstreamProxyUrl` 只在活跃租约的 `ready` 响应中返回给已认证的控制面调用者。
- `/v1/status`、租约历史、错误正文和日志不返回 `upstreamProxyUrl`。
- Python 服务只在内存租约对象中保存本次实际凭据和 URL；释放后不继续对外提供该 URL。
- Rust 将该字段作为可选字段解析，以便新网关短暂连接旧出口服务时仍能完成探测；旧服务不会产生代理关联值。

### 3. Rust 获取器与 Turn State

- `dynamic_egress::Lease` 保存 `upstream_proxy_url`。
- `FetchOutcome` 增加可选代理地址，但只有响应中捕获到有效 292 字符串时才填充。
- `TurnStateValue` 增加 `proxy_url: Option<String>`，序列化名称为 `proxyUrl`，并使用默认值兼容旧 JSON。
- 获取器成功捕获 292 时调用缓存观察接口，同时传入本次租约的完整上游代理地址。
- 缓存更新规则：
  - 新的有效 292：保存新的代理地址（仅获取器来源有值）。
  - 相同 292 再次由获取器成功获取：更新时间并把代理地址更新为最近一次成功探测使用的地址，但不延长原有有效期。
  - 普通业务流量收到相同 292：保留既有代理地址。
  - 普通业务流量收到新的 292：保存新值并清空代理地址，因为该值不是由动态探测代理获得。
  - 无效值、非 292 响应和错误响应：不改变当前有效值及其代理地址。
- 恢复旧值时重新计算 Turn State 有效期，缺失的 `proxyUrl` 按 `None` 处理。

### 4. 管理端展示

- `FetcherValue` 增加 `proxyUrl: string | null`。
- 账号和模型卡片在当前值区域分别展示：
  - 292 字符串及复制按钮。
  - 探测代理地址及复制按钮。
- 代理地址显示完整内容，允许复制，明确标注其包含代理认证信息。
- 若当前值来自普通请求或旧版本数据没有代理地址，显示“未绑定探测代理”，不伪造地址。
- 不在动态出口实例状态卡片或探测历史中展示代理认证信息。

### 5. 安全与兼容性

- 代理密码不写入日志、测试数据、截图或提交说明；测试使用占位凭据。
- 代理地址会进入现有 Turn State 持久化 JSON，并通过管理员获取器快照返回，这是满足用户复制需求的必要敏感数据扩展；仍受现有管理端权限和数据库文件权限保护。
- `TurnStateValue` 的新增字段不需要 PostgreSQL 表结构变化。
- 旧 Turn State JSON、旧动态出口服务响应和旧 SOCKS5 配置均可读取；升级出口服务后，新租约才会产生 `upstreamProxyUrl`。

## 影响文件

- `deploy/dynamic-egress/socks5.py`：SID 生成、用户名替换、SOCKS5H URL 构造。
- `deploy/dynamic-egress/egress.py`：为每个租约使用复制后的凭据并返回实际上游地址。
- `deploy/dynamic-egress/test_socks5.py`、`test_egress.py`：协议、随机 SID、租约响应和敏感信息边界测试。
- `backend/crates/providers/openai/src/provider/dynamic_egress.rs`：解析并保存上游代理地址。
- `backend/crates/providers/openai/src/provider/turn_state_fetcher.rs`：只在有效 292 成功路径关联代理地址。
- `backend/crates/providers/openai/src/provider/turn_state.rs`：缓存更新规则与普通流量来源处理。
- `backend/crates/gateway-core/src/provider_ports/turn_state.rs`：持久化合同新增可选字段。
- 相关 Rust 管理端测试和 PostgreSQL Turn State 测试：补齐新字段初始化与兼容性断言。
- `frontend/src/api/modules/turn-state-fetcher.ts`、`frontend/src/views/turn-state-fetcher/index.vue`：类型、展示和复制交互。
- `deploy/dynamic-egress/README.md`、`deploy/README.md`：更新 SOCKS5H sticky 配置和敏感地址说明。

## 错误处理

- SID 生成或上游代理 URL 构造失败：租约申请失败，不发送 OpenAI 请求。
- 上游 SOCKS5 认证或 CONNECT 失败：沿用现有“动态出口连接失败”重试路径，不回退直连或业务代理。
- 租约响应缺少本地代理字段：沿用现有租约解析失败路径。
- 新增上游代理字段缺失：允许旧服务兼容运行，但成功 292 不保存代理地址，并在前端显示未绑定代理。
- 代理地址只随有效 292 写入缓存，错误响应中的同长度头仍不进入自动缓存。

## 测试与验收

### 自动化测试

- Python：验证 SID 为 12 位 `[a-z0-9]`、不同租约生成不同 SID、占位符替换不修改原实例配置、SOCKS5 CONNECT 使用域名 ATYP、状态接口不泄露密码、租约响应返回实际上游 URL。
- Rust：验证旧 JSON 可恢复、同值更新代理地址但不延长有效期、普通流量新值清空代理地址、获取器只对有效 292 保存代理地址。
- 前端：执行格式检查和构建，确保新增类型与模板交互通过。

### 命令

```bash
python -m unittest discover -s deploy/dynamic-egress -v
cargo fmt --all -- --check
cargo test -p provider-openai --test admin
cargo test -p gateway-core
cargo test -p gateway-store --test postgres
pnpm --dir frontend format:check
pnpm --dir frontend build
git diff --check
```

### 手工验收

在管理端配置用户名模板后，触发一次 292 获取，确认账号模型卡片同时显示 292 与 `socks5h://...` 代理地址，两个复制按钮均能复制完整内容；再次触发成功探测，确认 SID/代理地址变化；查看动态出口状态和探测记录，确认其中不出现代理密码。
