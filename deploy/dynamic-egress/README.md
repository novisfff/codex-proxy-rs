# 292 获取器动态出口

此服务运行在宿主机上，支持 Azure 专用公网 IP 和 NovaProxy Rotating 住宅代理。
控制面由网关管理 API 代理，CONNECT 端口只允许一次性租约访问 `chatgpt.com:443` 和
`api.openai.com:443`；TLS 由网关直接与 OpenAI 建立，出口服务不读取账号凭据或请求内容。
业务请求仍使用账号原有代理。

## 准备网络

1. 在目标 VM 启用系统分配 Managed Identity。为其配置专用公网 IP 资源组的读写删除权限、
   指定专用 NIC 的读取和 IP configuration 更新权限、必要的子网读取 / join 权限。
   CLI 安装和角色分配属于部署准备；进程本身不需要 root 或修改路由的权限。
2. IPv4 准备一个**非主** IP configuration，固定专用私网地址。保留主公网 IPv4、主 IP configuration、
   默认路由、SSH 和业务连接。IPv6 使用无人使用的专用配置，Azure 每个 NIC 的私网 IPv6 数量受限，
   必要时使用专用双栈 NIC；不要假设可以在当前 NIC 任意增加 IPv6 配置。
3. 先在 Azure 和来宾系统中配置好相应私网源地址及其路由、NSG。服务绑定的是私网地址，不能填写公网 IP。
   IPv6 专用源地址必须设置 `preferred_lft 0`（保留有效期），避免普通新连接自动优先选择该地址；
   服务会通过 `ip -j -6 address show` 进行只读检查。显式绑定仍可使用它。普通流量保留自己的正常出口，
   不应配置以专用源地址为默认 `src` 的路由，也不应由其他进程显式绑定专用地址。
   将该专用配置原有的公网 IP 关联解除；服务不会接管或删除没有自己所有权标签的现有 IP。
   `dedicated` 表示管理员确认该配置没有其他服务使用；程序另外禁止使用 IPv4 主配置。
4. 不使用 NAT Gateway、Azure Firewall 或其他会覆盖该源地址公网映射的出口。服务通过绑定相同源地址
   请求 `https://api64.ipify.org` 验证公网 IP，映射不符就失败关闭。探测请求不携带账号凭据。

参考微软文档：[NIC 地址配置](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/virtual-network-network-interface-addresses)、
[IPv6 限制](https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/ipv6-overview)、
[Managed Identity 登录](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-managed-identity)。

## 安装与配置

需要 Python 3.11+ 的 Linux 宿主机；Azure 实例额外需要 Azure CLI、iproute2 和完成网络准备。
将本目录放到 `/opt/cpr292-egress`，建立独立虚拟环境并安装 `requirements.txt`。
创建无登录权限的 `cpr292` 用户，将示例配置复制到 `/etc/cpr292-egress/config.json`，
生成至少 32 字符随机控制令牌，存到仅服务和网关可读的 `token` 文件。
使用同目录 systemd 单元运行。首次启动实例列表为空，可在管理端「292 获取器 → 动态出口」添加实例。

网关启动配置：

```yaml
openai:
  dynamic_egress:
    url: http://127.0.0.1:19080
    token_file: /run/secrets/cpr292-egress-token
```

网关运行在容器时，配置其可访问的宿主私网地址，并相应调整 `listen` 和 `proxyUrl`。
两端口只开放给网关，不能暴露到公网；跨不可信网络应先建立加密隧道。
不把控制令牌放进 URL、前端或账号代理配置。

Azure 实例可分别填写 IPv4 / IPv6 的 NIC、IP configuration 和私网源 IP。保存只修改本地配置，
首次获取才执行 Azure 预检。修改或移除实例前，暂停所有动态获取账号并等待活动租约清理完成。
实例配置保存在 SQLite 中，配置文件 `instances` 只作为首次启动的初值。

### NovaProxy Rotating

在「动态出口 → 添加出口实例」选择 NovaProxy Rotating，填写实例 ID、名称和服务器凭据名称
（例如 `nova-us`）。将凭据存入 `/etc/cpr292-egress/novaproxy/nova-us.json`：

```json
{"username":"YOUR_ROTATING_USERNAME","password":"YOUR_PROXY_PASSWORD"}
```

文件使用 `root:cpr292`、权限 `0640`，目录禁止其他用户写入；不支持符号链接。
可以用 `EGRESS_NOVAPROXY_CREDENTIALS_DIR` 指定凭据目录。凭据不会进入管理 API 或 SQLite；
修改文件后下一租约生效。更新服务时必须同时安装 `egress.py` 和 `novaproxy.py`。
账号获取器选择此动态出口实例和 IPv4。仅使用 Rotating 用户名，不添加 sticky/session 参数。

服务固定连接 `residential-gateway.novaproxy.io:1111`，每次尝试新建一个 CONNECT 隧道，
只访问官方 OpenAI；TLS 端到端校验证书，失败不会直连或改用业务代理。
当前 Residential Premium 仅支持 IPv4。供应商负责轮换，**不保证最近 24 小时不重复**；
独立 IP 探测与 OpenAI 连接可能走不同出口，因此不探测、不声明实际 OpenAI 出口 IP，页面显示未验证。
仅使用 NovaProxy 的服务无需 Azure 登录、身份或网络资源权限。

## 租约与故障恢复

- 全局只有一个活动租约，所有实例共享数据库和进程文件锁。不要启动使用其他数据库副本的第二个服务。
- 每次获取使用新 attempt ID；控制面重试使用同一 ID，不重复申请。
- Azure 依次创建 Standard 静态公网 IP、检查最近 24 小时历史、绑定专用配置、核实实际公网 IP。
  每次申请前先回收日志中自己的残留资源，再清理实例公网 IP 资源组内的闲置公网 IP（含非本服务创建的 IPv4/IPv6）。
  该资源组必须专供获取器使用；配置网卡的主 IPv4 公网 IP 始终保留，无法识别主地址时停止清理。
  其他地址若仍绑定资源或尚未完成操作则停止申请，不自动解绑；删除失败也不继续申请。
  被清理的地址计入最近 24 小时排除记录。
  重复分配最多尝试 5 次。Azure CLI 等待 ARM 完成，单次命令上限 10 分钟；网关等待上限 15 分钟，
  超时发出释放请求，服务完成正在进行的 ARM 操作后清理。
- 租约就绪后最长保留 90 秒，CONNECT 限一次，隧道最长 60 秒。网关上游请求最长 45 秒。
  失败、取消和到期均关闭隧道；Azure 还会解绑并删除自己的公网 IP，下一请求不会复用旧地址。
- IPv4/IPv6 是严格选择，DNS、绑定或探测失败不改用另一协议、旧 IP、业务代理或默认出口。
- Azure IP 历史按规范化地址持久化，连接和清理时延后最后使用时间。没有 24 小时外的可用 IP 时失败重试。
  重启不清空历史；最近任务保留 7 天，页面展示最近 50 条。备份和恢复必须同时保留 SQLite 与 WAL，
  停服务后备份最简单；丢失历史文件不能保证历史窗口内不重复。
- 先写资源日志再创建。启动时先对账和清理自己的资源；清理失败就禁止新租约。
  如 CLI 超时且 Azure 尚未显示创建结果，服务保留不确定记录并停用分配，不能把查询不到当作已取消。
  运维应按 SQLite `resources.name` 查 Azure Activity Log 确认最终结果，待资源可见后重启清理；
  只有确认创建最终失败且不存在资源，才可停止服务并移除对应日志记录。不要直接清空历史库。

第一版不自动创建 NIC、私网地址或修改 OS 路由。
公网 IPv4、带宽和相关资源可能产生 Azure 费用；云端配额和资源 SKU 决定可分配数量。

## 本地验证

```sh
python -m unittest discover -s deploy/dynamic-egress -v
```

测试使用替身 Azure 和本地真实 CONNECT 隧道，不申请云资源、不调用付费 OpenAI。
部署验收仍需对真实 Azure 双栈分配、出站映射、Managed Identity 权限和 OpenAI 响应分别验证。
