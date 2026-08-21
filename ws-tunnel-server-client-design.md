# WS 反向隧道 — 独立 Server / Client 设计文档

> 语言:Go 1.25+ · 形态:**独立二进制**(`tunnel-server` + `tunnel-client`)
>
> 多路复用:WS 上跑 **smux** · 数据通道 **固定 `channels` 条** · client 仅需 `url + key` · server 配置一份 **YAML**,支持 **热重载**
>
> 传输:**纯明文 `ws://`**,server 不做任何 TLS 处理

## 0. 相比原方案的改动

本方案去掉 Caddy 这层,直接做成两个独立进程:

- **Server**:一份 YAML 配置起步,`listen` 一个 WS 接入端口给 client 连;每条端口映射对应一个**反向监听端口**,外部连这个端口 = 经隧道连到 client 本地的对应服务。**纯 L4 TCP 转发**,不关心里面跑的是 HTTP/gRPC/MySQL 协议随便什么。
- **Server 自带配置热重载**(文件监控 + `SIGHUP`),而且是**增量式**的:改配置不会打断没受影响的连接,这是脱离 Caddy 之后才拿得到的能力。
- **Client 不变**:仍然只需要 `url + key`,端口清单等由 server 握手时下发,连接期间可再热更。

隧道协议本身(控制通道 + 固定数据通道 + smux + 端口 id 寻址)完全沿用原设计,这部分是稳定的、和"是不是 Caddy 插件"无关。

---

## 1. 概述

反向隧道:NAT/内网后的 **client 主动外连** server,维持 **1 条控制通道 + 固定多条数据通道**(数据通道用 smux 多路复用)。外部连到 server 某个"反向端口",server 经数据通道把这条连接的字节流转给 client,client 转发到本地端口,响应原路返回。

**寻址模型**:一个 `node` = 一台边缘机器;server 侧每个**反向监听端口**唯一对应一条转发路径,**服务器监听端口号本身就是端口 id**(全局唯一,写进流头)。

---

## 2. 角色与拓扑

```
        外部 TCP 连接                              Server 进程 (tunnel-server)
 ┌──────────────┐   连 127.0.0.1:19080       ┌───────────────────────────────────────┐
 │  外部客户端    │ ──────────────────────────► │  Listener :19080 (→ node1)             │
 └──────────────┘                             │            │                          │
                                               │            ▼ 取一条数据通道+开流(id=19080)│
                                               │      ┌──────────────┐                 │
                                               │      │ Node Registry │                │
                                               │      │  node1 固定池  │                │
                                               │      └──────┬───────┘                 │
                                               │             │ ▲ 控制通道(1条):握手/配置/心跳│
                              数据通道(固定 channels 条)        │ │
                                  smux 复用 stream            ▼ │
                                                     ┌────────────────┐
                                                     │  tunnel-client  │ NAT 后,主动外连
                                                     │  node1, key=xx1 │
                                                     └───────┬────────┘
                                                             │ 端口 id → 本地地址
                                                             ▼
                                                     127.0.0.1:8080 (client 本地服务)
```

| 角色                | 位置         | 职责                                                                  |
| ----------------- | ---------- | --------------------------------------------------------------------- |
| **tunnel-client**  | 边缘 / NAT 后 | 只带 `url + key` 启动;维持 1 控制通道 + 固定 `channels` 数据通道;收到开流请求后 dial 本地端口并转发字节 |
| **tunnel-server**  | client 可达的机器 | 读 YAML 配置;起**一个**全局 WS 接入端口 + 为每条端口映射起反向 TCP 监听;持有 Node 注册表与固定连接池;支持热重载 |

约束:所有 WS 连接均由 **client 主动外连**(NAT 友好);数据通道 **smux 多路复用**;流方向 **server → client**;隧道内是裸字节(L4 透传),不解析应用层协议。

---

## 3. 服务端配置

### 3.1 配置文件 `config.yaml`

**node 管理与端口映射分开管理**:`nodes` 以 node 名为 key,只放身份与容量;`ports` 以**服务器监听端口号**为 key,指向某个 node 的某个本地地址。运行时状态(在线与否、连接/离线时间)**不进配置文件**,只出现在 `/status`(见 §13)。

```yaml
# WS 接入地址(client 连这里),明文 ws
listen: ":8443"
# /status 监听地址,留空则不开启
status_listen: "127.0.0.1:8090"

settings:
  heartbeat: 15s
  dial_timeout: 10s
  queue_timeout: 5s
  max_streams_per_conn: 256

# ── node 管理 ─────────────────────────────
nodes:
  node1:
    key: "xx1"
    channels: 4
  node2:
    key: "xx2"
    channels: 2

# ── 端口映射 ───────────────────────────────
# key = server 反向监听端口号(纯数字,固定绑 127.0.0.1),同时也是流头里的端口 id
ports:
  19080:
    node: node1
    remote: "127.0.0.1:8080"
  15432:
    node: node1
    remote: "127.0.0.1:5432"
  1980:
    node: node2
    remote: "127.0.0.1:80"
```

新增一个反向端口 = 在 `ports` 下加一条,存好文件即可(见 §12 热重载)。

### 3.2 字段说明

| 字段                        | 含义                                                | 默认              |
| ------------------------- | ------------------------------------------------- | --------------- |
| `listen`                  | WS 接入地址,所有 client 连这里(明文 ws)                      | 必填              |
| `status_listen`           | `/status` 监听地址,留空不开启                              | 空(不开启)          |
| `settings.heartbeat`      | 控制通道 `ping/pong` 间隔(数据通道保活由 smux keepalive 负责)     | `15s`           |
| `settings.dial_timeout`   | 开流超时:写完流头后等 client 回 ack 的时限(见 §7)                | `10s`           |
| `settings.queue_timeout`  | 通道全满时请求排队超时                                       | `5s`            |
| `settings.max_streams_per_conn` | 单数据通道最大并发流(全局,无 node 级覆盖)                    | `256`           |
| `nodes.<name>.key`        | node 鉴权凭据,全局唯一;明文比对                               | 必填              |
| `nodes.<name>.channels`   | **固定**数据通道数                                       | `4`             |
| `ports.<port>`            | server 反向监听端口号,**纯数字**,固定绑 `127.0.0.1`;同时作为流头端口 id | 必填              |
| `ports.<port>.node`       | 该端口归属的 node 名,须在 `nodes` 中存在                      | 必填              |
| `ports.<port>.remote`     | client 侧本地地址,`host:port`                          | 必填              |

> **绑定地址是固定的**:反向监听端口一律绑 `127.0.0.1`,不提供配置项。也就是说外部流量必须来自 server 本机(或本机上的前置代理);这是有意收窄的攻击面,不做 `0.0.0.0` 支持。

### 3.3 配置校验规则

解析后按下列规则校验,**校验失败不阻止启动**(除 YAML 本身语法错误外),问题条目丢弃并打 WARN:

| 情况                                    | 处理                                            |
| ------------------------------------- | --------------------------------------------- |
| 两个 node 配了相同 `key`                    | **保留文档中先出现的**,后出现的整条 node 丢弃,打 `WARN`         |
| `ports` 引用了不存在(或已被上一条丢弃)的 node        | 该端口条目丢弃,打 `WARN`                              |
| 端口号不在 `1..65535`,或与 `listen`/`status_listen` 端口冲突 | 该端口条目丢弃,打 `WARN`                   |
| `remote` 不是合法 `host:port`             | 该端口条目丢弃,打 `WARN`                              |
| `channels` < 1                        | 取默认值 `4`,打 `WARN`                             |
| YAML 语法错误 / 文件不可读                     | 启动时:直接退出;reload 时:保留旧配置(见 §12)               |

> **实现注意**:"保留先出现的"要求解析时保持文档顺序。Go 的 `map[string]T` 无序,必须先解到 `yaml.Node` 再按节点顺序展开,否则"后面那个"是不确定的。

`ports` 以端口号为 key,天然杜绝两条映射抢同一个监听端口;同一 node 下多个端口、同一 `remote` 被多个端口指向都是允许的。

### 3.4 Client 启动参数

仍然只要两个参数;端口允许清单、通道数、超时等全部由 server 握手时下发,运行中可热更。

```
tunnel-client --url ws://tunnel.example.com:8443/ws --key xx1
#   或 TUNNEL_URL / TUNNEL_KEY 环境变量
```

---

## 4. 握手与配置下发

**要求**:node 一旦连上,server 必须**主动**把它需要的配置推给它,client 不用、也不能自己去查。这件事发生两次:

1. **首次握手**:client 发 `hello`,server 校验 `key` 通过后,`welcome` 里内嵌当前该 node 的 `config`(端口允许清单、`channels`、超时参数等)。client 只在此读取初始配置。
2. **运行期变更**(server 配置文件被改并 reload,见 §12):server 通过控制通道下发 `reload_config`,内容结构与 `welcome.config` 相同(**全量**)。client 收到后原地生效,**不需要断线重连**。

```
Client (只持 url + key)                    Server(配置权威)
  │  ── WS /ws ──────────────────────────► │
  │  ── {hello, role:control, node, key} ─►│  明文比对 key
  │  ◄── {welcome, session, heartbeat,     │  ★ 连接建立即主动下发配置
  │       channels:N(固定),                │
  │       config:{ports:{19080:"127.0.0.1:8080", ...}, ...}}
  │                                         │
  │  并发建立固定 channels 条数据通道:       │
  │  ── WS /ws → {hello, role:data,        │
  │       node, key, session} ────────────►│  校验 session,注册进 node 池,启 smux
  │  ◄── {welcome, channel_id} ────────────│
  │                                         │
  │            ...运行中,server 配置变了...  │
  │  ◄── {reload_config, config:{...}} ────│  ★ 主动推送,client 无需重连
```

**node 的确定方式**:`hello` 里带 `node` 名,server 用 `node` 查 `key` 并比对。`node` 不存在或 `key` 不匹配 → 回 `error` 后关闭连接。

**重复上线**:若该 node 已有控制通道在线,**拒绝后来者**(`error{code:"node_busy"}` 后关连接),已在线的连接不受影响。client 收到后按 §11 的退避策略重试,直到旧连接被判死(见下方空窗期说明)。

> **已知空窗期**:client 崩溃重启后,在 server 通过 `heartbeat×3`(默认 45s)判定旧控制通道死亡之前,新连接会被持续拒绝。这是"拒绝后来者"策略的既定代价,已接受;client 退避重试会在空窗期结束后自动接上。

---

## 5. 连接模型

### 5.1 控制通道(Control Channel)

- 每个 node **恰好 1 条**,client 启动第一条建立的 WS。
- 承载 **JSON 控制消息**:握手、配置下发、保活、遥测(见 §6)。
- **控制通道断 ⇒ 该 node 立即整体下线**:关闭并释放它名下所有反向监听端口、断开它所有数据通道、关闭所有在途转发连接。无宽限窗口(见 §11)。

### 5.2 数据通道(Data Channel)

- 每个 node **固定 `channels` 条**,全部由 client 主动建立;运行期不增减,单条断了由 client 补齐。
- 每条 = 一条 WS 包成 `net.Conn` 后运行 smux;并发承载多个 stream。
- 每个 stream 在 server 侧表现为一个 `net.Conn`,与 §9 里 TCP 监听器 accept 出来的连接做 `io.Copy` 双向转发。
- 数据通道握手必须带控制阶段拿到的 `session`;`session` 失效(控制通道已断/已换)则拒绝。

---

## 6. 控制指令

| type            | 方向             | 关键字段                                                    | 用途                                  |
| --------------- | -------------- | ------------------------------------------------------- | ----------------------------------- |
| `hello`         | client → server | `role`(control/data)、`node`、`key`、[`session`]           | 握手与鉴权                               |
| `welcome`       | server → client | `session`、`heartbeat`、`channels`、`config`、[`channel_id`] | 握手应答,内嵌下发会话与初始配置                    |
| `reload_config` | server → client | `config`(全量)                                            | server 配置热重载后主动推送(§12),无需重连         |
| `ping` / `pong` | 双向             | `nonce`、`ts`                                            | 控制通道保活与 RTT                         |
| `stats`         | client → server | `channels`、`streams`、`bytes_in/out`、`local_dial_errors` | client 侧遥测上报,喂给 `/status` 与指标;与 `heartbeat` 同频 |
| `drain`         | server → client | `deadline`                                              | 通知 client 停止接受新流、等存量流结束(优雅停机/节点下线)  |
| `bye`           | 双向             | `reason`                                                | 优雅关闭通告                              |
| `error`         | 双向             | `code`、`msg`、[`ref`]                                    | 通用错误/拒绝(鉴权失败、`node_busy`、配置非法等)     |

`config` 结构:

```json
{
  "ports": { "19080": "127.0.0.1:8080", "15432": "127.0.0.1:5432" },
  "channels": 4,
  "heartbeat": "15s",
  "max_streams_per_conn": 256
}
```

---

## 7. 多路复用(smux over WS)

数据通道把 WS 包成 `net.Conn`,在其上跑 [`xtaci/smux`](https://github.com/xtaci/smux),获得多路复用 + 每流流控 + keepalive,产出 `OpenStream()/AcceptStream() → net.Conn`。不手写帧协议。WS 库用 `coder/websocket`(二进制流模式,纯当字节管道)。

> **实现注意**:`coder/websocket` 默认 read limit 是 32 KiB,而 smux 单帧最大 64 KiB。**必须显式 `SetReadLimit`**(建议 1 MiB),否则大帧会直接把 WS 打断。

### 7.1 流头与开流确认

开流时 server 先写一个极小流头告知目标端口 id;client dial 完本地服务后回 **1 字节 ack**。

```
Server 在 smux.OpenStream() 后:
 ┌──────────┬────────────────────────┬───────────────────┐
 │ ver (1B) │ 端口 id(varint,即 server 监听端口号) │ 原始字节流(L4)… │
 └──────────┴────────────────────────┴───────────────────┘

Client 在 smux.AcceptStream() 后回:
 ┌────────────┬───────────────────┐
 │ status(1B) │  原始字节流(L4)…   │
 └────────────┴───────────────────┘

 status: 0x00 OK  ·  0x01 端口 id 不在清单  ·  0x02 本地 dial 失败  ·  0x03 其它拒绝
 ver:    固定 0x01,不匹配则 client 直接关流
 端口 id: varint,取值须在 1..65535,越界则关流
```

**为什么加这 1 个字节**:原设计把 `dial_timeout` 定义为"写流头 → 首字节回流"超时,这对 HTTP/gRPC/Redis 这类 **client 先说话**的协议是错的——本地服务在收到完整请求前不会回任何字节,慢请求会被误杀。改成显式 ack 后,超时判定与业务数据彻底解耦,并且顺带把失败原因带回了 server 侧(否则 server 只能看到一个和"正常 EOF"无法区分的关流)。

**不增加延迟**:server 写完流头**立即**开始把外部连接的字节 pipeline 进 stream,不等 ack;ack 在另一个方向上单独读取。

- 读到 `0x00` → 开始把 stream 的后续字节回写给外部连接,正常转发。
- 读到非 0 → 按错误码计数,关闭 stream 与外部连接。
- `dial_timeout` 内没读到 ack → 同上,计 `stream_open_total{result="timeout"}`。

**队头阻塞(HOL)**:smux 跑在单条 WS 之上,TCP 层 HOL 无法消除。缓解手段:用固定多条 WS(`channels`)把流摊开,配比由 §8 的峰值统计指导。

---

## 8. 固定连接池与峰值统计

- `channels` = 每个 node 数据通道 **固定数量**;client 启动建满、单条断了补齐,运行期不增减。
- **理论并发上限** = `channels × max_streams_per_conn`。
- 新流落点:挑"当前并发流最少"的通道(least-loaded)→ 未满即用;全部通道打满 → 进队列,等 ≤ `queue_timeout`;超时则关闭外部连接,累加 `saturated_total`。
- **`peak_demand`**:并发需求高水位(含排队/被拒部分),统计口径为**自进程启动以来**,不滑动、不重置(进程重启即清零)。`channels_needed_at_peak = ceil(peak_demand / max_streams_per_conn)`;`> channels` ⇒ 建议调大 `channels`,长期 `≤` ⇒ 可下调。三值进 §13 指标与 `/status`。

---

## 9. 反向监听 → 隧道转发全链路时序

```
1. Server 启动 → 读 config.yaml → 校验 → 起 WS 接入监听(:8443)
   注意:此时**不起**任何反向 TCP 监听
2. Client 启动 → 控制通道 hello/auth → welcome+config → 建固定 channels 条数据通道
3. Server 在该 node 控制通道就绪后,才为它名下的 ports 起 TCP Listener(127.0.0.1:19080 ...)
4. 外部 TCP 连接 connect 127.0.0.1:19080
5. Listener(id=19080, node1).Accept() → 拿到外部连接 conn
6. PickChannel(并发最少) → smux.OpenStream() → 写流头(ver=1, id=19080)
   → 立即开始 io.Copy(conn → stream)
7. Client AcceptStream() → 读流头 → 查允许清单得 127.0.0.1:8080 → dial 本地
   → 回 ack(0x00) → io.Copy 双向
8. Server 读到 ack=0x00 → 开始 io.Copy(stream → conn),双向打通
9. 任一端关闭 → 另一端读到 EOF → 关闭 stream/conn
10. smux 回收 streamID;通道并发数 -1
```

反向监听端口的生命周期**绑定 node 控制通道**:控制通道在 → 端口在;控制通道断 → 端口立刻关闭并释放(§11)。这样 `/status` 里"端口是否可连"和"node 是否在线"永远一致,不会出现连上了却打不通的假在线。

不再有"HTTP 请求经 reverse_proxy 落地"这一层——转发对象就是原始 TCP 连接,HTTP/MySQL/gRPC 等应用层协议对隧道透明。

---

## 10. 鉴权与安全边界

- **传输明文**:server 只跑 `ws://`,不做 TLS 终止、不读证书、不集成 ACME。
- **鉴权**:`hello` 携带 `node + key`,server 明文比对。无 HMAC 挑战、无 nonce 防重放。
- **由此产生的边界(部署时必须满足)**:`key` 以明文出现在网络上,任何能嗅探 `listen` 端口链路的人都能拿到并冒充该 node。因此 **`listen` 只应暴露在可信网络中**(内网 / VPN / 专线)。这是本设计有意接受的取舍,不在协议层补救。
- **数据通道绑定**:数据通道握手须带 control 阶段的 `session`,防孤立数据连接注入。
- **端口白名单**:client 只 dial server 下发清单内的端口 id,清单外回 `0x01` 拒绝。
- **反向端口绑 `127.0.0.1`**:外部流量只能来自 server 本机,收窄暴露面。
- **隔离**:不同 node 的流按命名空间严格隔离,杜绝跨 node 串流。

---

## 11. 健康、心跳、重连、故障

| 场景            | 检测                               | 处理                                                                                 |
| ------------- | -------------------------------- | ---------------------------------------------------------------------------------- |
| **控制通道断开**    | WS 关闭 / `ping` 超时(`heartbeat×3`) | **node 立即整体下线**:①关闭并**释放**它名下全部反向监听端口 ②断开它全部数据通道 ③关闭全部在途转发连接 ④session 作废。**无宽限窗口** |
| 数据通道死连接(控制仍在) | smux keepalive 失败 / WS 关闭        | 该通道移出在线集;其上在途流的外部连接被关闭;client 重连补回这一条                                               |
| client 整体掉线   | 控制 + 数据通道全断                      | 同"控制通道断开"                                                                           |
| 重复上线          | 已有同 node 控制通道在线                  | 拒绝后来者(`node_busy`),在线连接不动;后来者退避重试(§4 空窗期说明)                                         |
| 开流超时          | 写流头后 `dial_timeout` 内无 ack       | 关流,关闭对应外部连接;通道连续超时降权                                                                |
| client 本地不可达  | client dial 失败,回 ack `0x02`      | server 关流并关外部连接,累加 `local_dial_errors`                                              |
| 端口 id 不在清单    | client 查清单未命中,回 ack `0x01`       | 同上,单独计数(通常意味着 server/client 配置不同步)                                                  |
| 通道全满          | 固定通道全部达 `max_streams_per_conn`   | 外部新连接排队 ≤ `queue_timeout`,超时后关闭;累加 `saturated_total`、记录 `peak_demand`               |

**重连与恢复**:

- 控制通道断开后,client 按**指数退避(`1s → 30s`)** 重连;重连视为**全新会话**——拿到新 `session`,并重新建满 `channels` 条数据通道。旧 session 的一切不做续接。
- server 侧在新控制通道就绪后,重新为该 node 起反向监听端口。若端口此刻被本机其它进程占用,记录错误并按退避重试,`/status` 中该端口标记 `listening:false` 并附 `error`。
- 因为转发的是原始 TCP 字节流,不存在 HTTP 502/503 语义——所有"拒绝/故障"场景统一表现为**直接关闭那条外部连接**。
- `heartbeat` 仅用于控制通道 `ping/pong` 与判活;数据通道保活由 smux 自带 keepalive 负责。

---

## 12. 配置热重载(Reload)

**改配置不用重启进程,也不打断没受影响的连接。**

### 12.1 触发方式

- 收到 `SIGHUP` 信号,或
- 监听配置文件变化(fsnotify),文件保存后自动触发(带 ~200ms 去抖,避免编辑器多次写触发多轮)。

### 12.2 增量应用(diff)

重新解析 `config.yaml` 并跑完 §3.3 校验后,与当前运行配置做 diff:

| 变更类型                    | 处理方式                                                        |
| ----------------------- | ----------------------------------------------------------- |
| 新增 node                 | 无需动作,等它后续连接时鉴权、走 §4 握手下发流程(端口也随之起)                          |
| 删除 node                 | 对已连接的该 node 发 `drain`,等存量流结束(或到 deadline)后断开;其端口随之关闭        |
| node 的 `key` 变化         | 视为身份变更,强制断开旧连接(等价于控制通道断,端口即刻释放),等待用新 `key` 重连               |
| node 的 `channels` 变化    | 下发 `reload_config`,client 原地补建或多退少补,**不断连**                 |
| 新增 `ports` 条目           | 若该 node 在线 → 立即起 Listener 并下发 `reload_config`;不在线 → 只记配置    |
| 删除 `ports` 条目           | 停止 `Accept()` 并关闭 Listener;已建立的转发连接跑完(可配上限,默认等到自然结束);下发 `reload_config` |
| `ports` 条目的 `remote` 变化 | 只下发 `reload_config`,Listener 不动;**存量连接不受影响**,新连接落到新 `remote` |
| `ports` 条目的 `node` 变化   | 等价于"删旧 + 加新":关旧 Listener → 给两个 node 各下发 `reload_config` → 在新 node 在线时起 Listener |
| `settings.*` 变化         | 全局生效;`heartbeat`/`max_streams_per_conn` 随 `reload_config` 下发给所有在线 node |
| 解析或校验整体失败               | **保留旧配置**,打 `ERROR`,不影响任何在途连接                               |

> 端口号本身是 map key,不存在"监听地址变化"这种情况——改端口号 = 删一条 + 加一条。

### 12.3 伪代码

```go
func (s *Server) watchConfig() {
    sig := make(chan os.Signal, 1)
    signal.Notify(sig, syscall.SIGHUP)
    watcher, _ := fsnotify.NewWatcher()
    watcher.Add(s.configPath)

    debounce := time.NewTimer(0)
    if !debounce.Stop() { <-debounce.C }

    for {
        select {
        case <-sig:
            debounce.Reset(0)
        case <-watcher.Events:
            debounce.Reset(200 * time.Millisecond)
        case <-debounce.C:
            newCfg, err := LoadConfig(s.configPath) // 内含 §3.3 校验,WARN 由此产生
            if err != nil {
                s.logger.Error("reload failed, keep old config", zap.Error(err))
                continue
            }
            s.applyReload(newCfg)
        }
    }
}

func (s *Server) applyReload(newCfg *Config) {
    diff := DiffConfig(s.current, newCfg)

    for _, n := range diff.RemovedNodes    { s.registry.Drain(n, gracePeriod) }
    for _, n := range diff.KeyChangedNodes { s.registry.ForceDisconnect(n) } // 端口随之释放
    for _, p := range diff.RemovedPorts    { s.listeners.StopGraceful(p) }
    for _, p := range diff.AddedPorts      { s.listeners.StartIfNodeOnline(p) }
    for _, n := range diff.ChangedNodes    { s.registry.PushReloadConfig(n) } // 不断连

    s.current = newCfg
    s.logger.Info("reload applied", zap.Any("diff", diff))
}
```

一次改动只影响它涉及到的 node / 端口,其余连接和转发完全无感知。

---

## 13. 可观测性

### 13.1 `/status`

需配置 `status_listen`;`GET /status?node=node1` 可过滤。**运行时状态只在这里出现,不回写配置文件。**

```json
{
  "nodes": [{
    "node": "node1",
    "up": true,
    "connected_at": "2026-08-21T09:12:04Z",
    "disconnected_at": null,
    "control": { "connected": true, "last_seen": "2026-08-21T10:30:01Z", "rtt_ms": 3 },
    "channels": { "configured": 4, "online": 4 },
    "streams":  { "active": 47, "opened_total": 12880, "queue_depth": 0 },
    "capacity": {
      "max_streams_per_conn": 256,
      "max_concurrent": 1024,
      "peak_demand": 612,
      "channels_needed_at_peak": 3,
      "saturated_total": 0
    },
    "traffic": {
      "bytes_in": 91234567, "bytes_out": 4523110987,
      "rate_in_bps": 120480, "rate_out_bps": 8841200
    },
    "ports": [
      { "port": 19080, "remote": "127.0.0.1:8080", "listening": true,  "active_conns": 41, "error": null },
      { "port": 15432, "remote": "127.0.0.1:5432", "listening": true,  "active_conns": 6,  "error": null }
    ],
    "last_error": null
  }],
  "offline_nodes": [{
    "node": "node2",
    "up": false,
    "connected_at": "2026-08-21T08:01:00Z",
    "disconnected_at": "2026-08-21T10:22:13Z",
    "ports": [
      { "port": 1980, "remote": "127.0.0.1:80", "listening": false, "active_conns": 0,
        "error": "node offline" }
    ],
    "last_error": "control channel closed: read tcp ... i/o timeout"
  }]
}
```

`connected_at` / `disconnected_at` 语义:当前会话的建立时间 / 上一次会话的结束时间;进程重启后清空(不做持久化)。

### 13.2 Prometheus 指标

`tunnel_node_up`、`tunnel_channels{state=configured|online}`、`tunnel_streams_active`、`tunnel_streams_peak`、`tunnel_channels_needed_peak`、`tunnel_stream_open_total{result=ok|timeout|not_allowed|dial_failed|rejected}`、`tunnel_bytes_total{dir}`、`tunnel_node_saturated_total`、`tunnel_local_dial_errors_total`、`tunnel_port_listening{port}`(均带 `node` 标签)。

---

## 14. 优雅停机

Server 收到 `SIGTERM` / `SIGINT`:

1. 停止所有反向 Listener 的 `Accept()`(端口立即释放),`/status` 与 WS 接入继续服务。
2. 向所有在线 node 下发 `drain{deadline}`,client 停止接受新流。
3. 等待存量流自然结束,最长 `shutdown_grace`(硬编码 30s)。
4. 超时或全部结束后,发 `bye` 并关闭所有 WS,进程退出。

Client 收到 `SIGTERM`:停止建立新流 → 等存量流结束(同 30s 上限)→ 发 `bye` → 退出。收到 server 的 `drain` 时行为一致,但不退出进程(等 `bye` 或断线重连)。

---

## 15. 关键设计选择

| 选择         | 取值                                                     |
| ---------- | ------------------------------------------------------ |
| 形态         | 独立二进制(`tunnel-server` + `tunnel-client`),不依赖 Caddy     |
| 传输         | **纯明文 `ws://`**,无 TLS,无证书                              |
| 隧道方向       | 反向隧道,流由 server → client 发起                             |
| 转发对象       | 原始 TCP 连接(L4 透传),应用层协议(HTTP/gRPC/DB 等)对隧道透明            |
| 多路复用       | smux over WS(可切 yamux)                                 |
| 连接池        | 固定 `channels` 条,无动态扩缩容;容量靠峰值统计离线评估                      |
| 寻址         | **server 监听端口号即端口 id**,server 权威 + 允许清单下发               |
| 开流确认       | 流头后 1 字节 ack,超时与业务数据解耦                                 |
| 反向监听绑定地址   | 固定 `127.0.0.1`,不可配                                     |
| 端口生命周期     | 绑定 node 控制通道:控制断 ⇒ 端口关闭并释放,无宽限窗口                        |
| client 配置  | 仅 `url + key`,其余 server 下发                             |
| server 配置  | 一份 YAML,`nodes`(身份/容量)与 `ports`(映射)**分开的两个顶层 section** |
| 重复 key     | 保留先出现的,后者丢弃 + WARN                                     |
| 重复上线       | 拒绝后来者,接受 `heartbeat×3` 的空窗期                            |
| reload     | 文件监控 + `SIGHUP`,增量应用,不影响未变更的连接                         |
| 鉴权         | 明文 `key` 比对,无 HMAC / 无防重放                              |

---

## 16. 非目标(明确不做)

- TLS 终止、证书管理、mTLS —— 一律不做,明文 `ws://`。
- PROXY protocol / 真实源 IP 透传 —— 不做,client 本地服务看到的源地址就是 `127.0.0.1`。
- 反向监听绑 `0.0.0.0` 或自定义地址 —— 不做。
- 动态扩缩容数据通道 —— 不做,`channels` 固定,靠 `peak_demand` 离线调参。
- 正向隧道(client 侧监听 → 转发到 server 侧)—— 不在本设计范围。
- node 级别的 `max_streams_per_conn` 覆盖 —— 不做,只有全局值。
- 状态持久化 —— `/status` 中的计数与时间戳全部随进程生命周期,重启清零。

---

## 附:本轮修订记录

**按确认的决策改动**

1. 传输统一为明文 `ws://`,删除全部 TLS/wss 痕迹(§0/§3/§10/§15/§16)。
2. 配置格式统一为 **YAML**(原文档在 "YAML / config.toml / ```yaml 里写 TOML / server.yaml" 之间四处打架);`nodes` 与 `ports` 拆成两个顶层 section(§3.1)。
3. 运行时状态(在线、连接/离线时间)移出配置文件,只在 `/status` 呈现(§3.1/§13.1)。
4. 端口 id 改为**直接使用 server 监听端口号**,`ports` 以纯端口号为 key,绑定地址固定 `127.0.0.1`(§3.1/§7.1)。
5. 重复 `key`:保留先出现的、丢弃后者 + WARN;启动与 reload 一致(§3.3)。附带补了"顺序如何确定"的实现约束和"孤儿端口条目"的处理。
6. 重复上线:拒绝后来者,client 退避重试,空窗期已在 §4 显式写明。
7. 控制通道断开 ⇒ **关闭并释放**该 node 全部反向监听端口,在途连接一并关闭,**取消原 §11 的宽限窗口**(§5.1/§9/§11)。

**顺带修掉的问题**

8. `dial_timeout` 语义错误(client-speaks-first 协议会被误杀)→ 改为 1 字节 ack 确认,pipeline 不加延迟,并带回失败原因(§7.1)。选 ack 而非"直接删掉超时判定",是因为后者会让 server 完全看不到 dial 失败,`/status.last_error` 和错误分类指标都没法填。
9. 补 `coder/websocket` read limit(32KiB)与 smux 帧(64KiB)冲突的实现警告(§7)。
10. session 与重连的关系明确:控制重连 = 全新 session + 全部数据通道重建(§11)。
11. `peak_demand` 统计口径、`stats` 上报频率补齐(§6/§8)。
12. 流头 `ver` 与端口 id 的非法值处理补齐(§7.1)。
13. 删除 §3.2 中"`max_streams_per_conn` 可 node 级覆盖"的说法(配置里从来没有这个字段)。
14. 修正 §2 "为每个 node 起 WS 接入" → 全局只有一个 WS 接入端口。
15. 新增 §14 优雅停机、§16 非目标。
16. reload 增加 fsnotify 去抖,避免编辑器多次写盘触发多轮 reload。

**需要你留意的一处影响(不是问题,是提醒)**

反向监听端口固定绑 `127.0.0.1` 之后,外部流量必须来自 server 本机——也就是说 server 上还得有个前置进程(L4 代理 / SSH 转发之类)把公网流量接到 `127.0.0.1:19080`。如果原意是让隧道端口直接对外提供服务,这里需要改成 `0.0.0.0`。
