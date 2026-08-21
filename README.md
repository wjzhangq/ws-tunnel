# ws-tunnel

WS 反向隧道的两个独立二进制实现:`tunnel-server` + `tunnel-client`。

- 传输:纯明文 `ws://`,server 不做任何 TLS 处理
- 多路复用:WS 上跑 [smux](https://github.com/xtaci/smux),每个 node 固定 `channels` 条数据通道 + 1 条控制通道
- 转发对象:原始 TCP 字节流(L4 透传),HTTP / gRPC / MySQL 等应用层协议对隧道透明
- 寻址:**server 反向监听端口号本身就是端口 id**
- Client 只需要 `url + key`,端口清单等由 server 握手时下发,运行期可热更
- Server 一份 YAML,支持增量热重载(fsnotify + `SIGHUP`)

对应设计文档:`ws-tunnel-server-client-design.md` §1–§16。实现与文档的逐条对照见 **HANDOFF.md**。

---

## 快速开始

```bash
go mod tidy          # 首次拉依赖
make build           # 产出 bin/tunnel-server 与 bin/tunnel-client

cp config.example.yaml config.yaml
./bin/tunnel-server -config config.yaml

# 边缘机(NAT 后)
./bin/tunnel-client --url ws://tunnel.example.com:8443/ws --key xx1
```

client 连上后,server 才会为它名下的 `ports` 起反向监听:

```bash
curl http://127.0.0.1:19080/     # 经隧道打到 client 本地的 127.0.0.1:8080
curl http://127.0.0.1:8090/status
curl http://127.0.0.1:8090/metrics
```

## 命令行

**tunnel-server**

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-config` | `config.yaml` | 配置文件路径 |
| `-log-level` | `info` | `debug` / `info` / `warn` / `error` |
| `-version` | | 打印版本后退出 |

信号:`SIGHUP` 触发重载;`SIGINT` / `SIGTERM` 触发优雅停机(30s 上限)。

**tunnel-client**

| 参数 | 环境变量 | 说明 |
| --- | --- | --- |
| `--url` | `TUNNEL_URL` | 必填,例如 `ws://tunnel.example.com:8443/ws` |
| `--key` | `TUNNEL_KEY` | 必填,node 凭据 |
| `--node` | `TUNNEL_NODE` | 可选,留空时 server 用 key 反查 node(见 HANDOFF 的歧义处理) |
| `-log-level` | | `debug` / `info` / `warn` / `error` |

## 配置

见 `config.example.yaml`,字段语义与设计文档 §3 一致。要点:

- `nodes`(身份 + 容量)与 `ports`(映射)是两个独立顶层 section
- `ports` 的 key 是纯端口号,固定绑 `127.0.0.1`,不提供绑定地址配置项
- 非法条目**不阻止启动**:丢弃该条并打 `WARN`(YAML 语法错误、`listen` 缺失除外)
- 重复 `key`:保留文档中先出现的那个 node,后者整条丢弃

改完存盘即可生效,或 `kill -HUP $(pidof tunnel-server)`。

## 可观测性

- `GET /status`、`GET /status?node=node1` — 在线/离线 node、通道数、流计数、容量峰值、流量与每个反向端口的监听状态
- `GET /metrics` — Prometheus 文本格式,指标见 HANDOFF.md §可观测性

## 部署提醒

反向监听端口固定绑 `127.0.0.1`。如果要让隧道端口直接对外服务,**server 上还需要一个前置进程**(L4 代理 / SSH 转发 / Caddy 之类)把公网流量接到 `127.0.0.1:19080`。

`key` 明文过网,`listen` 只应暴露在可信网络中(内网 / VPN / 专线)。

## 开发

```bash
make test    # 单元 + 端到端集成测试
make race    # 带竞态检测
make lint    # gofmt + go vet
```
