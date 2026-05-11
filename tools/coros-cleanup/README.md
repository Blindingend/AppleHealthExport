# COROS Training Hub Cleanup

一个用于安全清理 COROS Training Hub 运动记录的小工具。它会打开真实浏览器让用户登录，然后直接调用 COROS Web API 做查询和删除；删除前一定先 dry-run、保存备份，并要求输入精确确认字符串。

适合场景：

- 批量删除某个日期之前的历史运动数据。
- 清理误导入产生的重复运动记录。
- 避免在网页 UI 上手工一条条点删除。

## 前置要求

- Node.js 22 或更新版本。
- Microsoft Edge、Google Chrome 或 Brave。
- 一个 COROS Training Hub 账号。

不需要 `npm install`。

## 快速开始

运行交互式向导：

```powershell
node .\coros-cleanup.js
```

向导会依次完成：

1. 选择浏览器：Edge、Chrome 或 Brave。
2. 用独立本地 profile 打开 COROS Training Hub。
3. 引导用户在浏览器里登录账号。
4. 读取运动历史概况：总数、日期范围、年份分布、重复组分布。
5. 询问删除模式和截止日期。
6. 执行 dry-run，并保存完整 JSON 备份。
7. 展示将删除的数量、日期范围和样例。
8. 要求输入精确确认字符串。
9. 删除时逐条写日志，支持中断后审计。

## 删除模式

`before-date`

选择截止日期之前的所有运动记录。例如 cutoff 是 `2025-09-04`，则候选范围是 `2025-09-04` 之前，也就是到 `2025-09-03` 为止。

`duplicates-before-date`

只选择 cutoff 之前、并且 `startTime` 出现多条记录的重复组。适合“同一时间误导入出两条或多条”的情况。

## 非交互式用法

查看历史概况，不删除：

```powershell
node .\coros-cleanup.js history --browser edge
```

dry-run：

```powershell
node .\coros-cleanup.js dry-run --browser edge --cutoff 2025-09-04 --mode before-date
```

只 dry-run 重复组：

```powershell
node .\coros-cleanup.js dry-run --browser chrome --cutoff 2025-09-04 --mode duplicates-before-date
```

从 dry-run 备份删除：

```powershell
node .\coros-cleanup.js delete --browser edge --from-backup .\coros-cleanup-data\dry-run-before-date-before-2025-09-04-....json --confirm "DELETE N COROS ACTIVITIES BEFORE 2025-09-04"
```

先测试删除前 5 条：

```powershell
node .\coros-cleanup.js delete --browser edge --from-backup .\backup.json --confirm "DELETE N COROS ACTIVITIES BEFORE 2025-09-04" --limit 5
```

## 浏览器说明

工具会用本地 DevTools 端口启动所选浏览器，并使用独立 profile：

```text
coros-cleanup-data/browser-profile-<browser>
```

这避免绑定用户平时使用的 Edge/Chrome profile。第一次使用通常需要登录；后续运行可以复用这个本地 profile。

支持：

- `edge`
- `chrome`
- `brave`

## 安全设计

- `history` 和 `dry-run` 不发送删除请求。
- 真删必须基于 dry-run 生成的备份文件，不会临时重新决定删除集合。
- 真删必须输入 dry-run 输出的精确确认字符串。
- 默认每条删除间隔 `650ms`，降低接口限流、风控、会话失效风险。
- 可用 `--limit` 先删除 1 条或几条测试。
- 删除日志同时写 JSON 和 NDJSON，便于中断后排查。

## 隐私和文件

不要分享这些内容：

- `coros-cleanup-data/`
- dry-run 备份 JSON
- 删除日志 JSON/NDJSON
- 浏览器 profile 目录

这些文件可能包含活动元数据、`labelId`、账号区域信息或登录态相关数据。仓库里的 `.gitignore` 已默认排除它们。

主要文件：

- `coros-cleanup.js`：推荐分享使用的 CLI。
- `README.md`：使用说明。
- `SECURITY.md`：隐私和安全说明。
- `coros_activity_cleanup_cdp.js`：早期调试脚本。
- `coros_activity_cleanup_api.js`：早期直接 API 删除脚本。
- `coros_activity_cleanup_console.js`：浏览器控制台备用脚本。

## 中断后怎么办

如果删除运行中断，不要直接复用旧备份继续删。正确流程：

1. 对同一范围重新 dry-run。
2. 检查剩余数量和样例。
3. 从新的 dry-run 备份执行删除。

这样可以避免对已经变化的集合重复操作。
