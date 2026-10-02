# API 契约

基础路径：`/api/v1`。除注册、登录和刷新外，请求使用 `Authorization: Bearer <accessToken>`。

错误统一返回：

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "请求字段不合法",
    "details": [],
    "traceId": "req-..."
  }
}
```

## 认证

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/auth/register` | 注册并返回 Access Token，同时设置 Refresh Cookie |
| POST | `/auth/login` | 登录并轮换 Refresh Cookie |
| POST | `/auth/refresh` | 使用 Cookie 轮换刷新令牌 |
| POST | `/auth/logout` | 撤销当前 Refresh Session 并清除 Cookie |

Refresh Cookie 路径为 `/api/v1/auth`，生产环境在 HTTPS 下自动使用 `Secure`。

## 用户与设置

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/users/me` | 当前用户 |
| PATCH | `/users/me` | 更新展示名、默认乐器、时区和语言 |
| POST | `/users/me/password` | 修改密码并撤销其他会话 |

## 练习

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/sessions` | 光标分页、搜索、筛选和排序 |
| POST | `/sessions` | 创建练习 |
| GET | `/sessions/:id` | 详情，包含音频、标记、目标和复盘 |
| PATCH | `/sessions/:id` | 乐观锁更新；请求必须带 `version` |
| POST | `/sessions/:id/start-review` | 存在已就绪音频时进入 `IN_REVIEW` |
| GET | `/sessions/:id/completion-check` | 返回结构化缺失项 |
| POST | `/sessions/:id/complete` | 原子完成复盘 |
| POST | `/sessions/:id/archive` | 归档已完成练习 |
| POST | `/sessions/:id/restore` | 恢复归档练习 |
| DELETE | `/sessions/:id` | 必须提交完整 `confirmationTitle` |

创建练习：

```json
{
  "title": "协奏曲第二乐章 17-24 小节",
  "instrument": "小提琴",
  "startedAt": "2026-09-29T12:00:00.000Z",
  "focus": "换把后的音准",
  "location": "琴房 A",
  "notes": "节拍器 84 BPM",
  "actualDurationMs": 1800000
}
```

## 音频上传（分片断点续传 + 内容复用）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/sessions/:sessionId/media/uploads` | 初始化分片上传；同摘要已有就绪物理对象时直接复用 |
| GET | `/media/:mediaId/upload-state` | 查询服务端已收到的分片（ListParts），用于断点恢复 |
| GET | `/media/:mediaId/upload-parts?partNumbers=1,3` | 为指定分片批量签发预签名 PUT URL |
| POST | `/media/:mediaId/complete-upload` | 核对分片、合并对象、校验大小/SHA-256 并投递探测任务 |
| POST | `/media/:mediaId/abort-upload` | 放弃上传并中止 S3 分片会话 |
| GET | `/media/:mediaId` | 状态、元数据与波形峰值 |
| GET | `/media/:mediaId/playback-url` | 获取短期私有播放地址 |
| POST | `/media/:mediaId/retry-probe` | 重试音频探测 |
| DELETE | `/media/:mediaId` | 删除引用；最后一个引用删除时回收物理对象 |

创建上传会话：

```json
{
  "originalName": "practice.wav",
  "mimeType": "audio/wav",
  "sizeBytes": 2646000,
  "sha256": "64-hex-characters",
  "partSize": 8388608
}
```

`partSize` 可选（最小 5 MiB，默认 8 MiB）。响应中 `upload.parts` 是每片的
`{partNumber,start,end}` 字节区间，`partUrls` 为首批预签名 PUT 地址，
`upload.uploadedPartNumbers` 是对象存储端已确认存在的分片。

断点与复用语义：

1. 网络中断后重试，客户端先调 `upload-state`，仅对缺失分片重新签名并续传，
   已传分片不重复上传；确认时服务端再次以 ListParts 为准核对，
   缺片返回 `409 UPLOAD_PARTS_MISSING` 并在 `details.missingParts` 给出序号。
2. 同一用户相同 SHA-256 的物理对象只保留一份：初始化时若已有 `READY` 对象，
   响应 `reused=true` 且 `upload=null`，零网络传输直接建立引用。
3. 同摘要的并发确认由服务端事务 + advisory lock 串行化，只有第一个确认者
   合并并校验对象，其余确认只创建引用；探测（ffprobe/波形）只执行一次，
   结果同步到全部引用。
4. 删除引用时引用计数以 `media_objects` 为准：仍有其它练习引用则只解除关联，
   最后一个引用删除才回收 S3 内容。每次引用变更与物理对象回收均写审计日志
   （`MEDIA_REFERENCE_REMOVED` / `MEDIA_OBJECT_DELETED` /
   `MEDIA_OBJECT_GARBAGE_COLLECTED`），引用清理可追溯。

## 标记

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/sessions/:sessionId/annotations` | 标记列表 |
| POST | `/sessions/:sessionId/annotations` | 新增标记 |
| PATCH | `/annotations/:id` | 编辑标记 |
| DELETE | `/annotations/:id` | 删除标记 |

区间使用毫秒整数，最小时长 100 ms，且不能超过音频时长。问题类型为 `RHYTHM`、`FINGERING` 或 `EMOTION`。

## 复盘与目标

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/sessions/:sessionId/review` | 获取复盘 |
| PUT | `/sessions/:sessionId/review` | 保存复盘草稿 |
| POST | `/sessions/:sessionId/review/complete` | 完成复盘事务 |
| GET/POST | `/goals` | 目标列表/创建 |
| GET/PATCH | `/goals/:id` | 目标详情/更新 |
| POST | `/goals/:id/activate` | 重新激活取消或逾期目标 |
| POST | `/goals/:id/cancel` | 带原因取消 |
| POST | `/goals/:id/complete` | 用户确认完成 |
| GET/POST | `/goals/:id/progress` | 进度列表/新增 |

完成复盘请求会原子写入复盘、目标、进度并更新练习状态。任一步失败时全部回滚，返回 `REVIEW_INCOMPLETE` 且 `details` 为缺失项数组。

## 统计与导出

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/statistics/overview` | 总览指标 |
| GET | `/statistics/trends` | 按用户时区分日趋势 |
| GET | `/statistics/issues` | 问题类型、严重度和困难片段 |
| GET | `/statistics/goals` | 目标完成率和逾期 |
| GET | `/statistics/instruments` | 各乐器聚合 |
| GET | `/statistics/dashboard` | 首页聚合 |
| POST | `/exports` | 创建 JSON/CSV 用户数据导出 |
| GET | `/exports/:id` | 查询导出状态和短时下载地址 |

统计接口必须传 `from`、`to` 和 IANA `timezone`。

## 健康检查

| 路径 | 说明 |
|---|---|
| `/health/live` | 仅检查进程存活 |
| `/health/ready` | 检查 PostgreSQL、Redis 和对象存储 |
| `/metrics` | Prometheus 文本指标 |
