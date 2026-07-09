# actr RFC 指南

英文版：[README.md](README.md)。

这个目录用于记录 actr 中跨层、难回滚、会影响长期契约的设计决策。

这份文档面向维护者和 Agents，目标是说明如何创建、评审、接受、实现和替代
RFC。每个 RFC 文件中的 metadata 是状态的唯一来源。不要手写维护 RFC index；
目录列表和 Git 历史已经足够。

## 文件结构

```text
rfcs/
├── 0000-template.md        # 新建 RFC 时复制这个模板
├── README.md              # 英文流程指南
├── README.zh.md           # 中文流程指南
└── NNNN-short-name.md     # RFC 文档
```

非英文 RFC 可以在 `.md` 前加语言代码，例如
`0323-explicit-reply.zh.md`。

## 什么时候需要 RFC

实现前先写 RFC，如果改动满足以下任一条件：

- 改变 protocol、wire format、并发语义或回复语义；
- 需要跨 crate 或跨层协同，例如 `core/hyper`、`framework`、codegen、FFI
  或 WIT；
- 新增发布后难以撤回的 public API 或 option；
- 需要在有实质取舍的多个方案之间做持久决策。

以下情况通常不需要 RFC：

- bug fix；
- 纯文档改动；
- 不改变行为的局部重构；
- 没有 caller-visible 影响的内部改进；
- 对性能、平台覆盖、并行度、warning 覆盖等指标的增量改进。

## 创建 RFC

1. 新建 tracking issue，标题为 `RFC: <name>`。
2. 使用 issue 编号作为 RFC 编号。比如 issue `#323` 对应 `RFC-0323`。
3. 复制 `0000-template.md` 为 `NNNN-short-name.md`。
4. 填完整个模板，包括 `Status`、`RFC PR` 和 `Tracking issue`。
5. 设置 `Status: Proposed`。
6. 新建 PR，标题为 `docs: add RFC-NNNN <name>`。
7. RFC 处于 `Proposed` 时保持 PR 打开；不要合并 proposed RFC。

RFC 应该包含足够背景，方便未来读者理解。不要使用指向 `rfcs/` 之外仓库文件
的相对链接；那些文件可能移动或删除。需要引用代码时，可以直接写
`core/.../file.rs` 这样的路径文本，并链接相关 issue、PR 或外部参考。

## 状态

只使用以下持久状态：

| 状态 | 含义 |
|---|---|
| `Proposed` | RFC 正在 open PR 中评审，尚未合入。 |
| `Accepted` | 维护者已接受设计，RFC PR 已合入 `main`。实现可以在 tracking issue 下推进。 |
| `Implemented` | 必要实现阶段和验收标准已完成。 |
| `Superseded` | 有新的 accepted RFC 替代了这个 RFC。 |

Rejected 和 withdrawn 是 PR 结果，不是持久 RFC 状态。遇到这两种情况时，关闭
RFC PR 和 tracking issue，不要合并。

## 接受 RFC

1. 在 tracking issue 中记录验收标准。
2. 把 RFC metadata 从 `Proposed` 改为 `Accepted`。
3. 在最新 commit 上请求最终 review。
4. 只有 maintainer approval 和 CI 通过后才能合并。

合入 `main` 的那一刻，`Accepted` 才正式生效。

## 跟踪实现

tracking issue 用来收集：

- implementation checklist；
- 必要验收标准；
- 相关实现 PR；
- 未关闭的 follow-up question。

当必要工作全部完成后：

1. 新建文档 PR，把 RFC 状态改为 `Implemented`。
2. 链接已经完成的实现工作。
3. 合并状态更新 PR。
4. 在 tracking issue 中链接状态更新 PR，然后关闭 issue。

可选后续工作和 future possibilities 不阻塞 `Implemented`，除非它们属于必要验收
标准。

## 替代 RFC

1. 为替代设计创建新的 RFC。
2. 维护者接受替代设计后，把旧 RFC 状态改为 `Superseded`。
3. 在旧 RFC 的 `Superseded by` 中填写 successor。
4. 合并替代 RFC PR。
5. 在旧 tracking issue 中评论 successor RFC，然后关闭旧 issue。

仍然有效的实现任务应移动到 successor tracking issue。

## Agent checklist

创建或更新 RFC 时：

- 只以 RFC metadata 作为状态来源；
- 不新增、不更新中心化 index；
- RFC 文档直接放在 `rfcs/` 下；
- 状态变为 `Accepted`、`Implemented` 或 `Superseded` 时，同步更新
  tracking issue；
- accepted 之后的实质设计变化应新建 RFC；
- 小的修正或澄清可以用 follow-up PR。
