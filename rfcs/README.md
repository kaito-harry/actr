# actr RFC Guide

Chinese version: [README.zh.md](README.zh.md).

This directory records accepted design decisions for actr changes that are
cross-cutting, hard to reverse, or likely to affect long-term contracts.

Use this guide as an operating checklist for creating and managing RFCs. The
metadata in each RFC file is the source of truth. Do not maintain a hand-written
RFC index; the directory listing and Git history are enough.

## Files

```text
rfcs/
├── 0000-template.md        # Copy this when starting a new RFC
├── README.md              # English process guide
├── README.zh.md           # Chinese process guide
└── NNNN-short-name.md     # RFC documents
```

Append a language code before `.md` for a non-English RFC, for example
`0323-explicit-reply.zh.md`.

## When to write an RFC

Write an RFC before implementation when the change:

- changes protocol, wire format, concurrency, or reply semantics;
- coordinates behavior across crates or layers such as `core/hyper`,
  `framework`, code generation, FFI, or WIT;
- adds a public API or option that will be difficult to withdraw after release;
- needs a durable decision among meaningful alternatives.

An RFC is usually unnecessary for:

- bug fixes;
- documentation-only changes;
- local refactors that do not change behavior;
- internal improvements with no caller-visible effect;
- incremental metric improvements such as performance, platform coverage,
  parallelism, or warning coverage.

## Create an RFC

1. Open a tracking issue named `RFC: <name>`.
2. Use the issue number as the RFC number. Issue `#323` becomes `RFC-0323`.
3. Copy `0000-template.md` to `NNNN-short-name.md`.
4. Fill every template section, including `Status`, `RFC PR`, and
   `Tracking issue`.
5. Set `Status: Proposed`.
6. Open a pull request named `docs: add RFC-NNNN <name>`.
7. Keep the RFC PR open while it is `Proposed`; do not merge a proposed RFC.

RFCs should include enough background for future readers. Avoid relative links
to repository files outside `rfcs/`; those files may move or be deleted. Mention
code paths such as `core/.../file.rs` as text when needed, and link to issues,
pull requests, or external references for discussion history.

## Statuses

Use only these persisted statuses:

| Status | Meaning |
|---|---|
| `Proposed` | The RFC is under review in an open PR and is not merged. |
| `Accepted` | Maintainers have accepted the design, and the RFC PR has merged into `main`. Implementation may proceed under the tracking issue. |
| `Implemented` | Required implementation phases and acceptance criteria are complete. |
| `Superseded` | A newer accepted RFC replaces this RFC. |

Rejected and withdrawn proposals are PR outcomes, not persisted RFC statuses.
Close the RFC PR and tracking issue without merging them.

## Accept an RFC

1. Record acceptance criteria in the tracking issue.
2. Update the RFC metadata from `Proposed` to `Accepted`.
3. Request final review on the latest commit.
4. Merge only after maintainer approval and passing CI.

The merge into `main` is the point where `Accepted` takes effect.

## Track implementation

Use the tracking issue to collect:

- implementation checklist items;
- required acceptance criteria;
- related implementation PRs;
- open follow-up questions.

When all required work is done:

1. Open a documentation PR that changes the RFC status to `Implemented`.
2. Link the completed implementation work.
3. Merge the status update PR.
4. Close the tracking issue with a link to the status update.

Optional follow-up work and future possibilities do not block `Implemented`
unless they were part of the required acceptance criteria.

## Supersede an RFC

1. Create a new RFC for the replacement design.
2. When maintainers accept the replacement, update the old RFC to
   `Superseded`.
3. Fill `Superseded by` in the old RFC.
4. Merge the replacement RFC PR.
5. Comment on the old tracking issue with the successor RFC and close it.

Move any still-relevant implementation tasks to the successor tracking issue.

## Agent checklist

When creating or updating an RFC:

- keep the status in the RFC metadata as the only status source;
- do not add or update a central index;
- keep RFC documents directly under `rfcs/`;
- update the tracking issue when status changes to `Accepted`, `Implemented`,
  or `Superseded`;
- use a new RFC for material design changes after acceptance;
- use small follow-up PRs only for corrections or clarifications.
