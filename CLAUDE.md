# Project Rules

## Neural Memory - Always Active

At the START of every session, ALWAYS run:
1. `nmem_recap()` — load previous context
2. `nmem_context(limit=10)` — get recent memories
3. `nmem_session(action="get")` — resume session state

Before answering questions about past work, ALWAYS `nmem_recall(query="...")` first.

Automatically save important info during conversations:
- Decisions → `nmem_remember(type="decision")`
- Errors/fixes → `nmem_remember(type="error")`
- User preferences → `nmem_remember(type="preference")`
- Project context → `nmem_remember(type="context")`

At the END of a session or before `/compact`, run:
- `nmem_auto(action="flush", text="<recent context>")`
- `nmem_session(action="end")`

---

# All Rules — Master Reference (87 rules)

> Auto-generated from 9 rule files + MEMORY.md
> Last updated: 2026-02-24

---

## CODING STYLE (10 rules) — `rules/coding-style.md`

| # | Rule | Detail |
|---|------|--------|
| 1 | **Immutability** | LUÔN tạo object mới, KHÔNG BAO GIỜ mutate. Dùng spread `{...obj}` |
| 2 | **File size** | 200-400 dòng typical, 800 max |
| 3 | **Organize by feature/domain** | Không organize by type (controllers/, models/) |
| 4 | **High cohesion, low coupling** | Extract utilities từ large components |
| 5 | **Error handling** | LUÔN try/catch, throw Error với message user-friendly |
| 6 | **Input validation** | LUÔN validate bằng Zod schema |
| 7 | **Functions < 50 lines** | Nhỏ gọn, single responsibility |
| 8 | **No deep nesting** | Max 4 levels. Early return thay vì nested if |
| 9 | **No console.log** | Không có trong production code |
| 10 | **No hardcoded values** | Dùng constants, env vars, config |

---

## GIT WORKFLOW (8 rules) — `rules/git-workflow.md`

| # | Rule | Detail |
|---|------|--------|
| 11 | **Commit format** | `<type>: <description>` — types: feat, fix, refactor, docs, test, chore, perf, ci |
| 12 | **No co-author line** | Attribution disabled globally |
| 13 | **PR: full commit history** | Analyze tất cả commits, không chỉ latest |
| 14 | **PR: git diff base...HEAD** | Xem toàn bộ changes so với base branch |
| 15 | **PR: comprehensive summary** | Include test plan với TODOs |
| 16 | **PR: push -u** | Dùng flag `-u` nếu new branch |
| 17 | **Plan First** | Dùng **planner** agent trước khi code |
| 18 | **Feature workflow** | Plan → TDD → Code Review → Commit |

---

## TESTING (7 rules) — `rules/testing.md`

| # | Rule | Detail |
|---|------|--------|
| 19 | **Minimum coverage 80%** | Không merge nếu dưới 80% |
| 20 | **Unit Tests required** | Individual functions, utilities, components |
| 21 | **Integration Tests required** | API endpoints, database operations |
| 22 | **E2E Tests required** | Playwright, critical user flows |
| 23 | **TDD MANDATORY** | RED (write test) → GREEN (implement) → IMPROVE (refactor) |
| 24 | **Fix implementation, not tests** | Trừ khi test sai |
| 25 | **Testing agents** | tdd-guide (proactive), e2e-runner (Playwright) |

---

## SECURITY (9 rules) — `rules/security.md`

| # | Rule | Detail |
|---|------|--------|
| 26 | **No hardcoded secrets** | API keys, passwords, tokens → env vars |
| 27 | **All user inputs validated** | Zod, parameterized queries |
| 28 | **SQL injection prevention** | Parameterized queries only |
| 29 | **XSS prevention** | Sanitized HTML output |
| 30 | **CSRF protection** | Enabled on all forms/endpoints |
| 31 | **Auth verified** | Authentication + authorization checked |
| 32 | **Rate limiting** | On all public endpoints |
| 33 | **Error messages safe** | Không leak sensitive data trong errors |
| 34 | **Security protocol** | STOP → security-reviewer agent → fix CRITICAL → rotate secrets → review codebase |

---

## HOOKS (8 rules) — `rules/hooks.md`

| # | Rule | Detail |
|---|------|--------|
| 35 | **PreToolUse: tmux reminder** | Gợi ý tmux cho npm, pnpm, yarn, cargo, etc. |
| 36 | **PreToolUse: git push review** | Mở Zed review trước khi push |
| 37 | **PreToolUse: doc blocker** | Block tạo file .md/.txt không cần thiết |
| 38 | **PostToolUse: PR creation** | Log PR URL + GitHub Actions status |
| 39 | **PostToolUse: Prettier** | Auto-format JS/TS sau mỗi edit |
| 40 | **PostToolUse: TypeScript check** | Run tsc sau edit .ts/.tsx |
| 41 | **PostToolUse: console.log warning** | Warn nếu có console.log trong edited files |
| 42 | **Stop: console.log audit** | Check tất cả modified files trước khi kết thúc session |

---

## PATTERNS (4 rules) — `rules/patterns.md`

| # | Rule | Detail |
|---|------|--------|
| 43 | **API Response format** | `ApiResponse<T>` chuẩn: success, data, error, meta (total/page/limit) |
| 44 | **Custom Hooks pattern** | useDebounce template cho React hooks |
| 45 | **Repository pattern** | Generic CRUD interface: findAll, findById, create, update, delete |
| 46 | **Skeleton Projects** | Search battle-tested skeleton → parallel agent evaluate → clone → iterate |

---

## PERFORMANCE (5 rules) — `rules/performance.md`

| # | Rule | Detail |
|---|------|--------|
| 47 | **Haiku** | Lightweight/worker agents, pair programming (3x tiết kiệm) |
| 48 | **Sonnet** | Main dev work, orchestrating, complex coding |
| 49 | **Opus** | Complex architecture, deep reasoning, research |
| 50 | **Context Window** | Tránh 20% cuối cho large refactoring/multi-file features |
| 51 | **Ultrathink + Plan Mode** | Complex tasks: ultrathink → Plan Mode → multiple critique → split sub-agents |

---

## AGENT ORCHESTRATION (7 rules) — `rules/agents.md`

| # | Rule | Detail |
|---|------|--------|
| 52 | **Auto-route: feature → planner** | Complex feature requests tự động dispatch |
| 53 | **Auto-route: code → code-reviewer** | Code vừa viết/modified → review ngay |
| 54 | **Auto-route: bug → tdd-guide** | Bug fix hoặc new feature → TDD |
| 55 | **Auto-route: architecture → architect** | Architectural decisions → architect agent |
| 56 | **Auto-route: tests → codex-sandbox** | Run tests/build/lint trong sandbox |
| 57 | **Auto-route: security → codex-security** | Security audit, secret detection, CVE |
| 58 | **Auto-route: research → gemini-researcher** | Large codebase (>50 files), docs research |

---

## BRAIN / NEURAL MEMORY (10 rules) — `rules/brain.md`

| # | Rule | Detail |
|---|------|--------|
| 59 | **Session start: nmem_recap** | Load project context (level 2) + nmem_session get |
| 60 | **Session end: nmem_auto** | Process conversation highlights + nmem_session end |
| 61 | **Always remember decisions** | nmem_eternal với decision + reason |
| 62 | **Always remember bug patterns** | nmem_remember type: insight, priority: 8+ |
| 63 | **Always remember preferences** | nmem_remember type: preference, priority: 9 |
| 64 | **Before commit: capture learnings** | nmem_auto (action: process) |
| 65 | **After milestones: snapshot** | nmem_version (action: create) |
| 66 | **Before work: recall topic** | nmem_recall relevant topic, depth 1-2 |
| 67 | **Before architecture: check history** | nmem_recall depth 2 + nmem_conflicts check |
| 68 | **Weekly: brain health** | nmem_health + nmem_review queue + resolve conflicts |

---

## PROJECT RULES — MEMORY (8 rules) — `MEMORY.md`

| # | Rule | Detail |
|---|------|--------|
| 69 | **No Spec = No Code** | Viết mini-spec, chờ user OK, rồi mới code |
| 70 | **Update docs after code** | CHANGELOG.md, active_context.md, START.md, CLAUDE.md |
| 71 | **Read CLAUDE.md + START.md** | Mỗi session start cho full context |
| 72 | **Proactive bug scanning** | Tự tìm bugs, không chờ external tools |
| 73 | **Bug scanner** | `python scripts/scan_bugs.py src/` — AST-based, blocks P1 |
| 74 | **Chandelier Exit** | BUY + SELL đều dùng `chandelier_long`, trailing_sl ratchet LUÔN apply |
| 75 | **Cross-file analysis** | Track dataclass definitions → verify attribute access + function signatures |
| 76 | **User = Orchestrator** | Chỉ đạo, không code. Workflow: problem → spec → OK → implement → review → commit |

---

## CODE SAFETY (6 rules)

| # | Rule | Detail |
|---|------|--------|
| 82 | **No blind rewrite** | KHÔNG dùng Write tool rewrite cả file. LUÔN dùng Edit (find-replace). Nếu BẮT BUỘC rewrite: liệt kê TẤT CẢ functions/classes trước và sau, verify không mất gì |
| 83 | **Grep before remove** | Trước khi xóa function/variable/config: `grep` tất cả references across ALL files. Xóa hết references trước khi xóa definition |
| 84 | **Syntax check before deploy** | `python -c "import ast; ast.parse(open(f).read())"` cho MỌI file đã sửa, TRƯỚC khi scp |
| 85 | **Post-deploy verify** | Sau restart: đợi 10s, check logs cho errors/warnings. Nếu có lỗi → fix ngay, KHÔNG để user phải phát hiện |
| 86 | **VPS = verify after deploy** | Sau mỗi deploy: verify file sizes match hoặc spot-check content. KHÔNG assume scp thành công = code đúng |
| 87 | **Hardcoded values = read from code** | Khi user nói thay threshold/tier: ĐỌC code hiện tại trước, KHÔNG assume giá trị từ memory — memory có thể stale |

---

## META RULES (4 rules)

| # | Rule | Detail |
|---|------|--------|
| 77 | **Vietnamese speaker** | Trả lời concise, tiếng Việt khi user nói tiếng Việt |
| 78 | **Parallel execution** | LUÔN chạy parallel cho independent tasks/agents |
| 79 | **Multi-perspective analysis** | Claude (quality) + Codex (security) + Gemini (architecture) |
| 80 | **TodoWrite tracking** | Dùng TodoWrite cho multi-step tasks, track progress real-time |
| 81 | **"Go on" = Resume** | Khi user nói "go on": chạy `nmem_recap(level=2)` + `nmem_context(limit=20)` + `nmem_session(action="get")` để đọc lại toàn bộ context trước đó, rồi tiếp tục công việc dang dở |

---

## Quick Reference — Rule Sources

| File | Rules | Range |
|------|-------|-------|
| `rules/coding-style.md` | 10 | #1–10 |
| `rules/git-workflow.md` | 8 | #11–18 |
| `rules/testing.md` | 7 | #19–25 |
| `rules/security.md` | 9 | #26–34 |
| `rules/hooks.md` | 8 | #35–42 |
| `rules/patterns.md` | 4 | #43–46 |
| `rules/performance.md` | 5 | #47–51 |
| `rules/agents.md` | 7 | #52–58 |
| `rules/brain.md` | 10 | #59–68 |
| `MEMORY.md` | 8 | #69–76 |
| Code Safety | 6 | #82–87 |
| Meta (implicit) | 5 | #77–81 |
| **Total** | **87** | |
