# CLAUDE.md — Instructions for AI Agents

## STOP — read this first

**DO NOT create documentation `.md` files inside this project folder.**

All project documentation lives in the **Second Brain vault**, not in the repository. The vault is the single source of truth for design notes, decisions, how-tos, task tracking, and any documentation an agent writes. Follow the workflow below on every session.

---

## Quick Reference

| What | Where |
|---|---|
| Vault root | `G:\My Drive\Obsi\Second-Brain` |
| Master AI_CONTEXT.md (vault) | `G:\My Drive\Obsi\Second-Brain\00_System\AI_CONTEXT.md` |
| Project docs folder | `G:\My Drive\Obsi\Second-Brain\01_Projects\real-time-data-integration-platform\` |
| Templates | Per vault structure — see `00_System/AI_CONTEXT.md` (templates section) |
| Tags | Per vault convention — see `00_System/AI_CONTEXT.md` (tags section) |

> The master manual at `00_System/AI_CONTEXT.md` in the vault is the **authoritative** reference for vault structure, naming conventions, tags, and placement rules. Always read it before writing anything to the vault.

---

## Workflow (numbered)

1. **READ** the master manual: `00_System/AI_CONTEXT.md` in the vault (vault structure, naming, tags, placement rules).
2. **READ** this project's README (`README.md`) for technical context.
3. **FIND** existing docs in the project docs folder before creating anything new:
   `01_Projects/real-time-data-integration-platform/`
4. **CREATE** new docs in the vault — never in the project folder.
5. **UPDATE** `Tasks.md` in the project docs folder whenever a task or decision is recorded.
6. **LINK** related notes with `[[wikilinks]]` (Obsidian-style).

---

## The Rule

> **Everything goes to the brain. Nothing stays in the project folder.**

- No new `.md` documentation files in this repository (except this `CLAUDE.md` and existing project files).
- No "docs" copies of vault content in the repo.
- If a doc belongs to the project, it goes to `01_Projects/real-time-data-integration-platform/`.

---

## Project Context

**Real-Time Data Integration Platform**

A production-oriented event-processing platform that captures database changes in real time, streams them through Apache Kafka, and processes those events through independent Go microservices with Redis caching and a live operator dashboard.

- **Pipeline**: REST API (`POST /orders`) → PostgreSQL → Debezium CDC (WAL) → Kafka (KRaft) → three independent Go consumer services (Inventory, Notification, Analytics), each with its own consumer group, worker pool, Redis state, and idempotency protection.
- **Stack**: Go, PostgreSQL, Apache Kafka, Debezium, Redis, Docker Compose.
- **Docs**: `README.md` and `docs/` in the repo hold the technical guides; everything else goes to the vault.

Project docs folder: `G:\My Drive\Obsi\Second-Brain\01_Projects\real-time-data-integration-platform\`
