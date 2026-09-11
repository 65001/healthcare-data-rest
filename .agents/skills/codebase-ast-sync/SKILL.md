---
name: codebase-ast-sync
description: >-
  Provides runbook instructions and patterns for using Tree-Sitter via uv to extract codebase AST,
  verify documentation and skills synchronization, and audit symbols across Rust, TSX, and SQL.
---

# Codebase Tree-Sitter AST & Skills Sync Runbook

This skill guides agents on using Tree-Sitter AST analysis to monitor code changes, discover routes and models, and ensure skills and documentation remain synchronized.

## Prerequisites & Execution via `uv`

All Tree-Sitter parsers run in isolated virtual environments managed automatically by `uv`:

```bash
# 1. Scan codebase and print full AST inventory
uv run --with tree-sitter --with tree-sitter-rust --with tree-sitter-typescript --with tree-sitter-sql python scripts/codebase_ast.py scan

# 2. Output AST inventory in JSON format for automated tooling
uv run --with tree-sitter --with tree-sitter-rust --with tree-sitter-typescript --with tree-sitter-sql python scripts/codebase_ast.py scan --json

# 3. Verify synchronization between live AST and agent skills
uv run --with tree-sitter --with tree-sitter-rust --with tree-sitter-typescript --with tree-sitter-sql python scripts/codebase_ast.py check-sync

# 4. Generate Markdown reference tables from AST
uv run --with tree-sitter --with tree-sitter-rust --with tree-sitter-typescript --with tree-sitter-sql python scripts/codebase_ast.py markdown
```

## When Agents Should Run This Tool

Agents should run `codebase_ast.py` in the following scenarios:
1. **After adding or modifying backend routes**:
   - Run `check-sync` to verify that any new Axum route in `backend/src/routes/mod.rs` is documented with its parameters in `.agents/skills/healthcare-backend-api/SKILL.md`.
2. **After adding new frontend pages or components**:
   - Run `scan` to verify AST recognition of new pages in `frontend/src/pages/`.
   - Update `.agents/skills/healthcare-frontend/SKILL.md` with new page or component conventions.
3. **Before opening a PR or completing a feature task**:
   - Run `check-sync` to ensure exit code 0 (no documentation drift or undocumented endpoints).

## Extracted AST Elements
- **Rust (`tree-sitter-rust`)**:
  - Axum route handlers (`.route(...)`), methods (`GET`, `POST`), paths
  - Struct definitions (`struct ...`), field names, types, and counts
  - Repository impl methods (`fn search_...`)
- **TypeScript / TSX (`tree-sitter-typescript`)**:
  - React exported page functions in `frontend/src/pages/`
  - Reusable components across `frontend/src/components/`
  - API endpoint paths in `frontend/src/api/client.ts`
- **DuckLake SQL (`tree-sitter-sql`)**:
  - Convenience views (`current_hospitals`, `current_charges`)
  - Lake versioned tables (`lake.standard_charge_details`, etc.)
