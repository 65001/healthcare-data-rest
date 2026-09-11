---
name: playwright-browser
description: >-
  Provides best practices, tool schemas, exact parameter conventions, and anti-patterns
  for automating browser testing and visual verification via the Playwright MCP server.
---

# Playwright MCP Browser Automation Runbook & Learnings

This skill captures key learnings, operational patterns, exact tool schemas, and common pitfalls when interacting with the lazy-loaded `playwright` MCP server.

---

## 1. Quick Reference: Core Tools & Parameters

The Playwright MCP tools are lazily loaded. Always call them via:
`call_mcp_tool` with `ServerName: "playwright"` and `ToolName: "<tool_name>"`.

| Tool Name | Key Parameters | Crucial Notes |
| :--- | :--- | :--- |
| `browser_navigate` | `url: string` (required) | Always verify URL protocol (`http://127.0.0.1:...`). |
| `browser_tabs` | `action: "list" \| "new" \| "close" \| "select"` (required), `index?: number`, `url?: string` | **Never pass empty `{}`!** `action` is strictly required. |
| `browser_wait_for` | `time?: number`, `text?: string`, `textGone?: string` | **CRITICAL: `time` is in SECONDS, NOT milliseconds!** Passing `3000` waits 50 minutes and hangs. Use `time: 2` or `time: 3`. |
| `browser_snapshot` | `depth?: number`, `target?: string`, `boxes?: boolean`, `filename?: string` | Prefer `depth: 4` or `target` to avoid massive multi-megabyte YAML context dumps. |
| `browser_take_screenshot` | `scale: "css" \| "device"` (required), `filename?: string`, `fullPage?: boolean` | `scale` is required. Use `"scale": "css"`. Automatically renders the screenshot inline. |
| `browser_console_messages`| `level: "error" \| "warning" \| "info" \| "debug"` (required) | Always specify `{"level": "error"}` to avoid dumping thousands of Vite/React debug logs. |
| `browser_click` | `target: string` (required), `button?: "left" \| "right" \| "middle"` | `target` is the snapshot element ref, e.g. `"[ref=f2e16]"`, or a unique CSS selector. |
| `browser_type` | `text: string` (required), `target: string` (required) | Types into input/textbox targeted by snapshot reference. |
| `browser_fill_form` | `elements: Array<{ target: string, value: string }>` | Efficiently batch fills forms in a single MCP invocation. |

---

## 2. Hard-Won Learnings & Anti-Patterns to Avoid

### ⚠️ Pitfall 1: Milliseconds vs. Seconds in `browser_wait_for`
* **Anti-Pattern**:
  ```json
  // WRONG: Waits 3,000 SECONDS (50 minutes) and blocks/times out!
  { "time": 3000 }
  ```
* **Correct Pattern**:
  ```json
  // CORRECT: Waits 3 seconds
  { "time": 3 }
  // EVEN BETTER: Wait for specific element/text to appear
  { "text": "CarePrice" }
  ```

---

### ⚠️ Pitfall 2: Empty Invocations on `browser_tabs`
* **Anti-Pattern**:
  Calling `{}` fails immediately with:
  `Invalid option: expected one of "list"|"new"|"close"|"select" -> at action`.
* **Correct Pattern**:
  ```json
  { "action": "list" }
  ```
  To switch to a tab:
  ```json
  { "action": "select", "index": 0 }
  ```

---

### ⚠️ Pitfall 3: Massive Console & Snapshot Dumps
* **Anti-Pattern**:
  Calling `browser_console_messages` with no level or `level: "info"` pulls thousands of lines of React DevTools, HMR, and bundle logs.
* **Correct Pattern**:
  ```json
  { "level": "error" }
  ```
  For snapshots:
  ```json
  { "depth": 4 }
  ```
  Or save snapshot/logs to a temporary file when inspecting deep component trees.

---

### ⚠️ Pitfall 4: Screenshots of `about:blank`
* **Symptom**: `browser_take_screenshot` returns a blank white box.
* **Cause**: The active Playwright page has not navigated, or a new tab was opened that is on `about:blank`.
* **Fix**:
  1. Check `browser_tabs` with `action: "list"`.
  2. Ensure `browser_navigate` has completed on the active tab before calling `browser_take_screenshot`.

---

## 3. Recommended Workflow for Page Verification

When testing frontend pages in this project (`http://127.0.0.1:5173/`):

1. **Navigate**:
   ```json
   {
     "ServerName": "playwright",
     "ToolName": "browser_navigate",
     "Arguments": { "url": "http://127.0.0.1:5173/procedures" }
   }
   ```
2. **Check for Fatal Errors**:
   ```json
   {
     "ServerName": "playwright",
     "ToolName": "browser_console_messages",
     "Arguments": { "level": "error" }
   }
   ```
3. **Inspect Elements (Shallow)**:
   ```json
   {
     "ServerName": "playwright",
     "ToolName": "browser_snapshot",
     "Arguments": { "depth": 3 }
   }
   ```
4. **Capture Visual State**:
   ```json
   {
     "ServerName": "playwright",
     "ToolName": "browser_take_screenshot",
     "Arguments": { "scale": "css" }
   }
   ```
5. **Interact with Form / Click**:
   Find the `ref` from step 3 (e.g. `[ref=f2e88]`):
   ```json
   {
     "ServerName": "playwright",
     "ToolName": "browser_type",
     "Arguments": { "target": "[ref=f2e88]", "text": "knee" }
   }
   ```
