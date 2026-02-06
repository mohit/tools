# Repository Guidelines

## Project Structure & Module Organization
- Each tool is a top-level folder (e.g., `firefox-2fa-autofill/`).
- Tool-specific docs live inside each tool folder (typically `README.md`, and optional `AGENTS.md`).
- Shared repo docs live at the root (`README.md`, `CONTRIBUTING.md`, `LICENSE`).

## Build, Test, and Development Commands
- Commands are tool-specific; check each tool’s `README.md` for build/run steps.
- Example (Firefox extension): load `firefox-2fa-autofill/manifest.json` via `about:debugging`.
- If you add a tool with a CLI, include a `make` or `npm` command example in that tool’s README.

## Coding Style & Naming Conventions
- Follow the conventions of each tool’s language and ecosystem.
- Use consistent, descriptive folder names at the repo root (kebab-case is preferred).
- Keep scripts and binaries inside the tool folder; avoid adding them to the repo root.

## Testing Guidelines
- No repo-wide test runner is configured.
- Document any tool-specific testing commands in the tool’s README.
- If you add automated tests, note frameworks and naming patterns in that tool’s docs.

## Commit & Pull Request Guidelines
- No enforced commit message convention; keep messages concise and action-oriented.
- PRs should include: a short summary, verification steps, and screenshots/GIFs for UI changes.
- Link related issues when applicable.

## Security & Configuration Tips
- Do not introduce remote telemetry or data exfiltration by default.
- If a tool handles credentials or codes, keep processing local and document retention policies.

## Agent Workflow Standards
- Keep shared agent instructions in this root `AGENTS.md` so all agents can consistently load the same guidance.
- Use focused branches and PRs for a single feature/focus area at a time (use `codex/*` for Codex-created branches).
- Resolve conflicts locally before merge; avoid relying on ad-hoc web edits that can drift from local verification.
- Leave code better than found, without scope creep:
- If you find a real issue in files already being changed, fix it in the same PR when practical.
- If an issue is outside current scope, open a clear, detailed GitHub issue with context, impact, and proposed follow-up.
- Add tests that capture intended behavior for every new feature or meaningful behavior change so regressions are easy to detect.
- Update documentation in required places (`README.md`, tool-level docs, optional tool `AGENTS.md`) so architecture, goals, and tool choices remain clear for future agents.
- Exclude local artifacts and machine-specific files (for example `.cursor/`) from commits and PRs.
