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
