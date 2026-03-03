# Praxis Copilot

## Git workflow
- Always create a new feature branch and open a PR. Never commit directly to master/main or pile onto an existing feature branch.
- When commenting on GitHub PRs, always prefix with [claude] to differentiate from human comments.

## Code style
- Prefer Pydantic models and strong typing wherever possible. Avoid passing raw dicts between functions.
- pydantic is already a project dependency.
- Always import at the module top level. No inline/dynamic imports inside functions.
