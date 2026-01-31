---
name: release-new-version
description: "Create a new release in sentinel-video-receiver (or similar) by committing changes with a short no-emoji message, pushing to main, deriving the next version from the latest git tag, creating the tag, and pushing it. Use when the user says 'new release', 'cut a release', 'tag a release', 'bump version', or asks to commit+tag+push."
---

# Release New Version

## Workflow

1. Confirm repository root and target branch (default: `main`).
2. Check `git status --short` and summarize changes; stop if there is nothing to commit.
3. Determine latest tag:
   - Prefer semantic version tags: `git tag --list 'v*' --sort=-version:refname | head -n 1`.
   - If none found, ask the user for the initial version (suggest `v0.1.0`).
4. Decide next version from the latest tag:
   - Default to patch bump unless the user asks for minor/major.
   - Example: `v1.2.3` -> `v1.2.4` (patch), `v1.3.0` (minor), `v2.0.0` (major).
5. Create a commit with a short message and **no emojis**.
6. Push commits to the target branch (default: `main`).
7. Create a tag for the new version (annotated by default): `git tag -a vX.Y.Z -m "vX.Y.Z"`.
8. Push the tag: `git push origin vX.Y.Z`.

## Notes

- If the latest tag is not semver or multiple tag prefixes exist, ask which pattern to follow.
- If pushing to `main` is blocked by branch protection, stop and ask how to proceed.
- Keep the commit message under ~60 characters and use imperative style.
