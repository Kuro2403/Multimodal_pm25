# Releasing

This project uses a lightweight release process centered on `CHANGELOG.md`.

## Release steps

1. Confirm `main` is green and the intended release changes are merged.
2. Update `CHANGELOG.md`:
   - Move items from `Unreleased` into a new version section.
   - Add release date in `YYYY-MM-DD` format.
   - Keep entries concise and user-facing.
3. Commit the changelog update in a PR and merge to `main`.
4. Create a tag from `main`:
   - `git tag -a vX.Y.Z -m "Release vX.Y.Z"`
   - `git push origin vX.Y.Z`
5. Create the GitHub Release for tag `vX.Y.Z` and paste the matching changelog section into release notes.
6. After release, add a fresh empty `Unreleased` section if needed for upcoming work.

## Scope guidance

- Keep this process intentionally small; do not block releases on heavyweight ceremony.
- If a change is user-visible, add it to `CHANGELOG.md` before tagging.
