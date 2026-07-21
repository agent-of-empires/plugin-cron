# Releasing

Release automation mirrors Agent of Empires core and `plugin-github`: a
maintainer dispatches one workflow with a version, reviews the generated PR, and
merging it tags, publishes, and opens the featured-index PR.

## One-time setup

Configure these in the repository settings before the first release:

- **Secret `RELEASE_TOKEN`** — a PAT with `contents: write` + `pull-requests: write`
  on this repo. Used so the release-staging branch push, PR, and tag push come
  from a real user (a tag pushed with the default `GITHUB_TOKEN` does not
  trigger `release.yml`).
- **Secret `AOE_FEATURED_PR_TOKEN`** — a PAT with `contents: write` +
  `pull-requests: write` on `agent-of-empires/agent-of-empires`. Used by the
  post-release job to open the featured-index PR. If unset, the release still
  publishes; the featured PR step is skipped with a warning.
- **Environment `release`** — with a required reviewer. It gates both the
  release PR and the cross-repo featured PR.

`agent-of-empires.cron` is a reserved namespace, so the plugin only installs
once it is pinned in AoE's `plugins/featured.toml`. The featured PR is what
pins it.

## Cutting a release

1. **Actions → Prepare Release → Run workflow**, enter the version without the
   `v` (e.g. `1.0.0`). Approve the `release` environment gate.
   This opens a `release-staging/vX.Y.Z` PR that bumps `Cargo.toml`,
   `Cargo.lock`, and `aoe-plugin.toml`, and regenerates `CHANGELOG.md`.
2. **Review and merge** the release-staging PR once CI is green.
3. On merge, `tag-release-pr.yml` verifies the versions agree and pushes the
   `vX.Y.Z` tag.
4. The tag triggers `release.yml`: it runs the checks, publishes a GitHub
   Release (source archive + sha256 + git-cliff notes), then opens a PR on
   `agent-of-empires/agent-of-empires` pinning the version's source tree hash in
   `plugins/featured.toml`.
5. **Merge the featured-index PR** in the AoE repo. The plugin is now installable
   at that version.

The version lives in three places kept in lockstep by Prepare Release: the crate
`version` (`Cargo.toml` + its `Cargo.lock` entry) and the manifest `version`
(`aoe-plugin.toml`). The tagger refuses to tag if the crate and manifest
versions disagree.
