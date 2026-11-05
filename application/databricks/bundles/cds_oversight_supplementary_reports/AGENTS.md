# Project Instructions

These instructions apply both to the DataForge template and to standalone
projects generated from it. When maintaining DataForge inside a larger source
repository, also follow any applicable ancestor instructions. Keep this file
complete for standalone use; do not replace its general instructions with links
to files outside the copied project.

## DataBridge naming

- DataBridge is the current name for the ingestion project and service formerly
  called DIS/OEDIS. Its sibling source folder is `databridge`, and its vendored
  contract snapshot is under `docs/reference/databridge`.
- DataBridge intentionally retains the legacy `oedis` Python package,
  `oedis.handler.*` contracts, schema identifiers, DIS-prefixed client APIs and
  variables, and OEDIS-named infrastructure. Treat those as compatibility
  identifiers, not as the current service name.

## DataKit naming

- DataKit is the current name for the shared-library project formerly called
  Common/OEDS Common. Its sibling source folder is `datakit`, and its vendored
  reference is under `docs/reference/datakit`.
- DataKit intentionally retains the `oeds-common` distribution, `oeds.core`
  import namespace, package version, public APIs, and `python-wheel` artifact.

## Project responsibility and shared contracts

- Keep business-pipeline behavior and DataBridge client integration in this
  project. Keep server-side ingestion runtime and orchestration in DataBridge.
- Reuse documented DataKit APIs for generic, domain-neutral behavior. Do not
  copy shared library implementations into this project or make DataKit depend
  on project-specific code.
- Treat DataBridge request, response, task-result, and error structures as
  compatibility contracts. Reuse existing models, helpers, and exception types;
  do not introduce client-only interpretations or one-off output shapes.
- In DataBridge client APIs, use typed exceptions to distinguish invalid
  inputs, client-side failures, and service-side failures.
- Keep client verification explicit and testable, with its assumptions visible
  to callers. Keep it separate from job submission unless the API explicitly
  combines them.
- Preserve backward compatibility unless a breaking change is explicitly
  authorized. When a change requires a dependency contract update, coordinate
  it with the owning project and affected consumers. If that source is not
  available, report the required change rather than guessing its behavior.

## Generated project identity and documentation

- When a project is created or initialized from DataForge, the copied
  `dataforge` folder is renamed to the approved project bundle name. Treat the
  directory containing this `AGENTS.md` as the bundle root and resolve paths
  relative to it; do not assume the folder is still named `dataforge`.
- In generated projects, preserve the original documentation inherited from
  DataForge under `docs/`. Do not edit, rename, move, or delete those documents
  as part of project initialization or project-specific development.
- Keep all project-specific documentation under `docs/project/`, creating the
  directory when needed. Document project behavior, configuration, and
  differences from the template there, linking to inherited documentation
  instead of modifying it.
- Keep the required README catalogs as concise indexes; link to
  `docs/project/` for project-specific details.
- These documentation rules apply to generated projects. They do not prevent
  maintaining documentation in the original DataForge template.

## Project catalogs

- Maintain `src/oeds/domain/README.md` whenever a top-level domain package or
  module is added, removed, renamed, or changes the location of its detailed
  context.
- Maintain `conf/oeds/config/README.md` whenever a configuration area under
  `conf/oeds/config/` is added, removed, renamed, or changes its purpose or
  detailed context.
- Maintain `src/oeds/domain/utils/README.md` whenever a module under
  `src/oeds/domain/utils/` is added, removed, renamed, or changes its public
  APIs, Databricks assumptions, dependencies, side effects, restrictions, or
  linked guide, example, and test coverage.
- Keep the catalog concise, document public APIs only, and preserve valid
  relative links.

## Project configuration

- Keep project-owned configuration under `conf/oeds/config/<area>` and import
  it through `oeds.config.<area>`.
- Treat `conf` as a Python source root and keep `conf/oeds` as a namespace
  package without an `__init__.py`.
- Keep reusable workflow code under `src/oeds/domain`; do not place project
  configuration back under `src/oeds/domain`.
- Keep secrets, credentials, and environment-specific sensitive values outside
  source-controlled configuration.

## Synchronized dependency references

- Treat `docs/reference/datakit/api/`,
  `docs/reference/datakit/guides/`, `docs/reference/databridge/docs/`, and
  `docs/reference/databridge/examples/` as vendored snapshots owned by their
  source projects. Do not edit files in those directories directly.
- Synchronize or verify those snapshots only when the developer explicitly
  requests `/sync-references`. A DataKit or DataBridge code or documentation
  change does not authorize synchronization into DataForge.
- The synchronization skill requires sibling DataKit and DataBridge source
  projects.
  Report unavailable or stale sources instead of guessing or fetching them.

## DataKit library API reference

- DataForge consumes the `oeds-common` package version declared in
  `cluster/requirements.txt`; a sibling DataKit source project is not available
  in standalone projects.
- Before using or changing code that imports `oeds.core`, start with
  `docs/reference/datakit/README.md`. Use the generated API index and module
  pages under `docs/reference/datakit/api/` for the complete public
  inventory, exact signatures, and source docstrings, and use
  `docs/reference/datakit/guides/` for operational guidance.
- Use only public APIs documented in that reference. Do not infer missing
  functions, parameters, return behavior, exceptions, or runtime assumptions.
- Confirm that the reference version is compatible with the package requirement
  before relying on it. Report a missing, stale, or conflicting contract rather
  than guessing.

## DataBridge reference

- Before changing DataBridge client integration, handler configuration, or
  task-result handling, start with `docs/reference/databridge/docs/` and
  consult the matching examples under `docs/reference/databridge/examples/`.
- Treat those files as the DataBridge server contract snapshot. Report missing,
  stale, or conflicting behavior instead of inferring an undocumented contract.

## API selection

- Before adding Python or notebook processing code, search the
  [DataForge utilities catalog](src/oeds/domain/utils/README.md) and the
  [DataKit reference](docs/reference/datakit/README.md). Inspect the selected
  local source or documented public signature and relevant examples or tests.
- Reuse DataForge APIs for pipeline-specific behavior and DataKit APIs for
  generic behavior. Extend a compatible API in its owning project before
  creating a new implementation; explain any missing capability.
- Use documented public package imports only. Do not use private members,
  import directly from a DataKit checkout, or add forwarding-only wrappers.
- Treat `oeds.config` definitions as project configuration, not reusable APIs.

## Task skills

Read the applicable skill before its workflow. These links identify
workflow instructions; slash-command discovery depends on the agent in use.

| Skill | Purpose |
|---|---|
| [init-project](.agents/skills/init-project/SKILL.md) | Initialize once; stop if `databricks.yml` exists. |
| [upgrade-dataforge](.agents/skills/upgrade-dataforge/SKILL.md) | Confirmed partial template upgrade. |
| [create-table](.agents/skills/create-table/SKILL.md) | Create one table reference. |
| [create-view](.agents/skills/create-view/SKILL.md) | Create one view reference. |
| [config-ingest](.agents/skills/config-ingest/SKILL.md) | Compose ingestion configuration. |
| [sync-references](.agents/skills/sync-references/SKILL.md) | Explicitly requested reference synchronization/check. |
| [release-dataforge](.agents/skills/release-dataforge/SKILL.md) | Template release, including reference sync; confirm publication. |
| [tag-dataforge](.agents/skills/tag-dataforge/SKILL.md) | Confirmed tag after the template release merges. |
| [release-project](.agents/skills/release-project/SKILL.md) | Prepare project release metadata. |
| [tag-project](.agents/skills/tag-project/SKILL.md) | Confirmed project release tag. |

After initialization, edit `databricks.yml` and job resources directly. Keep
`common.py` fallbacks and `resources/jobs/common.yml` aligned. Initialization
uses `default.databricks.yml` and adjacent comment mappings and removes only
the copied default after successful validation. Do not read or remove legacy
domain YAML files as initialization inputs or cleanup targets.

# Common Instructions



## Git workflow

- At the start of every new task or thread, create a new Git branch before
  making code changes.
- Branch names must use the `code/` prefix.
- Prefer short, descriptive branch names derived from the task, for example
  `code/fix-login-timeout`.
- Do not commit directly to `main`.

## Request evaluation and verification

Before acting, independently evaluate my request for incorrect premises, logical
gaps, missing information, ambiguity, and hidden risks. Do not automatically
accept my proposed solution; recommend a safer, simpler, or more maintainable
approach when appropriate.

Clearly distinguish confirmed facts, reasonable inferences, and unverified
assumptions. Verify technical claims involving code, data, APIs, versions,
security, or system behavior using available evidence and authoritative sources;
never fabricate details or results.

Ask focused questions when missing information could materially change the
outcome. For minor, low-risk gaps, proceed with clearly stated assumptions.
Before making changes, assess maintainability, compatibility, security,
operational impact, reversibility, and effects on existing systems or users.

Apply analysis proportionally: avoid both blind execution and unnecessary
over-analysis. After acting, verify the result where possible and report what was
done, what was confirmed, remaining assumptions, and any important risks or
follow-up actions.

## Change scope

- Prefer minimal, targeted changes.
- Do not refactor unrelated code unless explicitly requested.
- Preserve existing behavior unless the task explicitly requires a logic
  change.
- Distinguish clearly between required fixes and optional cleanup.

## General engineering

- Reuse existing patterns before introducing new ones.
- Validate inputs and configuration instead of relying on implicit
  assumptions.
- Raise exceptions for real failures rather than returning partial success.
- Preserve useful error context and stack traces.
- Avoid swallowing exceptions unless there is a clear recovery path.
- Never log secrets, credentials, tokens, or sensitive payload contents.

## Configuration

- Keep configuration structures consistent across handlers and bundle projects.
- Validate resolved configuration before execution where possible.
- Avoid handler-specific configuration patterns unless necessary and clearly
  documented.

## Logging and error handling

- Log major execution steps, path resolution, verification, and external I/O.
- Use `debug` for verbose diagnostics and `info` for major lifecycle events.
- Keep logs actionable and include task or handler identifiers where possible.
- Use project-specific exception types where available.
- Preserve useful stack traces, especially for Databricks job runs.
- Never log secrets, credentials, tokens, or sensitive payload contents.

## Storage and Databricks behavior

- Prefer shared helpers for path normalization.
- Be explicit about local paths, `file:`, `dbfs:`, `/Volumes`, and cloud URIs.
- Do not mix path conventions or filesystem APIs without a clear need.
- Keep notebook and task return payloads compact and structured.
- Put detailed diagnostics in logs rather than notebook exit payloads.
- Design for Databricks job-output limits and storage semantics.

## PR workflow

### Provider selection

- Determine the repository provider from `git remote get-url origin`.
- Treat `github.com` origins as GitHub repositories.
- Treat `dev.azure.com`, `visualstudio.com`, and `ssh.dev.azure.com` origins as
  Azure DevOps repositories.
- If the provider cannot be determined, stop and ask the user.
- Do not infer the source repository provider from pipeline files in the
  repository.

### Shared workflow

- Keep the flow compact:
  1. Run only the necessary validation for the changed files.
  2. Summarize the completed changes and validation results, then wait for the
     user's explicit confirmation before staging, committing, pushing, creating
     a PR, or merging.
  3. After confirmation, stage and commit the intended files.
  4. Push the current branch to origin.
  5. Create or reuse an active PR from the current branch to `main`.
  6. Complete or merge the PR, then poll briefly until it is merged or a
     policy/merge error is returned.
  7. After a successful merge, delete the remote source branch.
  8. Switch back to `main`, fast-forward pull, delete the local source branch,
     and prune deleted remote branches.
- Use PowerShell-native scripts and command sequencing.
- Do not run repeated `git status`, `git diff`, or validation checks unless the
  previous step failed or new changes appeared.
- If completion succeeds, report the PR ID, source commit, merge commit, and
  final local branch status.

### GitHub repositories

- Use the GitHub connector to create, inspect, and merge pull requests.
- Do not use `gh` unless the connector is unavailable and the user explicitly
  approves the fallback.
- Pin the expected head commit when merging.
- Delete the remote source branch after a successful merge.

### Azure DevOps repositories

- Use the Azure DevOps REST API directly with cached Git Credential Manager
  credentials.
- Do not use Azure CLI.
- Avoid exploratory CLI attempts unless the REST API fails.
- Retrieve credentials once per PR flow and never print usernames, passwords,
  PATs, or authorization headers.
- Immediately complete the PR with `deleteSourceBranch=true`.
- If Azure DevOps returns `queued`, poll every 2 seconds for up to 60 seconds
  before reporting a policy or merge delay.

## Shell and tool fallbacks

- Use PowerShell-native syntax for command sequencing; do not use `&&`.
- Do not try `apply_patch` since it is blocked by policy.


## Code Style and Formatting

- **MUST** use meaningful, descriptive variable, function, and class names.
- **MUST** follow PEP 8 style guidelines.
- **MUST** use 4 spaces for indentation. Never use tabs.
- Limit line length to 88 characters (Ruff formatter standard).
- Use `snake_case` for functions and variables.
- Use `PascalCase` for classes.
- Use `UPPER_CASE` for constants.
- Use English in all code, comments, docstrings, logs, and documentation.
- **NEVER** use emoji or Unicode characters that emulate emoji (for example `✓` or `✗`). The only exception is tests that intentionally verify multibyte character handling.

## Comments and Documentation

- Prefer clear docstrings over redundant inline comments.
- **MUST** avoid comments that simply restate what the code already makes obvious.
- Remove comments that are tautological or self-documenting.
- **MUST NOT** include comments that:
  - Reveal the original user prompt.
  - Expose agent instructions or implementation guidance.
  - Describe what the file contains when it is already obvious.
  - Leak internal reasoning or generation context.

## Code Organization

- Favor readability and maintainability over clever or overly compact code.
- Do not extract a helper function solely to reduce the length of another function.
- Prefer inline code when the logic is:
  - Short
  - Readable
  - Used only once
- Create helper functions when they:
  - Eliminate meaningful duplication.
  - Represent a real domain concept.
  - Isolate complex path, file system, or storage behavior.
  - Follow an existing local pattern in the codebase.
- Avoid layers of small wrapper functions that make control flow harder to follow.
