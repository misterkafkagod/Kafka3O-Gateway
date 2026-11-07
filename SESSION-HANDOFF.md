# Session continuation handoff

Snapshot: 2026-11-06, approximately 23:02 UTC-03. The user has **paused
implementation** and requested this handoff. Do not resume implementation or
change the deployment until the user asks to continue. This is a snapshot, not
the source of truth for requirements or current Git and deployment state.

## Project and workflow

The solution spans two sibling Windows workspaces:

- Gateway and Console: `D:\sources\Kafka3O-Gateway`.
- UI and shared specifications: `D:\sources\Kafka3O-UI`.
- User-requested phase workflows: `D:\sources\my-skills`.

On resumption, read the current [roadmap](../Kafka3O-UI/spec/ROADMAP.md),
[functional spec](../Kafka3O-UI/spec/FUNC-SPEC.md),
[technical spec](../Kafka3O-UI/spec/TECH-SPEC.md), and
[P53 plan](../Kafka3O-UI/spec/phases/53-cognito-sign-in.md). The specs and
phase checklists take precedence over this historical snapshot. Local links
to the UI and workflows assume the workspaces are siblings.

The user requested the roadmap, breakdown, discovery, plan, build, and review
workflow in a loop, including after context compaction; routine steps are
auto-approved. See the [workflow README](../my-skills/README.md). Six
workspace skill definitions were placed in the Gateway's untracked
`.github\skills\` for the chat interface. Preserve them; they are not in the
tracked repository. If they are not registered in a future session, read the
corresponding workflow files rather than claiming they were invoked.
Auto-approval does **not** resolve significant security, behavior, or scope
choices; ask the user when one is needed.

The user also requested task-scoped commits and synchronization. Fetch and
check divergence before pushing; never force-push or overwrite unrelated
work. Push Gateway changes before UI/spec changes that depend on them.

## Exact stopping point

M10 is active, P53 is building, and T53.4s is the latest completed
implementation slice. T53.4 and T53.5 remain open; P54-P59 remain ahead.
The [P53 plan](../Kafka3O-UI/spec/phases/53-cognito-sign-in.md) is the
authoritative task list. The local-authentication preview and Cognito
foundations are **not** a secured Kafka deployment. No Cognito browser login
route or stable-marker cookie setter is exposed.

Before this handoff, the last Gateway implementation/documentation commit was
`9d47f00` and the matching UI/spec commit was `b7baf74`. Both repositories
were on `feat/partial` and matched their fetched `origin/feat/partial`.
Check again: the Gateway also had untracked `.github\` and
`configs\dev.yaml`; the UI worktree was clean. Preserve those untracked files
and stage only the intended task files.

### Browser decision still open

The user was offered a choice about compatibility when SSO is enabled:

1. Require modern browser coordination and fail closed when it is unavailable
   (the recommendation).
2. Research broader browser support before enabling SSO.
3. Defer SSO and continue local authentication and authorization, with an
   appropriate plan adjustment.

The user answered by pausing the work, **not by selecting an option**. When
implementation resumes and the decision is needed, ask again rather than
treating the recommendation or general auto-approval as consent. Provider
outages must not prevent local login in supported browsers.

The immediate technical blocker is safe first-time stable-marker
initialization and its lifecycle across tabs. Two initial requests with no
shared browser identifier cannot simply be assumed to be from one browser.
An earlier response can also arrive late and overwrite a newer HttpOnly
cookie. Web Locks and a same-origin callback landing/fetch were identified
only as possible research avenues, not an approved or proven solution; Web
Locks alone are not proof that delayed responses are safe. The
[technical spec](../Kafka3O-UI/spec/TECH-SPEC.md) records the limited
synthetic Chromium observations and existing primitives.

## Recommended continuation

1. Recheck both worktrees, upstream divergence, the P53 checklist, and the
   user's browser-policy decision. If the policy tradeoff is still necessary,
   obtain the decision before committing to a design.
2. Design and verify a fail-closed marker protocol for concurrent first use,
   local login, SSO callback, logout, cancellation, tab closure, delayed
   responses, restarts, storage loss, and uncertain outcomes. Verify real
   browser behavior across the supported browsers, including real redirects
   through the synthetic HTTPS provider and network interruptions. Do not
   extrapolate the earlier intercepted Chromium probe into a safety proof.
3. Only after the browser protocol is proven, complete the opt-in HTTPS
   authentication-only P53 routes and synthetic end-to-end fixture as
   specified. Keep any fixture or preview isolated, without Kafka routes; do
   not present it as a secured deployment. Real Cognito User Pool sign-in,
   refresh, and logout remain unverified until a real pool is available.
4. Continue P54-P59 in roadmap order: access administration and
   classification, Console-to-Gateway delegation and enforcement,
   Production confirmations, audit, and secure end-to-end deployment.
   The Gateway must enforce current policy independently; the existing
   shared-token and manual-proxy paths must not bypass policy in secured
   mode. Do not mark M10 complete based on standalone foundations.
5. Follow each phase's exact verification and review requirements, preserve
   prior behavior, document unverified cases, commit one completed task at a
   time, and synchronize. If a necessary plan change is not routine, get
   the user's decision rather than silently weakening a security invariant.

Never put credentials, provider/session/CSRF tokens, callback parameters,
browser markers, encryption keys, issuer, subject, audience, or verified
group snapshots in logs, the handoff, or commits.

## Workstation and manual cluster

Go `1.27.3` was available. The default Node was `22.21.0`, below the UI's
declared minimum of `24.21.0`. A verified Node `24.21.0` and `npm.cmd` were
present under
`%USERPROFILE%\.copilot\session-state\b44e3c70-1105-4a50-a153-90c37562be35\files\node-v24.21.0-win-x64`.
That session path may disappear: verify it and the active `node --version`
before UI validation, and put the verified Node directory first on the
current PowerShell process's `PATH` when using it.

The test environment is self-hosted **EKS-D**, not AWS EKS. Work only on
`eksd-01` among the user's VMs. The existing manual UI at
`http://localhost:5173` returned HTTP 200 at handoff; that alone does not
prove that its Gateways or brokers are healthy. Recheck rather than assuming
the tunnel or workloads are still running. See the
[manual test guide](deploy/kubernetes/manual-test/README.md) and
[persistent Kafka guide](deploy/kubernetes/test-kafka/README.md) for access
and operational details.

The original cluster handoff reported no StorageClass, ingress, load
balancer, metrics server, or backups; check the current cluster before
planning deployment. It also reported that `pause:3.10.2` is a local retag of
the EKS-D pause image while containerd is pinned to the EKS-D tag. Recheck
that arrangement before any cluster upgrade.

Preserve the manual deployment and both persistent Kafka volumes. Do not run
destructive disposable-fixture tests against those brokers or perform
unapproved live mutation tests; successful live writes and multi-broker
placement were not verified. Do not change browser trust stores or disable
TLS verification. The manual proxy is a test convenience, not user
authentication. Before **any** firewall, SSH, or network change, arm an
automatic rollback timer and cancel it only after a fresh independent login
has been verified. Do not print or store kubeconfigs, join tokens, or other
secrets.
