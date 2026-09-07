# Model-fit history and design scenarios

Reviewed 2026-09-06 against local checkout `bbf6f83d4` and GitHub PR descriptions.
This captures the state before the onboarding implementation. It is design
research, not a new admission policy or a live-machine measurement. Current
picker behavior lives in `Sources/darkbloom/Onboarding/ModelPickerPolicy.swift`
under `provider-swift/`.

## Relevant changes

- [#273](https://github.com/Layr-Labs/d-inference/pull/273) separated model-load
  admission from full-concurrency request capacity. The old gate compounded
  weight and free-memory discounts. Its historical 2 GB load-headroom constant
  and RAM-tier examples are superseded by later policy.
- [#363](https://github.com/Layr-Labs/d-inference/pull/363) established the unified
  cap: weights, working memory, and KV share 90% of physical memory, with at least
  2 GiB left for the OS. Loading and post-load serviceability use the same policy.
- [#390](https://github.com/Layr-Labs/d-inference/pull/390) made the provider report
  cold-load capacity using actual available memory and reclaimable idle models.
  Physical RAM alone does not say whether a model can load now.
- [#590](https://github.com/Layr-Labs/d-inference/pull/590) raised the default
  activation reserve from 3 to 5.5 GiB and repaired a two-model co-residency issue.
  [#600](https://github.com/Layr-Labs/d-inference/pull/600) subsequently reverted
  the default KV backend/concurrency changes. Do not treat #590's backend default
  as current policy or infer that the reserve was reverted with it.
- [#791](https://github.com/Layr-Labs/d-inference/pull/791) introduced measured
  activation floors across the serving set. Only `gpt-oss-20b` currently has a
  3.5 GiB floor; unmeasured models use 5.5 GiB. The maximum across advertised,
  resident, and loading models governs the provider reserve. Merely enabling a
  higher-floor model can reduce another model's serving headroom.
  Admit-time weights retain disk × 1.2 for load transients. Measured steady
  residency is used only for post-load cold token-budget estimates.
  The PR explicitly did not fully solve the 24 GB GPT-OSS tier.
  Eviction feasibility now prevents discarding usable models for a new model
  that cannot fit even after eviction.

## Current UI mismatch

`StartCommand+Picker.swift` uses `sizeGb <= memoryGb - 4`. Available entries use
catalog size; downloaded entries can use scanner estimates (disk × 1.2).
`StartCommand+TUIPicker.swift` sums those sizes and announces either simultaneous
serving or one active model at a time. Neither is a reliable runtime verdict.

Runtime policy lives in `UnifiedMemoryCap.swift` and `ModelLoadAdmission.swift`.
It includes OS/operator reserves, real available memory, padded weights, the
serving-set activation reserve, at least 1 GiB of KV headroom, and outstanding
reservations. `ProviderLoop+ModelLoading.swift` checks admission again near
allocation and verifies post-load headroom. The default model-slot limit is 3;
selection count and co-resident slot count are separate concepts.

## Runtime context, not model-selection scope

Scope correction after reviewing the provider quickstart, model-registry,
hardware-support, CLI reference, and current picker/daemon code: the cases below
document downstream behavior. They are not a checklist of scenarios to implement
inside model selection. The bounded selection scope is in
[`model-selection-scope.md`](model-selection-scope.md).

| Scenario | What the customer should learn |
|---|---|
| Fresh Mac, one suitable model | Download size and estimated memory to run are different. |
| Existing downloads | Already downloaded does not mean currently loaded or runnable. |
| Multiple models can remain loaded | Selection is supported; request capacity remains dynamic. |
| Models fit individually, not together | They can load on demand; do not promise exactly one active model. |
| Suitable hardware, too little available memory now | This is a temporary capacity issue, distinct from unsupported hardware. |
| Model cannot fit even after idle-model eviction | Preserve working models and explain why the new one cannot load. |
| Adding a model increases the shared reserve | Re-evaluate the whole selection, not only the newly checked row. |
| Post-load headroom is insufficient | Download completion is not serving readiness; allow selection changes. |
| Selected model needs a newer provider capability | Distinguish compatibility from a memory failure. |

Proposed labels: **Downloaded**, **Available to download**, **Estimated memory to
run**, **Not enough memory available now**, **Loads on demand**, and **Requires a
newer provider**. Show details on demand. Any definitive fit verdict must come
from the shared runtime policy and a fresh memory snapshot, not a new Python
formula in the mock. Until then, label memory verdicts as illustrative fixtures.
