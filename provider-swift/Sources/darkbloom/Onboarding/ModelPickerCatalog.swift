// Start interactive catalog picker: build picker entries from the catalog,
// budget/fit + rollout filtering, fallback selection, and the prompt flow.
import Foundation
import ArgumentParser
import ProviderCore
#if canImport(Darwin)
import Darwin
#endif

extension Start {
    // MARK: - Interactive Catalog Picker

    private static let gemmaPublicID = "gemma-4-26b"
    private static let gemmaQATID = "gemma-4-26b-qat-4bit"
    private static let gemmaRollbackID = "gemma-4-26b-8bit"

    /// Apply the shared runtime-requirements gate before resolving public aliases.
    /// The coordinator's `primary_build` is derived for the unfiltered catalog, so
    /// it must not select a protected build that this provider cannot run.
    static func evaluateEligiblePickerCatalog(
        models: [CatalogModel],
        aliases: [CatalogAlias],
        runtimeCapabilities: Set<ProviderRuntimeCapability>,
        includeIneligible: Bool = false
    ) -> EligiblePickerCatalog {
        let activeModels = models.filter { $0.active != false }
        let eligibleModels = activeModels.filter {
            ModelRuntimeRequirements.isEligible(
                modelID: $0.id,
                catalogRequirements: $0.requiredProviderCapabilities,
                available: runtimeCapabilities)
        }
        let eligibleConcreteIDs = Set(eligibleModels.map(\.id))
        var aliasDisplayByBuildID: [String: String] = [:]
        var hiddenBuildIDs = Set<String>()

        for alias in aliases {
            hiddenBuildIDs.insert(alias.desiredBuild)
            if let previous = alias.previousBuild {
                hiddenBuildIDs.insert(previous)
            }
            for retired in alias.retiredBuilds ?? [] {
                hiddenBuildIDs.insert(retired)
            }

            if eligibleConcreteIDs.contains(alias.desiredBuild) {
                aliasDisplayByBuildID[alias.desiredBuild] = alias.displayName
            } else if let previous = alias.previousBuild,
                eligibleConcreteIDs.contains(previous)
            {
                aliasDisplayByBuildID[previous] = alias.displayName
            } else if includeIneligible, activeModels.contains(where: { $0.id == alias.desiredBuild }) {
                aliasDisplayByBuildID[alias.desiredBuild] = alias.displayName
            }
        }

        return EligiblePickerCatalog(
            models: includeIneligible ? activeModels : eligibleModels,
            aliasDisplayByBuildID: aliasDisplayByBuildID,
            hiddenBuildIDs: hiddenBuildIDs,
            sourceHasAliases: !aliases.isEmpty)
    }

    static func pickerCatalogRows(catalog: EligiblePickerCatalog) -> [PickerCatalogRow] {
        if !catalog.sourceHasAliases {
            let gemmaQATAvailable = catalog.models.contains { $0.id == Self.gemmaQATID }
            return catalog.models.compactMap { model in
                if shouldHideGemmaRolloutModel(model, qatAvailable: gemmaQATAvailable)
                    || isHiddenPickerModel(model)
                {
                    return nil
                }
                return PickerCatalogRow(
                    model: model,
                    displayName: gemmaRolloutDisplayName(for: model) ?? model.displayName)
            }
        }

        let aliasDisplayByBuild = catalog.aliasDisplayByBuildID
        return catalog.models.compactMap { model in
            if let displayName = aliasDisplayByBuild[model.id] {
                return PickerCatalogRow(model: model, displayName: displayName)
            }
            if catalog.hiddenBuildIDs.contains(model.id) || isHiddenPickerModel(model) {
                return nil
            }
            return PickerCatalogRow(model: model, displayName: model.displayName)
        }
    }

    private static func isHiddenPickerModel(_ model: CatalogModel) -> Bool {
        if let metadata = model.metadata {
            if metadata["hidden_from_picker"] == .bool(true) { return true }
            if metadata["hide_standalone"] == .bool(true) { return true }
        }
        return model.displayName.localizedCaseInsensitiveContains("rollback")
    }

    private static func gemmaRolloutDisplayName(for model: CatalogModel) -> String? {
        // Temporary Gemma 4 rollout shim. Remove after the coordinator alias
        // catalog contract is deployed and the picker consumes alias metadata.
        model.id == Self.gemmaQATID ? "Gemma 4 26B" : nil
    }

    private static func shouldHideGemmaRolloutModel(_ model: CatalogModel, qatAvailable: Bool) -> Bool {
        guard qatAvailable else { return model.id == Self.gemmaRollbackID }
        return model.id == Self.gemmaPublicID || model.id == Self.gemmaRollbackID
    }

}
