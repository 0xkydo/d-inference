// Compiled alongside the repository's pure policy files. No MLX/model loading.
import Foundation

struct Input: Decodable {
    let physicalBytes: UInt64
    let reserveBytes: UInt64
    let models: [Model]
    struct Model: Decodable { let id: String; let bytes: UInt64 }
}

struct Estimate: Encodable {
    let id: String
    let requiredGiB: Double
    let availableGiB: Double
    let fits: Bool
}

@main struct PreviewEstimate {
    static func main() throws {
        let input = try JSONDecoder().decode(Input.self, from: FileHandle.standardInput.readDataToEndOfFile())
        let reserve = UnifiedMemoryCap.loadReserveBytes(
            physicalBytes: input.physicalBytes, configReserveBytes: input.reserveBytes)
        // Conservative open-world floor covers mixed selections without making
        // the group membership jump whenever a checkbox is toggled.
        let headroom = ModelLoadAdmission.defaultLoadHeadroomGb
        // Onboarding estimates machine capacity, independent of running apps.
        let budgetBytes = input.physicalBytes > reserve ? input.physicalBytes - reserve : 0
        let available = Double(budgetBytes) / 1_073_741_824
        let result = input.models.map { model in
            // Same admit-time disk × 1.2 policy as ModelScanner. Catalog total
            // includes small ancillary files, making this slightly conservative.
            let required = ModelLoadAdmission.requiredToLoadGb(
                weightsGb: Double(model.bytes) / 1_073_741_824 * 1.2, headroomGb: headroom)
            return Estimate(id: model.id, requiredGiB: required, availableGiB: available,
                            fits: required <= available)
        }
        FileHandle.standardOutput.write(try JSONEncoder().encode(result))
    }
}
