import Foundation
import Testing
@testable import ProviderCore

private final class StorageDownloadProtocol: URLProtocol, @unchecked Sendable {
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        // Stop at manifest fetch. No model files or staging directories are made.
        let response = HTTPURLResponse(url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}

@Suite("Explicit model storage")
struct ModelStorageDownloadTests {
    @Test("only explicit storage can reach download IO for an unsupported model")
    func storageDoesNotRelaxNormalDownloads() async throws {
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [StorageDownloadProtocol.self]
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }
        let downloader = ModelDownloader(r2CDNURL: "https://storage.test", urlSession: session)
        let model = CatalogModel(id: "storage-test", s3Name: "storage-test", displayName: "Storage test",
                                 sizeGb: 1, r2Prefix: "v2/test/v1", aggregateSHA256: "test",
                                 requiredProviderCapabilities: [.appleM5])
        do {
            try await downloader.download(model: model)
            Issue.record("normal download must reject unsupported hardware")
        } catch let error as ModelCatalogError {
            guard case .ineligible = error else { Issue.record("unexpected error: \(error)"); return }
        }
        do {
            try await downloader.downloadForStorage(model: model)
            Issue.record("missing manifest must still fail")
        } catch let error as ModelCatalogError {
            if case .ineligible = error { Issue.record("explicit storage must reach manifest IO") }
        }
        // Storage intent does not change the serving eligibility evaluator.
        #expect(!ModelRuntimeRequirements.isEligible(modelID: model.id,
                catalogRequirements: model.requiredProviderCapabilities, available: []))
    }
}
