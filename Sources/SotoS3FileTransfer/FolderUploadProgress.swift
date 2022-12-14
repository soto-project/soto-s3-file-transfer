//===----------------------------------------------------------------------===//
//
// This source file is part of the Soto for AWS open source project
//
// Copyright (c) 2020-2021 the Soto project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Soto project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import NIO
import NIOConcurrencyHelpers

extension S3FileTransferManager {
    class FolderUploadProgress {
        let lock: NIOLock
        let totalSize: UInt64
        let sizes: [String: UInt64]
        var uploadedSize: UInt64
        var currentUploadingSizes: [String: UInt64]
        var progressFunc: (Double) throws -> Void = { _ in }

        init(_ s3Files: [S3FileDescriptor], progress: @escaping (Double) throws -> Void = { _ in }) {
            self.lock = .init()
            self.sizes = .init(s3Files.map { (key: $0.file.key, value: UInt64($0.size)) }) { first, _ in first }
            self.totalSize = self.sizes.values.reduce(UInt64(0), +)
            self.uploadedSize = 0
            self.currentUploadingSizes = [:]
            self.progressFunc = progress
        }

        init(_ files: [FileDescriptor], progress: @escaping (Double) throws -> Void = { _ in }) {
            self.lock = .init()
            self.sizes = .init(files.map { (key: $0.name, value: UInt64($0.size)) }) { first, _ in first }
            self.totalSize = self.sizes.values.reduce(UInt64(0), +)
            self.uploadedSize = 0
            self.currentUploadingSizes = [:]
            self.progressFunc = progress
        }

        func updateProgress(_ file: String, progress: Double) throws {
            try self.lock.withLock {
                currentUploadingSizes[file] = sizes[file].map { UInt64(Double($0) * progress) } ?? 0
                try progressFunc(self.progress)
            }
        }

        func setFileUploaded(_ file: String) {
            self.lock.withLock {
                currentUploadingSizes[file] = nil
                uploadedSize += sizes[file] ?? 0
            }
        }

        var finished: Bool { self.totalSize == self.uploadedSize && self.currentUploadingSizes.count == 0 }

        var progress: Double {
            let progress = self.uploadedSize + self.currentUploadingSizes.values.reduce(UInt64(0), +)
            return Double(progress) / Double(self.totalSize)
        }
    }
}
