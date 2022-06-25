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

#if compiler(>=5.5) && canImport(_Concurrency)

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
extension S3FileTransferManager {
    func shutdown() async throws {
        if case .createNew = self.threadPoolProvider {
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Swift.Error>) in
                threadPool.shutdownGracefully { error in
                    if let error = error {
                        cont.resume(throwing: error)
                    } else {
                        cont.resume(returning: ())
                    }
                }
            }
        }
    }

    /// Copy from local file, to S3 file
    /// - Parameters:
    ///   - from: local filename
    ///   - to: S3 file
    ///   - progress: progress function, updated with value from 0 to 1 based on how much we have uploaded
    public func copy(
        from: String,
        to: S3File,
        options: PutOptions = .init(),
        progress: @escaping (Double) throws -> Void = { _ in }
    ) async throws {
        return try await self.copy(from: from, to: to, options: options, progress: progress).get()
    }

    /// Copy from S3 file, to local file
    /// - Parameters:
    ///   - from: S3 file
    ///   - to: local filename
    ///   - progress: progress function, updated with value from 0 to 1 based on how much we have uploaded
    public func copy(
        from: S3File,
        to: String,
        options: GetOptions = .init(),
        progress: @escaping (Double) throws -> Void = { _ in }
    ) async throws {
        return try await self.copy(from: from, to: to, options: options, progress: progress).get()
    }

    /// Copy from S3 file, to S3 file
    /// - Parameters:
    ///   - from: source S3 file
    ///   - to: destination S3 file
    ///   - fileSize: Size of file to copy. If you don't provide this then function will call `headObject` to ascertain this
    ///   - options: Options while copying
    public func copy(
        from: S3File,
        to: S3File,
        fileSize: Int? = nil,
        options: CopyOptions = .init()
    ) async throws {
        return try await self.copy(from: from, to: to, fileSize: fileSize, options: options).get()
    }

    /// Copy from local folder, to S3 folder
    /// - Parameters:
    ///   - from: local folder
    ///   - to: Path to S3 folder
    public func copy(
        from folder: String,
        to s3Folder: S3Folder,
        options: PutOptions = .init(),
        progress: @escaping (Double) throws -> Void = { _ in }
    ) async throws {
        return try await self.copy(from: folder, to: s3Folder, options: options, progress: progress).get()
    }

    /// Copy from S3 folder, to local folder
    /// - Parameters:
    ///   - from: Path to S3 folder
    ///   - to: Local folder
    public func copy(
        from s3Folder: S3Folder,
        to folder: String,
        options: GetOptions = .init(),
        progress: @escaping (Double) throws -> Void = { _ in }
    ) async throws {
        return try await self.copy(from: s3Folder, to: folder, options: options, progress: progress).get()
    }

    /// Copy from S3 folder, to S3 folder
    /// - Parameters:
    ///   - from: Path to source S3 folder
    ///   - to: Path to destination S3 folder
    public func copy(
        from srcFolder: S3Folder,
        to destFolder: S3Folder,
        options: CopyOptions = .init()
    ) async throws {
        return try await self.copy(from: srcFolder, to: destFolder, options: options).get()
    }

    /// Sync from local folder, to S3 folder.
    ///
    /// Copies files across unless the file already exists in S3 folder, or file in S3 is newer. Added flag to
    /// delete files on S3 that don't exist locally
    ///
    /// - Parameters:
    ///   - from: Local folder
    ///   - to: Path to destination S3 folder
    ///   - delete: Should we delete files on S3 that don't exists locally
    public func sync(
        from folder: String,
        to s3Folder: S3Folder,
        delete: Bool,
        options: PutOptions = .init(),
        progress: @escaping (Double) throws -> Void = { _ in }
    ) async throws {
        return try await self.sync(from: folder, to: s3Folder, delete: delete, options: options, progress: progress).get()
    }

    /// Sync from S3 folder, to local folder.
    ///
    /// Download files from S3 unless the file already exists in local folder, or local file is newer. Added flag to
    /// delete files locally that don't exist in S3.
    ///
    /// - Parameters:
    ///   - from: Path to source S3 folder
    ///   - to: Local folder
    ///   - delete: Should we delete files locally that don't exists in S3
    public func sync(
        from s3Folder: S3Folder,
        to folder: String,
        delete: Bool,
        options: GetOptions = .init(),
        progress: @escaping (Double) throws -> Void = { _ in }
    ) async throws {
        return try await self.sync(from: s3Folder, to: folder, delete: delete, options: options, progress: progress).get()
    }

    /// Sync from S3 folder, to another S3 folder.
    ///
    /// Copy files from S3 folder unless the file already exists in destination folder, or destination file is newer. Added flag to
    /// delete files from destination folder that don't exist in source folder.
    ///
    /// - Parameters:
    ///   - from: Path to source S3 folder
    ///   - to: Local folder
    ///   - delete: Should we delete files locally that don't exists in S3
    public func sync(
        from srcFolder: S3Folder,
        to destFolder: S3Folder,
        delete: Bool,
        options: CopyOptions = .init()
    ) async throws {
        return try await self.sync(from: srcFolder, to: destFolder, delete: delete, options: options).get()
    }

    /// delete a file on S3
    public func delete(_ file: S3File) async throws {
        try await self.delete(file).get()
    }

    /// delete a fodler on S3
    public func delete(_ file: S3Folder) async throws {
        try await self.delete(file).get()
    }
}

#endif // #if compiler(>=5.5) && canImport(_Concurrency)
