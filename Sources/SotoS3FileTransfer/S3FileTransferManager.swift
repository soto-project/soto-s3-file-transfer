//===----------------------------------------------------------------------===//
//
// This source file is part of the Soto for AWS open source project
//
// Copyright (c) 2020-2022 the Soto project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Soto project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import Atomics
import Foundation
import Logging
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import SotoS3

/// S3 Transfer manager. Transfers files/folders back and forth between S3 and your local file system
public class S3FileTransferManager {
    /// Configuration for S3 Transfer Manager
    public struct Configuration {
        /// Cancel operations as soon as an error is found. Otherwise the operation will be attempt to finish all transfers
        let cancelOnError: Bool
        /// size file has to be before using multipart upload
        let multipartThreshold: Int
        /// size of each multipart part upload
        let multipartPartSize: Int
        /// maximum number of uploads/downloads running concurrently
        let maxConcurrentTasks: Int

        public init(
            cancelOnError: Bool = true,
            multipartThreshold: Int = 8 * 1024 * 1024,
            multipartPartSize: Int = 8 * 1024 * 1024,
            maxConcurrentTasks: Int = 4
        ) {
            precondition(multipartThreshold >= 5 * 1024 * 1024, "Multipart upload threshold is required to be greater than 5MB")
            precondition(multipartThreshold >= multipartPartSize, "Multipart upload threshold is required to be greater than or equal to the multipart part size")
            self.cancelOnError = cancelOnError
            self.multipartThreshold = multipartThreshold
            self.multipartPartSize = multipartPartSize
            self.maxConcurrentTasks = maxConcurrentTasks
        }
    }

    /// List of file downloads to perform. This is return in a downloadFailed
    /// error and can be passed to the resume function to resume the download
    public struct DownloadOperation {
        let transfers: [(from: S3FileDescriptor, to: String)]
    }

    /// List of file uploads to perform. This is return in a uploadFailed
    /// error and can be passed to the resume function to resume the download
    public struct UploadOperation {
        let transfers: [(from: FileDescriptor, to: S3File)]
    }

    /// List of file copies to perform. This is return in a copyFailed
    /// error and can be passed to the resume function to resume the download
    public struct CopyOperation {
        let transfers: [(from: S3FileDescriptor, to: S3File)]
    }

    /// Errors created by S3TransferManager
    public enum Error: Swift.Error {
        /// File you referenced doesn't exist
        case fileDoesNotExist(String)
        /// Failed to create a folder during a download
        case failedToCreateFolder(String)
        /// Failed to enumerate the contents of a folder
        case failedToEnumerateFolder(String)
        /// Cannot download file from S3 as it is a folder on your local file system
        case fileFolderClash(String, String)
        /// download failed
        case downloadFailed(Swift.Error, DownloadOperation)
        /// upload failed
        case uploadFailed(Swift.Error, UploadOperation)
        /// copy failed
        case copyFailed(Swift.Error, CopyOperation)
    }

    /// S3 service object
    public let s3: S3
    /// Thread pool used by transfer manager
    public let threadPool: NIOThreadPool
    /// File IO manager
    public let fileIO: NonBlockingFileIO
    /// Configuration
    public let configuration: Configuration
    /// Logger
    public let logger: Logger
    /// Have we shutdown the Manager
    internal let isShutdown = ManagedAtomic(false) // <Bool>.makeAtomic(value: false)

    /// Initialize S3 Transfer manager.
    /// - Parameters:
    ///   - s3: S3 service object from Soto
    ///   - threadPoolProvider: Thread pool provider for file operations, Either create a new pool, or supply you have already
    ///   - configuration: transfer manager configuration
    ///   - logger: Logger
    public init(
        s3: S3,
        threadPoolProvider: S3.ThreadPoolProvider,
        configuration: Configuration = Configuration(),
        logger: Logger = AWSClient.loggingDisabled
    ) {
        self.s3 = s3
        self.threadPool = threadPoolProvider.syncThreadPool
        self.fileIO = NonBlockingFileIO(threadPool: self.threadPool)
        self.configuration = configuration
        self.logger = logger
    }

    /// Copy from local file, to S3 file
    /// - Parameters:
    ///   - from: local filename
    ///   - to: S3 file
    ///   - progress: progress function, updated with value from 0 to 1 based on how much we have uploaded
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func copy(
        from: String,
        to: S3File,
        options: PutOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        self.logger.debug("Copy from: \(from) to \(to)")
        let eventLoop = self.s3.eventLoopGroup.next()
        let fileSize: Int
        do {
            let attributes = try await self.threadPool.runIfActive(eventLoop: eventLoop) {
                try FileManager.default.attributesOfItem(atPath: from)
            }.get()
            fileSize = attributes[.size] as? Int ?? 0
        } catch {
            throw Error.fileDoesNotExist(String(describing: from))
        }
        // if file size is greater than multipart threshold then use multipart upload for uploading the file
        if fileSize > self.configuration.multipartThreshold {
            let request = S3.CreateMultipartUploadRequest(bucket: to.bucket, key: to.key, options: options)
            _ = try await self.s3.multipartUpload(
                request,
                partSize: self.configuration.multipartPartSize,
                filename: from,
                abortOnFail: true,
                threadPoolProvider: .shared(self.threadPool),
                logger: self.logger,
                progress: progress
            )
            try? await progress(1.0)
        } else {
            let fileIO = NonBlockingFileIO(threadPool: self.threadPool)
            let (fileHandle, fileRegion) = try await fileIO.openFile(path: from, eventLoop: eventLoop).get()
            defer {
                try? fileHandle.close()
            }

            let body: AWSHTTPBody = .init(
                asyncSequence: FileByteBufferAsyncSequence(
                    fileHandle,
                    fileIO: fileIO,
                    chunkSize: 64 * 1024,
                    byteBufferAllocator: self.s3.config.byteBufferAllocator,
                    eventLoop: eventLoop
                ),
                length: fileRegion.readableBytes
            ) /* .fileHandle(fileHandle, offset: 0, size: fileSize, fileIO: self.fileIO) { downloaded in
                 try progress(Double(downloaded) / Double(fileSize))
             }*/
            let request = S3.PutObjectRequest(body: body, bucket: to.bucket, key: to.key, options: options)
            _ = try await self.s3.putObject(request, logger: self.logger)
            try? await progress(1.0)
        }
    }

    /// Copy from S3 file, to local file
    /// - Parameters:
    ///   - from: S3 file
    ///   - to: local filename
    ///   - progress: progress function, updated with value from 0 to 1 based on how much we have uploaded
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func copy(
        from: S3File,
        to: String,
        options: GetOptions = .init(),
        progress: @escaping (Double) async throws -> Void = { _ in }
    ) async throws {
        self.logger.debug("Copy from: \(from) to \(to)")
        let eventLoop = self.s3.eventLoopGroup.next()

        // check for existence of file
        do {
            _ = try await self.s3.headObject(.init(bucket: from.bucket, key: from.key), logger: self.logger)
        } catch {
            if let error = error as? S3ErrorType, error == .notFound {
                throw Error.fileDoesNotExist(String(describing: from))
            }
            throw error
        }
        let filename = try await self.threadPool.runIfActive(eventLoop: eventLoop) { () -> String in
            var to = to
            // if `to` is a folder append name of object to end of `to` string to place object in folder `to`.
            var isDirectory: ObjCBool = false
            if FileManager.default.fileExists(atPath: to, isDirectory: &isDirectory), isDirectory.boolValue == true {
                if var lastSlash = from.key.lastIndex(of: "/") {
                    lastSlash = from.key.index(after: lastSlash)
                    to = "\(to)/\(from.key[lastSlash...])"
                } else {
                    to = "\(to)/\(from.key)"
                }
            } else {
                // create folder to place file in, if it doesn't exist already
                let folder: String
                var isDirectory: ObjCBool = false
                if let lastSlash = to.lastIndex(of: "/") {
                    folder = String(to[..<lastSlash])
                } else {
                    folder = to
                }
                if FileManager.default.fileExists(atPath: folder, isDirectory: &isDirectory) {
                    guard isDirectory.boolValue else { throw Error.failedToCreateFolder(folder) }
                } else {
                    try FileManager.default.createDirectory(atPath: folder, withIntermediateDirectories: true)
                }
            }
            return to
        }.get()
        let fileHandle = try await self.fileIO.openFile(path: filename, mode: .write, flags: .allowFileCreation(), eventLoop: eventLoop).get()
        defer {
            try? fileHandle.close()
        }
        let request = S3.GetObjectRequest(bucket: from.bucket, key: from.key, options: options)
        let response = try await self.s3.getObject(request, logger: self.logger)
        var bytesDownloaded = 0
        let fileSize = response.contentLength ?? 1
        for try await buffer in response.body {
            bytesDownloaded += buffer.readableBytes
            try await self.fileIO.write(fileHandle: fileHandle, buffer: buffer, eventLoop: eventLoop).get()
            try await progress(Double(bytesDownloaded) / Double(fileSize))
        }
    }

    /// Copy from S3 file, to S3 file
    /// - Parameters:
    ///   - from: source S3 file
    ///   - to: destination S3 file
    ///   - fileSize: Size of file to copy. If you don't provide this then function will call `headObject` to ascertain this
    ///   - options: Options while copying
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func copy(
        from: S3File,
        to: S3File,
        fileSize: Int? = nil,
        options: CopyOptions = .init()
    ) async throws {
        self.logger.debug("Copy from: \(from) to \(to)")
        let copySource = "/\(from.bucket)/\(from.key)".addingPercentEncoding(withAllowedCharacters: Self.pathAllowedCharacters)!
        let request = S3.CopyObjectRequest(bucket: to.bucket, copySource: copySource, key: to.key, options: options)

        do {
            let calculatedFileSize: Int
            if let fileSize = fileSize {
                calculatedFileSize = fileSize
            } else {
                let headRequest = S3.HeadObjectRequest(bucket: from.bucket, key: from.key)
                calculatedFileSize = try await numericCast(self.s3.headObject(headRequest, logger: self.logger).contentLength ?? 1)
            }
            if calculatedFileSize > self.configuration.multipartThreshold {
                _ = try await self.s3.multipartCopy(request, partSize: self.configuration.multipartPartSize, logger: self.logger)
            } else {
                _ = try await self.s3.copyObject(request, logger: self.logger)
            }
        } catch {
            if let error = error as? S3ErrorType, error == .notFound {
                throw Error.fileDoesNotExist(String(describing: from))
            }
            throw error
        }
    }

    /// Copy from local folder, to S3 folder
    /// - Parameters:
    ///   - from: local folder
    ///   - to: Path to S3 folder
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func copy(
        from folder: String,
        to s3Folder: S3Folder,
        options: PutOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        let files = try await listFiles(in: folder)
        let folderResolved = URL(fileURLWithPath: folder).standardizedFileURL.resolvingSymlinksInPath()
        let transfers = Self.targetFiles(files: files, from: folderResolved.path, to: s3Folder)
        try await self.copy(transfers, options: options, progress: progress)
    }

    /// Copy from S3 folder, to local folder
    /// - Parameters:
    ///   - from: Path to S3 folder
    ///   - to: Local folder
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func copy(
        from s3Folder: S3Folder,
        to folder: String,
        options: GetOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        let files = try await listFiles(in: s3Folder)
        let validatedFiles = try self.validateFileList(files, ignoreClashes: options.ignoreFileFolderClashes)
        let transfers = Self.targetFiles(files: validatedFiles, from: s3Folder, to: folder)
        try await self.copy(transfers, options: options, progress: progress)
    }

    /// Copy from S3 folder, to S3 folder
    /// - Parameters:
    ///   - from: Path to source S3 folder
    ///   - to: Path to destination S3 folder
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func copy(
        from srcFolder: S3Folder,
        to destFolder: S3Folder,
        options: CopyOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        let files = try await listFiles(in: srcFolder)
        let transfers = Self.targetFiles(files: files, from: srcFolder, to: destFolder)
        try await self.copy(transfers, options: options, progress: progress)
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
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func sync(
        from folder: String,
        to s3Folder: S3Folder,
        delete: Bool,
        options: PutOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        let files = try await listFiles(in: folder)
        let s3Files = try await listFiles(in: s3Folder)
        let folderResolved = URL(fileURLWithPath: folder).standardizedFileURL.resolvingSymlinksInPath()
        let targetFiles = Self.targetFiles(files: files, from: folderResolved.path, to: s3Folder)
        let s3KeyMap = Dictionary(uniqueKeysWithValues: s3Files.map { ($0.file.key, $0) })
        let transfers = targetFiles.compactMap { transfer -> (from: FileDescriptor, to: S3File)? in
            // does file exist on S3
            guard let s3File = s3KeyMap[transfer.to.key] else { return transfer }
            // does file on S3 have a later date
            guard s3File.modificationDate > transfer.from.modificationDate else { return transfer }
            return nil
        }
        try await self.copy(transfers, options: options, progress: progress)
        // construct list of files to delete, if we are doing deletion
        if delete == true {
            let targetKeys = Set(targetFiles.map { $0.to.key })
            let deletions = s3Files.compactMap { s3File -> S3File? in
                if targetKeys.contains(s3File.file.key) {
                    return nil
                } else {
                    return s3File.file
                }
            }
            try await self.delete(deletions)
        }
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
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func sync(
        from s3Folder: S3Folder,
        to folder: String,
        delete: Bool,
        options: GetOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        let files = try await listFiles(in: folder)
        let s3Files = try await listFiles(in: s3Folder)
        let validatedS3Files = try self.validateFileList(s3Files, ignoreClashes: options.ignoreFileFolderClashes)
        let targetFiles = Self.targetFiles(files: validatedS3Files, from: s3Folder, to: folder)
        let fileNameMap = Dictionary(uniqueKeysWithValues: files.map { ($0.name, $0) })
        let transfers = targetFiles.compactMap { transfer -> (from: S3FileDescriptor, to: String)? in
            // does file exist locally
            guard let file = fileNameMap[transfer.to] else { return transfer }
            // does local file have a later date
            guard file.modificationDate > transfer.from.modificationDate else { return transfer }
            return nil
        }
        // construct list of files to delete, if we are doing deletion
        if delete == true {
            let targetTos = Set(targetFiles.map { $0.to })
            let deletions = files.compactMap { file -> String? in
                if targetTos.contains(file.name) {
                    return nil
                } else {
                    return file.name
                }
            }
            try await deletions.concurrentForEach(
                maxConcurrentTasks: self.configuration.maxConcurrentTasks,
                cancelOnError: self.configuration.cancelOnError
            ) {
                try await self.delete($0)
            }
        }
        try await self.copy(
            transfers,
            options: options,
            progress: progress
        )
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
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func sync(
        from srcFolder: S3Folder,
        to destFolder: S3Folder,
        delete: Bool,
        options: CopyOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        let srcFiles = try await listFiles(in: srcFolder)
        let destFiles = try await listFiles(in: destFolder)
        let targetFiles = Self.targetFiles(files: srcFiles, from: srcFolder, to: destFolder)
        let destKeyMap = Dictionary(uniqueKeysWithValues: destFiles.map { ($0.file.key, $0) })
        let transfers = targetFiles.compactMap { transfer -> (from: S3FileDescriptor, to: S3File)? in
            // does file exist in destination folder
            guard let file = destKeyMap[transfer.to.key] else { return transfer }
            // does local file have a later date
            guard file.modificationDate > transfer.from.modificationDate else { return transfer }
            return nil
        }
        try await self.copy(
            transfers,
            options: options,
            progress: progress
        )
        // construct list of files to delete, if we are doing deletion
        if delete == true {
            let targetKeys = Set(targetFiles.map { $0.to.key })
            let deletions = destFiles.compactMap { file -> S3File? in
                if targetKeys.contains(file.file.key) {
                    return nil
                } else {
                    return file.file
                }
            }
            try await self.delete(deletions)
        }
    }

    /// Resume upload to S3 that previously failed
    ///
    /// When a copy or sync to S3 operation fails it will throw a
    /// S3TransferManager.Error.uploadFailed error. This contains a `UploadOperation`.
    /// struct. You can resume the upload by passing the struct to the this function.
    ///
    /// - Parameters:
    ///   - download: Details of remaining downloads to perform
    ///   - options: Download options
    ///   - progress: Progress function
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func resume(
        download: UploadOperation,
        options: PutOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        return try await self.copy(
            download.transfers,
            options: options,
            progress: progress
        )
    }

    /// Resume download from S3 that previously failed
    ///
    /// When a copy or sync to file system operation fails it will throw a
    /// S3TransferManager.Error.downloadFailed error. This contains a `DownloadOperation`.
    /// struct. You can resume the download by passing the struct to the this function.
    ///
    /// - Parameters:
    ///   - download: Details of remaining downloads to perform
    ///   - options: Download options
    ///   - progress: Progress function
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func resume(
        download: DownloadOperation,
        options: GetOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        return try await self.copy(
            download.transfers,
            options: options,
            progress: progress
        )
    }

    /// Resume copy from S3 to S3 that previously failed
    ///
    /// When a copy or sync to S3 operation fails it will throw a
    /// S3TransferManager.Error.copyFailed error. This contains a `CopyOperation`.
    /// struct. You can resume the copy by passing the struct to the this function.
    ///
    /// - Parameters:
    ///   - download: Details of remaining downloads to perform
    ///   - options: Download options
    ///   - progress: Progress function
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func resume(
        download: CopyOperation,
        options: CopyOptions = .init(),
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        return try await self.copy(
            download.transfers,
            options: options,
            progress: progress
        )
    }

    /// delete a file on S3
    public func delete(_ s3File: S3File) async throws {
        self.logger.debug("Deleting \(s3File)")
        _ = try await self.s3.deleteObject(.init(bucket: s3File.bucket, key: s3File.key), logger: self.logger)
    }

    /// delete a folder on S3
    public func delete(_ s3Folder: S3Folder) async throws {
        let files = try await listFiles(in: s3Folder)
        let request = S3.DeleteObjectsRequest(
            bucket: s3Folder.bucket,
            delete: .init(objects: files.map { .init(key: $0.file.key) })
        )
        _ = try await self.s3.deleteObjects(request, logger: self.logger)
    }
}

extension S3FileTransferManager {
    struct FileDescriptor: Equatable {
        let name: String
        let modificationDate: Date
        let size: Int
    }

    struct S3FileDescriptor: Equatable {
        let file: S3File
        let modificationDate: Date
        let size: Int
    }

    /// List files in local folder
    func listFiles(in folder: String) async throws -> [FileDescriptor] {
        let eventLoop = self.s3.eventLoopGroup.next()
        return try await self.threadPool.runIfActive(eventLoop: eventLoop) {
            var files: [FileDescriptor] = []
            let path = URL(fileURLWithPath: folder)
            guard let fileEnumerator = FileManager.default.enumerator(
                at: path,
                includingPropertiesForKeys: [.contentModificationDateKey, .isDirectoryKey, .fileSizeKey],
                options: .skipsHiddenFiles
            ) else {
                throw Error.failedToEnumerateFolder(folder)
            }
            while let file = fileEnumerator.nextObject() as? URL {
                let fileResolved = file.resolvingSymlinksInPath()
                var isDirectory: ObjCBool = false
                // ignore if it is a directory
                _ = FileManager.default.fileExists(atPath: fileResolved.path, isDirectory: &isDirectory)
                guard !isDirectory.boolValue else { continue }
                // get modification data and append along with file name
                let attributes = try FileManager.default.attributesOfItem(atPath: fileResolved.path)
                guard let modificationDate = attributes[.modificationDate] as? Date else { continue }
                guard let size = attributes[.size] as? NSNumber else { continue }
                let fileDescriptor = FileDescriptor(name: fileResolved.path, modificationDate: modificationDate, size: size.intValue)
                files.append(fileDescriptor)
            }
            return files
        }.get()
    }

    /// List files in S3 folder
    func listFiles(in folder: S3Folder) async throws -> [S3FileDescriptor] {
        let request = S3.ListObjectsV2Request(bucket: folder.bucket, prefix: folder.key)
        var files: [S3FileDescriptor] = []
        for try await objects in self.s3.listObjectsV2Paginator(request, logger: self.logger) {
            let newFiles: [S3FileDescriptor] = objects.contents?.compactMap {
                guard let key = $0.key,
                      let lastModified = $0.lastModified,
                      let fileSize = $0.size else { return nil }
                return S3FileDescriptor(
                    file: S3File(bucket: folder.bucket, key: key),
                    modificationDate: lastModified,
                    size: Int(fileSize)
                )
            } ?? []
            files += newFiles
        }
        return files
    }

    /// Validate we can save file list from S3 to file system
    ///
    /// Look for files that are the same name as directory names in other files
    func validateFileList(_ list: [S3FileDescriptor], ignoreClashes: Bool) throws -> [S3FileDescriptor] {
        let list = list.sorted { $0.file.key < $1.file.key }
        return try list.reduce([]) { result, file -> [S3FileDescriptor] in
            let filename = file.file.key
            return try result.compactMap {
                let prevFilename = $0.file.key
                if filename.hasPrefix(prevFilename),
                   prevFilename.last == "/" || filename.dropFirst(prevFilename.count).first == "/"
                {
                    if ignoreClashes {
                        return nil
                    } else {
                        throw Error.fileFolderClash(prevFilename, filename)
                    }
                }
                return $0
            } + [file]
        }
    }

    /// Internal version of copy from local file system to S3
    func copy(
        _ transfers: [(from: FileDescriptor, to: S3File)],
        options: PutOptions,
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        enum UploadTaskResult {
            case success
            case failed(error: Swift.Error, from: FileDescriptor, to: S3File)
        }

        let result = await withTaskGroup(of: UploadTaskResult.self) { group in
            var count = 0
            var failedTransfers: [(from: FileDescriptor, to: S3File)] = []
            var error: Swift.Error?
            let folderProgress = FolderUploadProgress(transfers.map { $0.from }, progress: progress)

            for transfer in transfers {
                group.addTask {
                    do {
                        try await self.copy(from: transfer.from.name, to: transfer.to, options: options) {
                            try await folderProgress.updateProgress(transfer.from.name, progress: $0)
                        }
                        await folderProgress.setFileUploaded(transfer.from.name)
                        return .success
                    } catch {
                        return .failed(error: error, from: transfer.from, to: transfer.to)
                    }
                }
                count += 1
                if count > self.configuration.maxConcurrentTasks {
                    let result = await group.first { _ in true }
                    if case .failed(let error2, let from, let to) = result {
                        error = error2
                        failedTransfers.append((from: from, to: to))
                        if self.configuration.cancelOnError {
                            group.cancelAll()
                            return (error: error, failed: failedTransfers)
                        }
                    }
                }
            }
            for await result in group {
                if case .failed(let error2, let from, let to) = result {
                    error = error2
                    failedTransfers.append((from: from, to: to))
                    if self.configuration.cancelOnError {
                        group.cancelAll()
                        break
                    }
                }
            }
            return (error: error, failed: failedTransfers)
        }
        if let error = result.error {
            throw Error.uploadFailed(error, .init(transfers: result.failed))
        }
    }

    /// Internal version of copy from S3 to local file system
    func copy(
        _ transfers: [(from: S3FileDescriptor, to: String)],
        options: GetOptions,
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        enum DownloadTaskResult {
            case success
            case failed(error: Swift.Error, from: S3FileDescriptor, to: String)
        }

        let result = await withTaskGroup(of: DownloadTaskResult.self) { group in
            var count = 0
            var failedTransfers: [(from: S3FileDescriptor, to: String)] = []
            var error: Swift.Error?
            let folderProgress = FolderUploadProgress(transfers.map { $0.from }, progress: progress)

            for transfer in transfers {
                group.addTask {
                    do {
                        try await self.copy(from: transfer.from.file, to: transfer.to, options: options) {
                            try await folderProgress.updateProgress(transfer.from.file.key, progress: $0)
                        }
                        await folderProgress.setFileUploaded(transfer.from.file.key)
                        return .success
                    } catch {
                        return .failed(error: error, from: transfer.from, to: transfer.to)
                    }
                }
                count += 1
                if count > self.configuration.maxConcurrentTasks {
                    let result = await group.first { _ in true }
                    if case .failed(let error2, let from, let to) = result {
                        error = error2
                        failedTransfers.append((from: from, to: to))
                        if self.configuration.cancelOnError {
                            group.cancelAll()
                            return (error: error, failed: failedTransfers)
                        }
                    }
                }
            }
            for await result in group {
                if case .failed(let error2, let from, let to) = result {
                    error = error2
                    failedTransfers.append((from: from, to: to))
                    if self.configuration.cancelOnError {
                        group.cancelAll()
                        break
                    }
                }
            }
            return (error: error, failed: failedTransfers)
        }
        if let error = result.error {
            throw Error.downloadFailed(error, .init(transfers: result.failed))
        }
    }

    /// Internal version of copy from S3 to S3
    func copy(
        _ transfers: [(from: S3FileDescriptor, to: S3File)],
        options: CopyOptions,
        progress: @escaping @Sendable (Double) async throws -> Void = { _ in }
    ) async throws {
        enum CopyTaskResult {
            case success
            case failed(error: Swift.Error, from: S3FileDescriptor, to: S3File)
        }

        let result = await withTaskGroup(of: CopyTaskResult.self) { group in
            var count = 0
            var failedTransfers: [(from: S3FileDescriptor, to: S3File)] = []
            var error: Swift.Error?
            let folderProgress = FolderUploadProgress(transfers.map { $0.from }, progress: progress)

            for transfer in transfers {
                group.addTask {
                    do {
                        try await self.copy(
                            from: transfer.from.file,
                            to: transfer.to,
                            fileSize: transfer.from.size,
                            options: options
                        )
                        await folderProgress.setFileUploaded(transfer.from.file.key)
                        return .success
                    } catch {
                        return .failed(error: error, from: transfer.from, to: transfer.to)
                    }
                }
                count += 1
                if count > self.configuration.maxConcurrentTasks {
                    let result = await group.first { _ in true }
                    if case .failed(let error2, let from, let to) = result {
                        error = error2
                        failedTransfers.append((from: from, to: to))
                        if self.configuration.cancelOnError {
                            group.cancelAll()
                            return (error: error, failed: failedTransfers)
                        }
                    }
                }
            }
            for await result in group {
                if case .failed(let error2, let from, let to) = result {
                    error = error2
                    failedTransfers.append((from: from, to: to))
                    if self.configuration.cancelOnError {
                        group.cancelAll()
                        break
                    }
                }
            }
            return (error: error, failed: failedTransfers)
        }
        if let error = result.error {
            throw Error.copyFailed(error, .init(transfers: result.failed))
        }
    }

    /// delete a local file
    func delete(_ file: String) async throws {
        self.logger.debug("Deleting \(file)")
        let eventLoop = self.s3.eventLoopGroup.next()
        try await self.threadPool.runIfActive(eventLoop: eventLoop) {
            try FileManager.default.removeItem(atPath: file)
        }.get()
    }

    /// delete files on S3.
    ///
    /// This assumes all files are in the same bucket
    func delete(_ s3Files: [S3File]) async throws {
        guard let first = s3Files.first else { return }
        let request = S3.DeleteObjectsRequest(
            bucket: first.bucket,
            delete: .init(objects: s3Files.map { .init(key: $0.key) })
        )
        _ = try await self.s3.deleteObjects(request, logger: self.logger)
    }

    /// convert file descriptors to equivalent S3 file descriptors when copying one folder
    /// to another. Function assumes the files have srcFolder prefixed
    static func targetFiles(files: [FileDescriptor], from srcFolder: String, to destFolder: S3Folder) -> [(from: FileDescriptor, to: S3File)] {
        let srcFolder = srcFolder.appendingSuffixIfNeeded("/")
        return files.map { file in
            let pathRelative = file.name.removingPrefix(srcFolder)
            return (from: file, to: S3File(bucket: destFolder.bucket, key: destFolder.key + pathRelative))
        }
    }

    /// convert S3 file descriptors to equivalent file descriptors when copying files from
    /// the S3 folder to a local folder. Function assumes the S3 files have the source path
    /// prefixed
    static func targetFiles(files: [S3FileDescriptor], from srcFolder: S3Folder, to destFolder: String) -> [(from: S3FileDescriptor, to: String)] {
        let destFolder = destFolder.appendingSuffixIfNeeded("/")
        return files.map { file in
            let pathRelative = file.file.key.removingPrefix(srcFolder.key)
            return (from: file, to: destFolder + pathRelative)
        }
    }

    /// convert S3 file descriptors to equivalent S3 file descriptors when copying files from
    /// the S3 folder to another S3 folder. Function assumes the S3 files have the source path
    /// prefixed
    static func targetFiles(files: [S3FileDescriptor], from srcFolder: S3Folder, to destFolder: S3Folder) -> [(from: S3FileDescriptor, to: S3File)] {
        return files.map { file in
            let pathRelative = file.file.key.removingPrefix(srcFolder.key)
            return (from: file, to: .init(bucket: destFolder.bucket, key: destFolder.key + pathRelative))
        }
    }

    static let pathAllowedCharacters = CharacterSet.urlPathAllowed.subtracting(.init(charactersIn: "+"))
}

extension Array {
    func concurrentForEach(
        maxConcurrentTasks: Int,
        cancelOnError: Bool,
        _ operation: @escaping @Sendable (Element) async throws -> Void
    ) async throws {
        let result = await withTaskGroup(of: Result<Void, Swift.Error>.self) { group in
            var count = 0
            var result: Result<Void, Swift.Error> = .success(())
            for element in self {
                group.addTask {
                    do {
                        try await operation(element)
                        return .success(())
                    } catch {
                        return .failure(error)
                    }
                }
                count += 1
                if count > maxConcurrentTasks {
                    let taskResult = await group.first { _ in true }
                    if case .failure(let error) = taskResult {
                        result = .failure(error)
                        if cancelOnError {
                            group.cancelAll()
                            return result
                        }
                    }
                }
            }
            for await taskResult in group {
                if case .failure = taskResult {
                    result = taskResult
                    if cancelOnError {
                        group.cancelAll()
                        return result
                    }
                }
            }
            return result
        }
        if case .failure(let error) = result {
            throw error
        }
    }
}
