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
import NIO
import NIOConcurrencyHelpers
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
    /// how class created its thread pool
    let threadPoolProvider: S3.ThreadPoolProvider
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
        self.threadPoolProvider = threadPoolProvider

        switch threadPoolProvider {
        case .createNew:
            self.threadPool = NIOThreadPool(numberOfThreads: 2)
            self.threadPool.start()
        case .shared(let sharedPool):
            self.threadPool = sharedPool
        }
        self.fileIO = NonBlockingFileIO(threadPool: self.threadPool)
        self.configuration = configuration
        self.logger = logger
    }

    deinit {
        // if class owned its own thread pool then makes sure it has been deleted
        if case .createNew = self.threadPoolProvider {
            assert(
                self.isShutdown.load(ordering: .relaxed),
                "S3FileTransferManager not shut down before the deinit. Please call S3FileTransferManager.syncShutdown() when no longer needed."
            )
        }
    }

    /// Shutdown S3 Transfer manager.
    ///
    /// If a thread pool was not passed at initialisation then it is required that `syncShutdown` is called
    /// prior to deleting the class so the thread pool can be shutdown.
    public func syncShutdown() throws {
        if case .createNew = self.threadPoolProvider {
            try threadPool.syncShutdownGracefully()
            self.isShutdown.store(true, ordering: .relaxed)
        }
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
        progress: @escaping (Double) throws -> Void = { _ in }
    ) -> EventLoopFuture<Void> {
        self.logger.debug("Copy from: \(from) to \(to)")
        let eventLoop = self.s3.eventLoopGroup.next()
        return self.fileIO.openFile(path: from, eventLoop: eventLoop)
            .flatMapErrorThrowing { _ in
                throw Error.fileDoesNotExist(String(describing: from))
            }
            .flatMap { fileHandle, fileRegion in
                let fileSize = fileRegion.readableBytes
                // if file size is greater than multipart threshold then use multipart upload for uploading the file
                if fileSize > self.configuration.multipartThreshold {
                    let request = S3.CreateMultipartUploadRequest(bucket: to.bucket, key: to.key, options: options)
                    return self.s3.multipartUpload(
                        request,
                        partSize: self.configuration.multipartPartSize,
                        fileHandle: fileHandle,
                        fileIO: self.fileIO,
                        uploadSize: fileSize,
                        abortOnFail: true,
                        logger: self.logger,
                        on: eventLoop,
                        progress: progress
                    )
                    .flatMapThrowing { _ in try? progress(1.0) }
                    .closeFileHandle(fileHandle)
                } else {
                    let payload: AWSPayload = .fileHandle(fileHandle, offset: 0, size: fileSize, fileIO: self.fileIO) { downloaded in
                        try progress(Double(downloaded) / Double(fileSize))
                    }
                    let request = S3.PutObjectRequest(body: payload, bucket: to.bucket, key: to.key, options: options)
                    return self.s3.putObject(request, logger: self.logger, on: eventLoop)
                        .flatMapThrowing { _ in try? progress(1.0) }
                        .closeFileHandle(fileHandle)
                }
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
        progress: @escaping (Double) throws -> Void = { _ in }
    ) -> EventLoopFuture<Void> {
        self.logger.debug("Copy from: \(from) to \(to)")
        let eventLoop = self.s3.eventLoopGroup.next()
        var bytesDownloaded = 0
        var fileSize: Int64 = 0

        // check for existence of file and get its filesize so we can calculate progress
        return self.s3.headObject(.init(bucket: from.bucket, key: from.key), logger: self.logger, on: eventLoop).flatMapErrorThrowing { error in
            if let error = error as? S3ErrorType, error == .notFound {
                throw Error.fileDoesNotExist(String(describing: from))
            }
            throw error
        }.flatMap { response in
            fileSize = response.contentLength ?? 1
            return self.threadPool.runIfActive(eventLoop: eventLoop) { () -> String in
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
            }
        }.flatMap { filename in
            self.fileIO.openFile(path: filename, mode: .write, flags: .allowFileCreation(), eventLoop: eventLoop)
        }.flatMap { fileHandle -> EventLoopFuture<S3.GetObjectOutput> in
            let request = S3.GetObjectRequest(bucket: from.bucket, key: from.key, options: options)
            return self.s3.getObjectStreaming(request, logger: self.logger, on: eventLoop) { byteBuffer, eventLoop in
                let bufferSize = byteBuffer.readableBytes
                return self.fileIO.write(fileHandle: fileHandle, buffer: byteBuffer, eventLoop: eventLoop).flatMapThrowing { _ in
                    bytesDownloaded += bufferSize
                    try progress(Double(bytesDownloaded) / Double(fileSize))
                }
            }
            .closeFileHandle(fileHandle)
        }.map { _ in }
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
    ) -> EventLoopFuture<Void> {
        self.logger.debug("Copy from: \(from) to \(to)")
        let eventLoop = self.s3.eventLoopGroup.next()
        let copySource = "/\(from.bucket)/\(from.key)".addingPercentEncoding(withAllowedCharacters: Self.pathAllowedCharacters)!
        let request = S3.CopyObjectRequest(bucket: to.bucket, copySource: copySource, key: to.key, options: options)

        let fileSizeFuture: EventLoopFuture<Int>
        if let fileSize = fileSize {
            fileSizeFuture = eventLoop.makeSucceededFuture(fileSize)
        } else {
            let headRequest = S3.HeadObjectRequest(bucket: from.bucket, key: from.key)
            fileSizeFuture = self.s3.headObject(headRequest, on: eventLoop).map { response in Int(response.contentLength!) }
        }
        return fileSizeFuture.flatMap { fileSize -> EventLoopFuture<Void> in
            if fileSize > self.configuration.multipartThreshold {
                return self.s3.multipartCopy(request, objectSize: fileSize, partSize: self.configuration.multipartPartSize)
                    .map { _ in }
            } else {
                return self.s3.copyObject(request, logger: self.logger)
                    .map { _ in }
            }
        }
        .flatMapErrorThrowing { error in
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
        progress: @escaping (Double) throws -> Void = { _ in }
    ) -> EventLoopFuture<Void> {
        let eventLoop = self.s3.eventLoopGroup.next()
        return listFiles(in: folder)
            .flatMap { files in
                let taskQueue = TaskQueue<Void>(maxConcurrentTasks: self.configuration.maxConcurrentTasks, on: eventLoop)
                let folderResolved = URL(fileURLWithPath: folder).standardizedFileURL.resolvingSymlinksInPath()
                let transfers = Self.targetFiles(files: files, from: folderResolved.path, to: s3Folder)
                let folderProgress = FolderUploadProgress(transfers.map { $0.from }, progress: progress)
                transfers.forEach { transfer in
                    taskQueue.submitTask {
                        self.copy(from: transfer.from.name, to: transfer.to, options: options) {
                            try folderProgress.updateProgress(transfer.from.name, progress: $0)
                        }.map { _ in
                            folderProgress.setFileUploaded(transfer.from.name)
                        }
                    }
                }
                return self.complete(taskQueue: taskQueue).map { _ in
                    assert(folderProgress.finished == true)
                }
            }
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
        progress: @escaping (Double) throws -> Void = { _ in }
    ) -> EventLoopFuture<Void> {
        let eventLoop = self.s3.eventLoopGroup.next()
        return listFiles(in: s3Folder)
            .flatMapThrowing { try self.validateFileList($0, ignoreClashes: options.ignoreFileFolderClashes) }
            .flatMap { files in
                let taskQueue = TaskQueue<Void>(maxConcurrentTasks: self.configuration.maxConcurrentTasks, on: eventLoop)
                let transfers = Self.targetFiles(files: files, from: s3Folder, to: folder)
                return self.copy(
                    transfers: transfers,
                    taskQueue: taskQueue,
                    options: options,
                    progress: progress
                )
            }
    }

    /// Copy from S3 folder, to S3 folder
    /// - Parameters:
    ///   - from: Path to source S3 folder
    ///   - to: Path to destination S3 folder
    /// - Returns: EventLoopFuture fulfilled when operation is complete
    public func copy(
        from srcFolder: S3Folder,
        to destFolder: S3Folder,
        options: CopyOptions = .init()
    ) -> EventLoopFuture<Void> {
        let eventLoop = self.s3.eventLoopGroup.next()
        return listFiles(in: srcFolder)
            .flatMap { files in
                let taskQueue = TaskQueue<Void>(maxConcurrentTasks: self.configuration.maxConcurrentTasks, on: eventLoop)
                let transfers = Self.targetFiles(files: files, from: srcFolder, to: destFolder)
                transfers.forEach { transfer in
                    taskQueue.submitTask { self.copy(from: transfer.from.file, to: transfer.to, fileSize: transfer.from.size, options: options) }
                }
                return self.complete(taskQueue: taskQueue)
            }
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
        progress: @escaping (Double) throws -> Void = { _ in }
    ) -> EventLoopFuture<Void> {
        let eventLoop = self.s3.eventLoopGroup.next()

        return listFiles(in: folder).and(listFiles(in: s3Folder))
            .flatMap { files, s3Files in
                let taskQueue = TaskQueue<Void>(maxConcurrentTasks: self.configuration.maxConcurrentTasks, on: eventLoop)
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
                let folderProgress = FolderUploadProgress(transfers.map { $0.from }, progress: progress)
                transfers.forEach { transfer in
                    taskQueue.submitTask {
                        self.copy(from: transfer.from.name, to: transfer.to, options: options) {
                            try folderProgress.updateProgress(transfer.from.name, progress: $0)
                        }.map { _ in
                            folderProgress.setFileUploaded(transfer.from.name)
                        }
                    }
                }
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
                    deletions.forEach { deletion in taskQueue.submitTask { self.delete(deletion) } }
                }
                return self.complete(taskQueue: taskQueue).map { _ in
                    assert(folderProgress.finished == true)
                }
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
        progress: @escaping (Double) throws -> Void = { _ in }
    ) -> EventLoopFuture<Void> {
        let eventLoop = self.s3.eventLoopGroup.next()

        return listFiles(in: folder)
            .and(listFiles(in: s3Folder).flatMapThrowing { try self.validateFileList($0, ignoreClashes: options.ignoreFileFolderClashes) })
            .flatMap { files, s3Files in
                let taskQueue = TaskQueue<Void>(maxConcurrentTasks: self.configuration.maxConcurrentTasks, on: eventLoop)
                let targetFiles = Self.targetFiles(files: s3Files, from: s3Folder, to: folder)
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
                    deletions.forEach { deletion in taskQueue.submitTask { self.delete(deletion) } }
                }
                return self.copy(
                    transfers: transfers,
                    taskQueue: taskQueue,
                    options: options,
                    progress: progress
                )
            }
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
        options: CopyOptions = .init()
    ) -> EventLoopFuture<Void> {
        let eventLoop = self.s3.eventLoopGroup.next()

        return listFiles(in: srcFolder).and(listFiles(in: destFolder))
            .flatMap { srcFiles, destFiles in
                let taskQueue = TaskQueue<Void>(maxConcurrentTasks: self.configuration.maxConcurrentTasks, on: eventLoop)
                let targetFiles = Self.targetFiles(files: srcFiles, from: srcFolder, to: destFolder)
                let destKeyMap = Dictionary(uniqueKeysWithValues: destFiles.map { ($0.file.key, $0) })
                let transfers = targetFiles.compactMap { transfer -> (from: S3FileDescriptor, to: S3File)? in
                    // does file exist in destination folder
                    guard let file = destKeyMap[transfer.to.key] else { return transfer }
                    // does local file have a later date
                    guard file.modificationDate > transfer.from.modificationDate else { return transfer }
                    return nil
                }
                transfers.forEach { transfer in
                    taskQueue.submitTask { self.copy(from: transfer.from.file, to: transfer.to, fileSize: transfer.from.size, options: options) }
                }
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
                    deletions.forEach { deletion in taskQueue.submitTask { self.delete(deletion) } }
                }
                return self.complete(taskQueue: taskQueue)
            }
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
        progress: @escaping (Double) throws -> Void = { _ in }
    ) -> EventLoopFuture<Void> {
        let eventLoop = self.s3.eventLoopGroup.next()

        let taskQueue = TaskQueue<Void>(maxConcurrentTasks: self.configuration.maxConcurrentTasks, on: eventLoop)
        return self.copy(
            transfers: download.transfers,
            taskQueue: taskQueue,
            options: options,
            progress: progress
        )
    }

    /// delete a file on S3
    public func delete(_ s3File: S3File) -> EventLoopFuture<Void> {
        self.logger.debug("Deleting \(s3File)")
        return self.s3.deleteObject(.init(bucket: s3File.bucket, key: s3File.key), logger: self.logger).map { _ in }
    }

    /// delete a folder on S3
    public func delete(_ s3Folder: S3Folder) -> EventLoopFuture<Void> {
        let eventLoop = self.s3.eventLoopGroup.next()
        return listFiles(in: s3Folder)
            .flatMap { files in
                let taskQueue = TaskQueue<Void>(maxConcurrentTasks: self.configuration.maxConcurrentTasks, on: eventLoop)
                files.forEach { deletion in taskQueue.submitTask { self.delete(deletion.file) } }
                return self.complete(taskQueue: taskQueue)
            }
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

    /// Wait on all tasks succeeding. If there is an error the operation will either continue or cancel depending on `Configuration.cancelOnError`
    func complete<T>(taskQueue: TaskQueue<T>) -> EventLoopFuture<Void> {
        taskQueue.andAllSucceed()
            .flatMapError { error in
                if self.configuration.cancelOnError {
                    return taskQueue.cancel().flatMapThrowing { throw error }
                } else {
                    return taskQueue.flush().flatMapThrowing { throw error }
                }
            }
    }

    /// List files in local folder
    func listFiles(in folder: String) -> EventLoopFuture<[FileDescriptor]> {
        let eventLoop = self.s3.eventLoopGroup.next()
        return self.threadPool.runIfActive(eventLoop: eventLoop) {
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
        }
    }

    /// List files in S3 folder
    func listFiles(in folder: S3Folder) -> EventLoopFuture<[S3FileDescriptor]> {
        let request = S3.ListObjectsV2Request(bucket: folder.bucket, prefix: folder.key)
        return self.s3.listObjectsV2Paginator(request, [], logger: self.logger) { accumulator, response, eventLoop in
            let files: [S3FileDescriptor] = response.contents?.compactMap {
                guard let key = $0.key,
                      let lastModified = $0.lastModified,
                      let fileSize = $0.size else { return nil }
                return S3FileDescriptor(
                    file: S3File(bucket: folder.bucket, key: key),
                    modificationDate: lastModified,
                    size: Int(fileSize)
                )
            } ?? []
            return eventLoop.makeSucceededFuture((true, accumulator + files))
        }
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

    /// Internal version of sync, with folder progress and task queue setup
    func copy(
        transfers: [(from: S3FileDescriptor, to: String)],
        taskQueue: TaskQueue<Void>,
        options: GetOptions,
        progress: @escaping (Double) throws -> Void
    ) -> EventLoopFuture<Void> {
        var failedTransfers: [(from: S3FileDescriptor, to: String)] = []
        let lock = NIOLock()
        let folderProgress = FolderUploadProgress(transfers.map { $0.from }, progress: progress)
        let transfersComplete = transfers.map { transfer in
            taskQueue.submitTask {
                self.copy(from: transfer.from.file, to: transfer.to, options: options) {
                    try folderProgress.updateProgress(transfer.from.file.key, progress: $0)
                }.map { _ in
                    folderProgress.setFileUploaded(transfer.from.file.key)
                }
            }.flatMapErrorThrowing { error in
                lock.withLock {
                    failedTransfers.append(transfer)
                }
                throw error
            }
        }
        return self.complete(taskQueue: taskQueue).map { _ in
            assert(folderProgress.finished == true)
        }.flatMapError { error in
            return EventLoopFuture.andAllComplete(transfersComplete, on: taskQueue.eventLoop).flatMapThrowing {
                throw Error.downloadFailed(error, .init(transfers: failedTransfers))
            }
        }
    }

    /// delete a local file
    func delete(_ file: String) -> EventLoopFuture<Void> {
        self.logger.debug("Deleting \(file)")
        let eventLoop = self.s3.eventLoopGroup.next()
        return self.threadPool.runIfActive(eventLoop: eventLoop) {
            try FileManager.default.removeItem(atPath: file)
        }
    }

    /// convert file descriptors to equivalent S3 file descriptors when copying one folder to another. Function assumes the files have srcFolder prefixed
    static func targetFiles(files: [FileDescriptor], from srcFolder: String, to destFolder: S3Folder) -> [(from: FileDescriptor, to: S3File)] {
        let srcFolder = srcFolder.appendingSuffixIfNeeded("/")
        return files.map { file in
            let pathRelative = file.name.removingPrefix(srcFolder)
            return (from: file, to: S3File(bucket: destFolder.bucket, key: destFolder.key + pathRelative))
        }
    }

    /// convert S3 file descriptors to equivalent file descriptors when copying files from the S3 folder to a local folder. Function assumes the S3 files have
    /// the source path prefixed
    static func targetFiles(files: [S3FileDescriptor], from srcFolder: S3Folder, to destFolder: String) -> [(from: S3FileDescriptor, to: String)] {
        let destFolder = destFolder.appendingSuffixIfNeeded("/")
        return files.map { file in
            let pathRelative = file.file.key.removingPrefix(srcFolder.key)
            return (from: file, to: destFolder + pathRelative)
        }
    }

    /// convert S3 file descriptors to equivalent S3 file descriptors when copying files from the S3 folder to another S3 folder. Function assumes the S3 files have
    /// the source path prefixed
    static func targetFiles(files: [S3FileDescriptor], from srcFolder: S3Folder, to destFolder: S3Folder) -> [(from: S3FileDescriptor, to: S3File)] {
        return files.map { file in
            let pathRelative = file.file.key.removingPrefix(srcFolder.key)
            return (from: file, to: .init(bucket: destFolder.bucket, key: destFolder.key + pathRelative))
        }
    }

    static let pathAllowedCharacters = CharacterSet.urlPathAllowed.subtracting(.init(charactersIn: "+"))
}

extension EventLoopFuture {
    func closeFileHandle(_ fileHandle: NIOFileHandle) -> EventLoopFuture<Value> {
        return self.flatMapErrorThrowing { error in
            try fileHandle.close()
            throw error
        }
        .flatMapThrowing { rt -> Value in
            try fileHandle.close()
            return rt
        }
    }
}
