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

import Atomics
import Foundation
import Logging
import NIOCore
import SotoS3
import Testing
import _NIOFileSystem

@testable import SotoS3FileTransfer

@Suite(.tempFolder("test-assets"))
struct S3FileTransferManagerTests {
    static var rootPath: FilePath {
        FilePath(#filePath).removingLastComponent().removingLastComponent().removingLastComponent()
    }

    init() {
        if TestEnvironment.isUsingLocalstack {
            Logger(label: "S3FileTransferManagerTests").info("Using Localstack")
        } else {
            Logger(label: "S3FileTransferManagerTests").info("Using AWS Services")
        }
    }
    package func withClient<Value>(_ awsClient: AWSClient, operation: () async throws -> Value) async throws -> Value {
        let result: Result<Value, any Error>
        do {
            result = .success(try await operation())
        } catch {
            result = .failure(error)
        }
        try await awsClient.shutdown()
        return try result.get()
    }

    func createBucket(name: String, s3: S3) async throws {
        let bucketRequest = S3.CreateBucketRequest(bucket: name)
        do {
            _ = try await s3.createBucket(bucketRequest, logger: TestEnvironment.logger)
            try await s3.waitUntilBucketExists(.init(bucket: name), logger: TestEnvironment.logger)
        } catch let error as S3ErrorType {
            if case .bucketAlreadyOwnedByYou = error.error {
            } else {
                throw error
            }
        }
    }

    func deleteBucket(name: String, s3: S3) async throws {
        let response = try await s3.listObjectsV2(
            .init(bucket: name, maxKeys: 2048),
            logger: TestEnvironment.logger
        )
        if let contents = response.contents {
            let request = S3.DeleteObjectsRequest(
                bucket: name,
                delete: S3.Delete(objects: contents.compactMap { $0.key.map { .init(key: $0) } })
            )
            _ = try await s3.deleteObjects(request, logger: TestEnvironment.logger)
        }
        try await s3.deleteBucket(.init(bucket: name), logger: TestEnvironment.logger)
    }

    func withS3FileTransfer(
        _ function: String = #function,
        test operation: @escaping (S3FileTransferManager) async throws -> Void
    ) async throws {
        try await withLogger(TestEnvironment.logger) { logger in
            let client = AWSClient(
                credentialProvider: TestEnvironment.credentialProvider,
                middleware: TestEnvironment.middlewares,
                logger: logger
            )
            let s3 = S3(
                client: client,
                region: .euwest1
            ).with(timeout: .seconds(30))
            let s3FileTransfer = S3FileTransferManager(s3: s3, logger: TestEnvironment.logger)
            do {
                try await operation(s3FileTransfer)
            } catch {
                try? await client.shutdown()
                throw error
            }
            try await client.shutdown()
        }
    }

    /// create S3 bucket with supplied name and run supplied closure
    func testBucket(
        _ function: String = #function,
        test operation: @escaping (S3FileTransferManager, String) async throws -> Void
    ) async throws {
        let name = self.generateResourceName(function)
        try await withS3FileTransfer { s3FileTransfer in
            try await self.createBucket(name: name, s3: s3FileTransfer.s3)
            do {
                try await operation(s3FileTransfer, name)
            } catch {
                try? await self.deleteBucket(name: name, s3: s3FileTransfer.s3)
                throw error
            }
            try await self.deleteBucket(name: name, s3: s3FileTransfer.s3)
        }
    }

    func generateResourceName(_ prefix: String = #function) -> String {
        let suffix = String(Int.random(in: Int.min..<Int.max), radix: 16)
        return "soto-" + (prefix + suffix).filter { $0.isLetter || $0.isNumber }.lowercased()
    }

    func createRandomBuffer(size: Int) -> [UInt8] {
        // create buffer
        let data = (0..<size).map { _ in UInt8.random(in: 0...255) }
        return data
    }

    func createRandomByteBuffer(size: Int) -> ByteBuffer {
        ByteBuffer(bytes: createRandomBuffer(size: size))
    }

    @Test(.tempFolder("testUploadDownload"), .tempFile(contentSize: 200_000))
    func testUploadDownload() async throws {
        try await testBucket { s3FileTransfer, bucket in
            try await s3FileTransfer.copy(
                from: .init(TempFileTrait.file.fullPath),
                to: S3FilePath(bucket: bucket, key: "testFile"),
                options: .init(metadata: ["test": "1,2,3"])
            ) { Logger.current.debug("\($0)") }
            let copy = "\(TempFileTrait.file.fullPath)-copy"
            try await s3FileTransfer.copy(
                from: S3FilePath(bucket: bucket, key: "testFile"),
                to: .init(copy)
            ) { Logger.current.debug("\($0)") }
            let buffer = try await ByteBuffer(contentsOf: FilePath(TempFileTrait.file.fullPath), maximumSizeAllowed: .unlimited)
            let buffer2 = try await ByteBuffer(contentsOf: FilePath(copy), maximumSizeAllowed: .unlimited)
            #expect(buffer == buffer2)
        }
    }

    @Test(.tempFolder("testS3Copy"), .tempFile(contentSize: 200_000))
    func testS3Copy() async throws {
        try await testBucket { s3FileTransfer, bucket in
            try await s3FileTransfer.copy(
                from: .init(TempFileTrait.file.fullPath),
                to: S3FilePath(bucket: bucket, key: "testFile")
            ) { Logger.current.debug("\($0)") }
            try await s3FileTransfer.copy(
                from: S3FilePath(bucket: bucket, key: "testFile"),
                to: S3FilePath(bucket: bucket, key: "testFile2")
            )
            let copy = "\(TempFileTrait.file.fullPath)-copy"
            try await s3FileTransfer.copy(
                from: S3FilePath(bucket: bucket, key: "testFile2"),
                to: .init(copy)
            ) { Logger.current.debug("\($0)") }

            let buffer = try await ByteBuffer(contentsOf: FilePath(TempFileTrait.file.fullPath), maximumSizeAllowed: .unlimited)
            let buffer2 = try await ByteBuffer(contentsOf: FilePath(copy), maximumSizeAllowed: .unlimited)
            #expect(buffer == buffer2)
        }
    }

    @Test func testS3UploadOfNonExistentFile() async throws {
        let filename = FilePath(TempFolderTrait.folder).appending(#function)
        try await testBucket { s3FileTransfer, bucket in
            await #expect(throws: S3FileTransferManager.Error.fileDoesNotExist(filename)) {
                _ = try await s3FileTransfer.copy(
                    from: filename,
                    to: S3FilePath(bucket: bucket, key: "doesNotExist")
                )
            }
        }
    }

    @Test func testS3DownloadOfNonExistentFile() async throws {
        let filename = FilePath(TempFolderTrait.folder).appending(#function)
        try await testBucket { s3FileTransfer, bucket in
            await #expect(throws: S3FileTransferManager.Error.s3FileDoesNotExist(S3FilePath(url: "s3://\(bucket)/doesNotExist")!)) {
                _ = try await s3FileTransfer.copy(
                    from: S3FilePath(bucket: bucket, key: "doesNotExist"),
                    to: filename
                )
            }
        }
    }

    @Test func testS3CopyOfNonExistentFile() async throws {
        try await testBucket { s3FileTransfer, bucket in
            await #expect(throws: S3FileTransferManager.Error.s3FileDoesNotExist(S3FilePath(url: "s3://\(bucket)/doesNotExist")!)) {
                _ = try await s3FileTransfer.copy(
                    from: S3FilePath(bucket: bucket, key: "doesNotExist"),
                    to: S3FilePath(bucket: bucket, key: "destination")
                )
            }
        }
    }

    @Test func testListFiles() async throws {
        try await withS3FileTransfer { s3FileTransfer in
            let files = try await s3FileTransfer.listFiles(in: Self.rootPath.appending("Tests"))
            #expect(
                files.firstIndex {
                    $0.name
                        == Self.rootPath.appending("/Tests/SotoS3FileTransferTests/S3FileTransferManagerTests.swift")
                } != nil
            )
            #expect(files.firstIndex { $0.name == Self.rootPath.appending("/Tests/SotoS3FileTransferTests") } == nil)
        }
    }

    @Test(.tempFolder("testListFilesIgnoresHiddenFiles"), .tempFile(.path(".hiddenFile")))
    func testListFilesIgnoresHiddenFiles() async throws {
        try await withS3FileTransfer { s3FileTransfer in
            let files = try await s3FileTransfer.listFiles(in: FilePath(TempFolderTrait.folder))
            #expect(files.count == 0)
        }
    }

    @Test func testListS3Files() async throws {
        let test = "testListS3Files"
        try await testBucket { s3FileTransfer, bucket in
            _ = try await s3FileTransfer.s3.putObject(
                .init(body: .init(string: test), bucket: bucket, key: "testListS3Files/2.txt")
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(body: .init(string: test), bucket: bucket, key: "testListS3Files2/1.txt")
            )

            let files = try await s3FileTransfer.listFiles(
                in: S3Folder(url: "s3://\(bucket)/testListS3Files")!
            )
            #expect(files.firstIndex { $0.file.key == "testListS3Files/2.txt" } != nil)
            #expect(files.firstIndex { $0.file.key == "testListS3Files2/1.txt" } == nil)
        }
    }

    /// test the list of target files is calculated correctly
    @Test func testTargetFiles() async throws {
        let files: [S3FileTransferManager.FileDescriptor] = [
            .init(name: "/User/JohnSmith/Documents/test.doc", modificationDate: Date(), size: 0),
            .init(name: "/User/JohnSmith/Documents/hello.doc", modificationDate: Date(), size: 0),
        ]
        let s3Files = S3FileTransferManager.targetFiles(
            files: files,
            from: "/User/JohnSmith/Documents/",
            to: S3Folder(url: "s3://my-bucket/")!
        )
        #expect(s3Files[0].to.key == "test.doc")
        #expect(s3Files[1].to.key == "hello.doc")

        let s3Files2 = S3FileTransferManager.targetFiles(
            files: files,
            from: "/User/JohnSmith",
            to: S3Folder(url: "s3://my-bucket/test")!
        )
        #expect(s3Files2[0].to.key == "test/Documents/test.doc")
        #expect(s3Files2[1].to.key == "test/Documents/hello.doc")

    }

    /// test the list of s3 target files is calculated correctly
    @Test func testS3TargetFiles() async throws {
        let s3Files: [S3FileTransferManager.S3FileDescriptor] = [
            .init(
                file: S3FilePath(url: "s3://my-bucket/User/JohnSmith/Documents/test.doc")!,
                modificationDate: Date(),
                size: 1024
            ),
            .init(
                file: S3FilePath(url: "s3://my-bucket/User/JohnSmith/Documents/hello.doc")!,
                modificationDate: Date(),
                size: 2000
            ),
        ]
        let files = S3FileTransferManager.targetFiles(
            files: s3Files,
            from: S3Folder(url: "s3://my-bucket")!,
            to: "/User/JohnSmith/Downloads"
        )
        #expect(files[0].to == "/User/JohnSmith/Downloads/User/JohnSmith/Documents/test.doc")
        #expect(files[1].to == "/User/JohnSmith/Downloads/User/JohnSmith/Documents/hello.doc")

        let files2 = S3FileTransferManager.targetFiles(
            files: s3Files,
            from: S3Folder(url: "s3://my-bucket/User/JohnSmith")!,
            to: "/User/JohnSmith/Downloads"
        )
        #expect(files2[0].to == "/User/JohnSmith/Downloads/Documents/test.doc")
        #expect(files2[1].to == "/User/JohnSmith/Downloads/Documents/hello.doc")
    }

    @Test func testCopyPathLocalToS3() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let folder = S3Folder(bucket: bucket, key: "testCopyPathLocalToS3")
            try await s3FileTransfer.copy(from: Self.rootPath.appending("Sources"), to: folder)
            let files = try await s3FileTransfer.listFiles(in: folder)
            #expect(
                files.first(where: {
                    $0.file.key == "testCopyPathLocalToS3/SotoS3FileTransfer/S3Path.swift"
                }) != nil
            )
        }
    }

    @Test func testS3toS3CopyPath() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let folder1 = S3Folder(bucket: bucket, key: "testS3toS3CopyPath")
            let folder2 = S3Folder(bucket: bucket, key: "testS3toS3CopyPath_Copy")
            try await s3FileTransfer.copy(from: Self.rootPath.appending("Sources"), to: folder1)
            try await s3FileTransfer.copy(from: folder1, to: folder2)
            let files = try await s3FileTransfer.listFiles(in: folder2)
            #expect(
                files.first(where: {
                    $0.file.key
                        == "testS3toS3CopyPath_Copy/SotoS3FileTransfer/S3FileTransferManager.swift"
                }) != nil
            )
        }
    }

    @Test(.tempFolder("testSyncPathLocalToS3"))
    func testSyncPathLocalToS3() async throws {
        try await testBucket { s3FileTransfer, bucket in
            try await s3FileTransfer.sync(
                from: Self.rootPath.appending("Tests"),
                to: S3Folder(bucket: bucket, key: "testSyncPathLocalToS3"),
                delete: true
            )
            try await s3FileTransfer.sync(
                from: S3Folder(bucket: bucket, key: "testSyncPathLocalToS3"),
                to: S3Folder(bucket: bucket, key: "testSyncPathLocalToS3_v2"),
                delete: true
            )
            try await s3FileTransfer.sync(
                from: S3Folder(bucket: bucket, key: "testSyncPathLocalToS3_v2"),
                to: FilePath(TempFolderTrait.folder),
                delete: true,
                progress: { Logger.current.debug("\($0)") }
            )

            let files = try await s3FileTransfer.listFiles(in: FilePath(TempFolderTrait.folder))
            #expect(
                files.first(where: {
                    $0.name.components.suffix(2) == ["SotoS3FileTransferTests", "S3PathTests.swift"]
                }) != nil
            )
        }
    }

    /// Test cancelling download and resuming it
    ///
    /// Used by testCancelledSyncWithCancel and testCancelledSyncWithFlush
    func testCancelledSync(
        _ s3FileTransfer: S3FileTransferManager,
        bucket: String,
        folderName: String
    ) async throws {
        let folder = S3Folder(url: "s3://\(bucket)/\(folderName)")!
        let localFolder = FilePath(TempFolderTrait.folder)
        let originalFiles = try await s3FileTransfer.listFiles(in: Self.rootPath.appending("Sources"))
        #expect(originalFiles.count != 0)

        // copy files to S3
        try await s3FileTransfer.sync(from: Self.rootPath.appending("Sources"), to: folder, delete: true)
        do {
            // copy file back to local file system but cancel halfway through process
            let cancelled = ManagedAtomic(false)
            try await s3FileTransfer.sync(from: folder, to: localFolder, delete: true) { progress in
                // make sure we only cancel one task
                if progress > 0.3, cancelled.exchange(true, ordering: .relaxed) == false {
                    throw CancellationError()
                }
            }
        } catch {
            // can't guarantee this check will work if only file with error is in download list
            if s3FileTransfer.configuration.cancelOnError == true {
                let localFiles = try await s3FileTransfer.listFiles(in: localFolder)
                // check total file size for folder is different
                let localFileSize = localFiles.map { $0.size }.reduce(0, +)
                let originalFileSize = originalFiles.map { $0.size }.reduce(0, +)
                #expect(localFileSize != originalFileSize)
            }
            let error = try #require(error as? S3FileTransferManager.Error)
            if case .downloadFailed(_, let download) = error {
                try await s3FileTransfer.resume(download: download)
            }
        }
        let localFiles = try await s3FileTransfer.listFiles(in: localFolder)
        // check total file size for folder is different
        let localFileSize = localFiles.map { $0.size }.reduce(0, +)
        let originalFileSize = originalFiles.map { $0.size }.reduce(0, +)
        #expect(localFileSize == originalFileSize)
    }

    /// Test cancelling download with cancelOnError set to true
    @Test(.tempFolder("testCancelledSyncWithCancel"))
    func testCancelledSyncWithCancel() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let s3FileTransfer = S3FileTransferManager(
                s3: s3FileTransfer.s3,
                configuration: .init(cancelOnError: true, maxConcurrentTasks: 2),
                logger: Logger(label: "S3TransferTests")
            )
            try await self.testCancelledSync(
                s3FileTransfer,
                bucket: bucket,
                folderName: "testCancelledSyncWithCancel"
            )
        }
    }

    /// Test cancelling download with cancelOnError set to false
    @Test(.tempFolder("testCancelledSyncWithFlush"))
    func testCancelledSyncWithFlush() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let s3FileTransfer = S3FileTransferManager(
                s3: s3FileTransfer.s3,
                configuration: .init(cancelOnError: false, maxConcurrentTasks: 2),
                logger: Logger(label: "S3TransferTests")
            )
            try await self.testCancelledSync(
                s3FileTransfer,
                bucket: bucket,
                folderName: "testCancelledSyncWithFlush"
            )
        }
    }

    /// Used by testCancelledDownloadWithCancel
    func testCancelledDownload(_ s3FileTransfer: S3FileTransferManager, bucket: String) async throws {
        let folder = S3Folder(url: "s3://\(bucket)/testCancelledDownload")!
        let localFolder = FilePath(TempFolderTrait.folder)
        let originalFiles = try await s3FileTransfer.listFiles(in: Self.rootPath.appending("Sources"))
        #expect(originalFiles.count != 0)

        try await s3FileTransfer.copy(from: Self.rootPath.appending("Sources"), to: folder)
        do {
            let cancelled = ManagedAtomic(false)
            try await s3FileTransfer.copy(from: folder, to: localFolder) { progress in
                Logger.current.debug("\(progress)")
                // make sure we only cancel one task
                if progress > 0.2, cancelled.exchange(true, ordering: .relaxed) == false {
                    throw CancellationError()
                }
            }
        } catch {
            let localFiles = try await s3FileTransfer.listFiles(in: localFolder)
            // check total file size for folder is different
            let localFileSize = localFiles.map { $0.size }.reduce(0, +)
            let originalFileSize = originalFiles.map { $0.size }.reduce(0, +)
            #expect(localFileSize != originalFileSize)
            let error = try #require(error as? S3FileTransferManager.Error)
            if case .downloadFailed(_, let download) = error {
                try? await s3FileTransfer.resume(download: download)
            }
        }
        let localFiles = try await s3FileTransfer.listFiles(in: localFolder)
        // check total file size for folder is equal
        let localFileSize = localFiles.map { $0.size }.reduce(0, +)
        let originalFileSize = originalFiles.map { $0.size }.reduce(0, +)
        #expect(localFileSize == originalFileSize)
    }

    /// Test cancelling download with cancelOnError set to true
    @Test(.tempFolder("testCancelledDownloadWithCancel"))
    func testCancelledDownloadWithCancel() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let s3FileTransfer = S3FileTransferManager(
                s3: s3FileTransfer.s3,
                configuration: .init(cancelOnError: true, maxConcurrentTasks: 4),
                logger: Logger(label: "S3TransferTests")
            )
            try await self.testCancelledDownload(s3FileTransfer, bucket: bucket)
        }
    }

    /// Test deleting a folder works
    @Test
    func testDeleteFolder() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let folder = S3Folder(url: "s3://\(bucket)/testDeleteFolder")!
            try await s3FileTransfer.sync(
                from: Self.rootPath.appending("Tests"),
                to: folder,
                delete: true
            )
            let files = try await s3FileTransfer.listFiles(in: folder)
            #expect(files.count > 0)
            try await s3FileTransfer.delete(folder)
            let files2 = try await s3FileTransfer.listFiles(in: folder)
            #expect(files2.count == 0)
        }
    }
    /// Test sync'ing folder, deleting subfolder and then sync'ing back to client. This uses source code
    /// as the data, so if source code changes structure it may start failing
    @Test(.tempFolder("testDeleteFolderAndSync"))
    func testDeleteFolderAndSync() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let folder = S3Folder(url: "s3://\(bucket)/testDeleteFolderAndSync")!
            let subfolder = folder.subFolder("_subFolder")
            let localFolder = FilePath(TempFolderTrait.folder)
            // get list of files in folder Sources
            let originalFiles = try await s3FileTransfer.listFiles(in: Self.rootPath.appending("Sources"))
            #expect(originalFiles.count != 0)
            // copy to S3
            try await s3FileTransfer.sync(
                from: Self.rootPath.appending("Sources"),
                to: folder,
                delete: true
            )
            // copy contents of s3 folder into sub-folder
            try await s3FileTransfer.copy(from: folder, to: subfolder)
            // copy s3 folder back to local folder
            try await s3FileTransfer.sync(from: folder, to: localFolder, delete: true)
            // number of files in folder should be double the original amount
            var localFiles = try await s3FileTransfer.listFiles(in: localFolder)
            #expect(localFiles.count == originalFiles.count * 2)
            // delete the s3 subfolder
            try await s3FileTransfer.delete(subfolder)
            // sync with local folder deleting any files that don't exist in original s3 folder
            // this should delete the sync'ed subfolder
            try await s3FileTransfer.sync(from: folder, to: localFolder, delete: true)
            // list folders in local folder, should be original count
            localFiles = try await s3FileTransfer.listFiles(in: localFolder)
            #expect(localFiles.count == originalFiles.count)
        }
    }

    /// test downloading a file into a folder
    @Test(.tempFolder("testDownloadFileToFolder"), .tempFile(contentSize: 150_000))
    func testDownloadFileToFolder() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let s3File = S3FilePath(bucket: bucket, key: "testDownload/test.dat")
            let filePath = FilePath(TempFileTrait.file.fullPath)
            try await s3FileTransfer.copy(from: filePath, to: s3File)
            try await s3FileTransfer.delete(filePath)
            try await s3FileTransfer.copy(from: s3File, to: FilePath(TempFolderTrait.folder))
            let files = try await s3FileTransfer.listFiles(in: FilePath(TempFolderTrait.folder))
            #expect(files.count == 1)
            #expect(files[0].name.components.last == "test.dat")
        }
    }

    /// test we get an error when trying to download a folder on top of a file
    @Test func testDownloadFolderToFile() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let folder1 = S3Folder(bucket: bucket, key: "testDownloadFolderToFile")
            try await s3FileTransfer.copy(from: Self.rootPath.appending("Sources"), to: folder1)
            let error = await #expect(throws: S3FileTransferManager.Error.self) {
                _ = try await s3FileTransfer.copy(
                    from: folder1,
                    to: Self.rootPath.appending("Package.swift")
                )
            }
            if case .downloadFailed = error {
                // #expect((error as NSError).code == 512)
            } else {
                Issue.record("\(error)")
            }
        }
    }

    /// check the correct error is thrown when trying to download a file on top of a folder
    @Test(.tempFolder("testFileFolderClash"))
    func testFileFolderClash() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let folder = S3Folder(bucket: bucket, key: "testFileFolderClash")
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "folder"),
                    bucket: bucket,
                    key: "testFileFolderClash/fold"
                )
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "folder"),
                    bucket: bucket,
                    key: "testFileFolderClash/folder"
                )
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "folder"),
                    bucket: bucket,
                    key: "testFileFolderClash/folder*"
                )
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "file"),
                    bucket: bucket,
                    key: "testFileFolderClash/folder/file"
                )
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "file"),
                    bucket: bucket,
                    key: "testFileFolderClash/folder/file2"
                )
            )

            await #expect(
                throws: S3FileTransferManager.Error.fileFolderClash(
                    "testFileFolderClash/folder",
                    "testFileFolderClash/folder/file"
                )
            ) {
                try await s3FileTransfer.copy(from: folder, to: FilePath(TempFolderTrait.folder))
            }
        }
    }

    /// check no error is thrown when trying to download a file with the same prefix
    @Test(.tempFolder("testIgnoreFileFolderClash"))
    func testIgnoreFileFolderClash() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let folder = S3Folder(bucket: bucket, key: "testIgnoreFileFolderClash")
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "folder"),
                    bucket: bucket,
                    key: "testIgnoreFileFolderClash/fold"
                )
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "folder"),
                    bucket: bucket,
                    key: "testIgnoreFileFolderClash/folder"
                )
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "folder"),
                    bucket: bucket,
                    key: "testIgnoreFileFolderClash/folder*"
                )
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "file"),
                    bucket: bucket,
                    key: "testIgnoreFileFolderClash/folder/file"
                )
            )
            _ = try await s3FileTransfer.s3.putObject(
                .init(
                    body: .init(string: "file"),
                    bucket: bucket,
                    key: "testIgnoreFileFolderClash/folder/file2"
                )
            )
            try await s3FileTransfer.copy(
                from: folder,
                to: FilePath(TempFolderTrait.folder),
                options: .init(ignoreFileFolderClashes: true)
            )
            let files = try await s3FileTransfer.listFiles(
                in: FilePath(TempFolderTrait.folder)
            )
            #expect(files.count == 4)
        }
    }

    @Test(.tempFolder("testMultipartUploadDownload"), .tempFile(contentSize: 10_202_400))
    func testMultipartUploadDownload() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let copy = "\(TempFileTrait.file.fullPath)-copy"

            try await s3FileTransfer.copy(
                from: FilePath(TempFileTrait.file.fullPath),
                to: S3FilePath(bucket: bucket, key: "testMultipartUploadDownload")
            )
            try await s3FileTransfer.copy(
                from: S3FilePath(bucket: bucket, key: "testMultipartUploadDownload"),
                to: FilePath(copy)
            ) { Logger.current.debug("\($0)") }

            let buffer = try await ByteBuffer(contentsOf: FilePath(TempFileTrait.file.fullPath), maximumSizeAllowed: .unlimited)
            let buffer2 = try await ByteBuffer(contentsOf: FilePath(copy), maximumSizeAllowed: .unlimited)
            #expect(buffer == buffer2)
        }
    }

    @Test(.tempFolder("testS3MultipartCopy"), .tempFile(contentSize: 10_202_400))
    func testS3MultipartCopy() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let copy = "\(TempFileTrait.file.fullPath)-copy"

            try await s3FileTransfer.copy(
                from: FilePath(TempFileTrait.file.fullPath),
                to: S3FilePath(bucket: bucket, key: "testS3MultipartCopy")
            )
            try await s3FileTransfer.copy(
                from: S3FilePath(bucket: bucket, key: "testS3MultipartCopy"),
                to: S3FilePath(bucket: bucket, key: "testS3MultipartCopy_Copy")
            )
            try await s3FileTransfer.copy(
                from: S3FilePath(bucket: bucket, key: "testS3MultipartCopy_Copy"),
                to: FilePath(copy)
            ) { Logger.current.debug("\($0)") }

            let buffer = try await ByteBuffer(contentsOf: FilePath(TempFileTrait.file.fullPath), maximumSizeAllowed: .unlimited)
            let buffer2 = try await ByteBuffer(contentsOf: FilePath(copy), maximumSizeAllowed: .unlimited)
            #expect(buffer == buffer2)
        }
    }

    @Test(.tempFolder("testUploadSymbolicLink"), .tempSymbolicLink(destination: Self.rootPath.appending("Package.swift").string))
    func testUploadSymbolicLink() async throws {
        try await testBucket { s3FileTransfer, bucket in
            try await s3FileTransfer.copy(
                from: .init(TempFolderTrait.folder),
                to: S3Folder(bucket: bucket, key: "/")
            ) { Logger.current.debug("\($0)") }
            let getObjectResponse = try await s3FileTransfer.s3.getObject(bucket: bucket, key: TempSymbolicLinkTrait.file.path)
            let contents = try await getObjectResponse.body.collect(upTo: .max)
            let contentsString = String(buffer: contents)
            #expect(contentsString.hasPrefix("// swift-tools-version:"))
        }
    }

    @Test(.tempSymbolicLink(destination: Self.rootPath.appending("Sources").string), .tempFolder("testUploadSymbolicLinkToFolder"))
    func testUploadSymbolicLinkToFolder() async throws {
        let tempFolder = FilePath(TempFolderTrait.folder)
        try await testBucket { s3FileTransfer, bucket in
            try await s3FileTransfer.copy(
                from: .init(TempSymbolicLinkTrait.file.fullPath),
                to: S3Folder(bucket: bucket, key: "/")
            ) { Logger.current.debug("\($0)") }

            try await s3FileTransfer.copy(from: S3Folder(bucket: bucket, key: "/"), to: tempFolder)
            let files = try await s3FileTransfer.listFiles(in: tempFolder)
            #expect(
                files.first {
                    $0.name.components.suffix(3) == ["testUploadSymbolicLinkToFolder", "SotoS3FileTransfer", "S3Path.swift"]
                } != nil
            )
        }
    }

    @Test(.tempFolder("testUploadSymbolicLinkInSubFolder"), .tempSymbolicLink(destination: Self.rootPath.appending("Sources").string))
    func testUploadSymbolicLinkInSubFolder() async throws {
        try await testBucket { s3FileTransfer, bucket in
            try await s3FileTransfer.copy(
                from: .init(TempFolderTrait.folder),
                to: S3Folder(bucket: bucket, key: "/temp")
            ) { Logger.current.debug("\($0)") }
            let files = try await s3FileTransfer.listFiles(in: S3Folder(bucket: bucket, key: "/temp/\(TempSymbolicLinkTrait.file.path)"))
            print(files)
            #expect(
                files.first {
                    $0.file.key == "temp/\(TempSymbolicLinkTrait.file.path)/SotoS3FileTransfer/S3Path.swift"
                } != nil
            )
        }
    }

    @Test(.tempFolder("testBigFolderUpload"))
    func testBigFolderUpload() async throws {
        try await testBucket { s3FileTransfer, bucket in
            let folder = S3Folder(bucket: bucket, key: "testBigFolderUpload")
            let folder2 = S3Folder(bucket: bucket, key: "testBigFolderUpload_Copy")
            let tempFolder = FilePath(TempFolderTrait.folder)
            let fileCount = try await s3FileTransfer.listFiles(
                in: Self.rootPath.appending(".build/checkouts/soto/Sources/Soto/Services/S3/")
            ).count
            #expect(fileCount != 0)
            try await s3FileTransfer.sync(
                from: Self.rootPath.appending(".build/checkouts/soto/Sources/Soto/Services"),
                to: folder,
                delete: true
            )
            try await s3FileTransfer.sync(from: folder, to: folder2, delete: true)
            try await s3FileTransfer.sync(
                from: folder2.subFolder("DynamoDB"),
                to: tempFolder,
                delete: true
            )
            try await s3FileTransfer.sync(
                from: folder2.subFolder("S3"),
                to: tempFolder,
                delete: true
            ) { Logger.current.debug("\($0)") }
            let files = try await s3FileTransfer.listFiles(in: tempFolder)
            #expect(files.count == fileCount)
        }
    }
}

/// Equtable conformance required by tests
extension S3FileTransferManager.Error: Equatable {
    public static func == (
        _ lhs: S3FileTransferManager.Error,
        _ rhs: S3FileTransferManager.Error
    )
        -> Bool
    {
        switch (lhs, rhs) {
        case (.fileDoesNotExist(let lhs), .fileDoesNotExist(let rhs)):
            return lhs == rhs
        case (.s3FileDoesNotExist(let lhs), .s3FileDoesNotExist(let rhs)):
            return lhs == rhs
        case (.failedToCreateFolder(let lhs), .failedToCreateFolder(let rhs)):
            return lhs == rhs
        case (.failedToEnumerateFolder(let lhs), .failedToEnumerateFolder(let rhs)):
            return lhs == rhs
        case (.fileFolderClash(let lhs, let lhs2), .fileFolderClash(let rhs, let rhs2)):
            return lhs == rhs && lhs2 == rhs2
        case (.downloadFailed, .downloadFailed):
            return true
        case (.uploadFailed, .uploadFailed):
            return true
        case (.copyFailed, .copyFailed):
            return true
        default:
            return false
        }
    }
}
