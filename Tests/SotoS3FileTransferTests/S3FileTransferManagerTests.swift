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
import SotoS3
@testable import SotoS3FileTransfer
import XCTest

class S3TransferManagerXCTestCase: XCTestCase {
    static var client: AWSClient!
    static var s3: S3!
    static var s3FileTransfer: S3FileTransferManager!

    static var rootPath: String {
        return #file
            .split(separator: "/", omittingEmptySubsequences: false)
            .dropLast(3)
            .map { String(describing: $0) }
            .joined(separator: "/")
    }

    static var tempFolder: String {
        return rootPath.appending("/temp-folder")
    }

    override class func setUp() {
        if TestEnvironment.isUsingLocalstack {
            print("Connecting to Localstack")
        } else {
            print("Connecting to AWS")
        }
        self.client = AWSClient(
            credentialProvider: TestEnvironment.credentialProvider,
            middleware: TestEnvironment.middlewares,
            httpClientProvider: .createNew
        )
        self.s3 = S3(
            client: self.client,
            region: .euwest1,
            endpoint: TestEnvironment.getEndPoint(environment: "LOCALSTACK_ENDPOINT")
        ).with(timeout: .seconds(30))
        self.s3FileTransfer = .init(s3: self.s3, logger: Logger(label: "S3TransferTests"))
        XCTAssertNoThrow(try FileManager.default.createDirectory(atPath: self.tempFolder, withIntermediateDirectories: false))
    }

    override class func tearDown() {
        XCTAssertNoThrow(try self.client.syncShutdown())
        XCTAssertNoThrow(try FileManager.default.removeItem(atPath: self.tempFolder))
    }

    static func createBucket(name: String, s3: S3) async throws {
        let bucketRequest = S3.CreateBucketRequest(bucket: name)
        do {
            _ = try await s3.createBucket(bucketRequest, logger: TestEnvironment.logger)
            try await s3.waitUntilBucketExists(.init(bucket: name), logger: TestEnvironment.logger)
        } catch let error as S3ErrorType where error == .bucketAlreadyOwnedByYou {}
    }

    static func deleteBucket(name: String, s3: S3) async throws {
        let response = try await s3.listObjectsV2(.init(bucket: name), logger: TestEnvironment.logger)
        if let contents = response.contents {
            let request = S3.DeleteObjectsRequest(
                bucket: name,
                delete: S3.Delete(objects: contents.compactMap { $0.key.map { .init(key: $0) } })
            )
            _ = try await s3.deleteObjects(request, logger: TestEnvironment.logger)
        }
        try await s3.deleteBucket(.init(bucket: name), logger: TestEnvironment.logger)
    }

    /// create S3 bucket with supplied name and run supplied closure
    func testBucket(
        _ function: String = #function,
        s3: S3? = nil,
        test: @escaping (String) async throws -> Void
    ) async throws {
        let name = self.generateResourceName(function)
        let s3 = s3 ?? Self.s3!
        try await XCTTestAsset {
            try await Self.createBucket(name: name, s3: s3)
            return name
        } test: {
            try await test($0)
        } delete: { (name: String) in
            try await Self.deleteBucket(name: name, s3: s3)
        }
    }

    func generateResourceName(_ prefix: String = #function) -> String {
        let suffix = String(Int.random(in: Int.min..<Int.max), radix: 16)
        return "soto-" + (prefix + suffix).filter { $0.isLetter || $0.isNumber }.lowercased()
    }

    func createRandomBuffer(size: Int) -> Data {
        // create buffer
        var data = Data(count: size)
        for i in 0..<size {
            data[i] = UInt8.random(in: 0...255)
        }
        return data
    }
}

final class S3FileTransferManagerTests: S3TransferManagerXCTestCase {
    func testUploadDownload() async throws {
        let filename = "\(Self.tempFolder)/\(#function)"
        let filename2 = "\(Self.tempFolder)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 202_400)
        try buffer.write(to: URL(fileURLWithPath: filename))
        defer { try? FileManager.default.removeItem(atPath: filename) }

        try await testBucket { bucket in
            try await Self.s3FileTransfer.copy(
                from: filename,
                to: S3File(bucket: bucket, key: "testFile"),
                options: .init(metadata: ["test": "1,2,3"])
            ) { print($0) }
            try await Self.s3FileTransfer.copy(
                from: S3File(bucket: bucket, key: "testFile"),
                to: filename2
            ) { print($0) }

            defer { try? FileManager.default.removeItem(atPath: filename2) }

            let buffer2 = try Data(contentsOf: URL(fileURLWithPath: filename2))
            XCTAssertEqual(buffer, buffer2)
        }
    }

    func testS3Copy() async throws {
        let filename = "\(Self.tempFolder)/\(#function)"
        let filename2 = "\(Self.tempFolder)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 202_400)
        XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

        try await testBucket { bucket in
            try await Self.s3FileTransfer.copy(
                from: filename,
                to: S3File(bucket: bucket, key: "testFile")
            ) { print($0) }
            try await Self.s3FileTransfer.copy(
                from: S3File(bucket: bucket, key: "testFile"),
                to: S3File(bucket: bucket, key: "testFile2")
            )
            try await Self.s3FileTransfer.copy(
                from: S3File(bucket: bucket, key: "testFile2"),
                to: filename2
            ) { print($0) }

            defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

            var buffer2: Data?
            XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
            XCTAssertEqual(buffer, buffer2)
        }
    }

    func testS3UploadOfNonExistentFile() async throws {
        let filename = "\(Self.tempFolder)/\(#function)"
        try await testBucket { bucket in
            await XCTAsyncExpectError(S3FileTransferManager.Error.fileDoesNotExist(filename)) {
                _ = try await Self.s3FileTransfer.copy(from: filename, to: S3File(bucket: bucket, key: "doesNotExist"))
            }
        }
    }

    func testS3DownloadOfNonExistentFile() async throws {
        let filename = "\(Self.tempFolder)/\(#function)"
        try await testBucket { bucket in
            await XCTAsyncExpectError(S3FileTransferManager.Error.fileDoesNotExist("s3://\(bucket)/doesNotExist")) {
                _ = try await Self.s3FileTransfer.copy(from: S3File(bucket: bucket, key: "doesNotExist"), to: filename)
            }
        }
    }

    func testS3CopyOfNonExistentFile() async throws {
        try await testBucket { bucket in
            await XCTAsyncExpectError(S3FileTransferManager.Error.fileDoesNotExist("s3://\(bucket)/doesNotExist")) {
                _ = try await Self.s3FileTransfer.copy(
                    from: S3File(bucket: bucket, key: "doesNotExist"),
                    to: S3File(bucket: bucket, key: "destination")
                )
            }
        }
    }

    func testListFiles() async throws {
        let files = try await Self.s3FileTransfer.listFiles(in: Self.rootPath)
        XCTAssertNotNil(files.firstIndex { $0.name == "\(Self.rootPath)/Tests/SotoS3FileTransferTests/S3FileTransferManagerTests.swift" })
        XCTAssertNil(files.firstIndex { $0.name == "\(Self.rootPath)/Tests/SotoS3FileTransferTests" })
    }

    func testListS3Files() async throws {
        let test = "testListS3Files"
        try await testBucket { bucket in
            _ = try await Self.s3.putObject(.init(body: .init(string: test), bucket: bucket, key: "testListS3Files/2.txt"))
            _ = try await Self.s3.putObject(.init(body: .init(string: test), bucket: bucket, key: "testListS3Files2/1.txt"))

            let files = try await Self.s3FileTransfer.listFiles(in: S3Folder(url: "s3://\(bucket)/testListS3Files")!)
            XCTAssertNotNil(files.firstIndex { $0.file.key == "testListS3Files/2.txt" })
            XCTAssertNil(files.firstIndex { $0.file.key == "testListS3Files2/1.txt" })
        }
    }

    /// test the list of target files is calculated correctly
    func testTargetFiles() async throws {
        let files: [S3FileTransferManager.FileDescriptor] = [
            .init(name: "/User/JohnSmith/Documents/test.doc", modificationDate: Date(), size: 0),
            .init(name: "/User/JohnSmith/Documents/hello.doc", modificationDate: Date(), size: 0)
        ]
        let s3Files = S3FileTransferManager.targetFiles(files: files, from: "/User/JohnSmith/Documents/", to: S3Folder(url: "s3://my-bucket/")!)
        XCTAssertEqual(s3Files[0].to.key, "test.doc")
        XCTAssertEqual(s3Files[1].to.key, "hello.doc")

        let s3Files2 = S3FileTransferManager.targetFiles(files: files, from: "/User/JohnSmith", to: S3Folder(url: "s3://my-bucket/test")!)
        XCTAssertEqual(s3Files2[0].to.key, "test/Documents/test.doc")
        XCTAssertEqual(s3Files2[1].to.key, "test/Documents/hello.doc")
    }

    /// test the list of s3 target files is calculated correctly
    func testS3TargetFiles() async throws {
        let s3Files: [S3FileTransferManager.S3FileDescriptor] = [
            .init(file: S3File(url: "s3://my-bucket/User/JohnSmith/Documents/test.doc")!, modificationDate: Date(), size: 1024),
            .init(file: S3File(url: "s3://my-bucket/User/JohnSmith/Documents/hello.doc")!, modificationDate: Date(), size: 2000)
        ]
        let files = S3FileTransferManager.targetFiles(files: s3Files, from: S3Folder(url: "s3://my-bucket")!, to: "/User/JohnSmith/Downloads")
        XCTAssertEqual(files[0].to, "/User/JohnSmith/Downloads/User/JohnSmith/Documents/test.doc")
        XCTAssertEqual(files[1].to, "/User/JohnSmith/Downloads/User/JohnSmith/Documents/hello.doc")

        let files2 = S3FileTransferManager.targetFiles(files: s3Files, from: S3Folder(url: "s3://my-bucket/User/JohnSmith")!, to: "/User/JohnSmith/Downloads")
        XCTAssertEqual(files2[0].to, "/User/JohnSmith/Downloads/Documents/test.doc")
        XCTAssertEqual(files2[1].to, "/User/JohnSmith/Downloads/Documents/hello.doc")
    }

    func testCopyPathLocalToS3() async throws {
        try await testBucket { bucket in
            let folder = S3Folder(bucket: bucket, key: "testCopyPathLocalToS3")
            try await Self.s3FileTransfer.copy(from: Self.rootPath, to: folder)
            let files = try await Self.s3FileTransfer.listFiles(in: folder)
            XCTAssertNotNil(files.first(where: { $0.file.key == "testCopyPathLocalToS3/Sources/SotoS3FileTransfer/S3Path.swift" }))
        }
    }

    func testS3toS3CopyPath() async throws {
        try await testBucket { bucket in
            let folder1 = S3Folder(bucket: bucket, key: "testS3toS3CopyPath")
            let folder2 = S3Folder(bucket: bucket, key: "testS3toS3CopyPath_Copy")
            try await Self.s3FileTransfer.copy(from: Self.rootPath + "/Sources", to: folder1)
            try await Self.s3FileTransfer.copy(from: folder1, to: folder2)
            let files = try await Self.s3FileTransfer.listFiles(in: folder2)
            XCTAssertNotNil(files.first(where: { $0.file.key == "testS3toS3CopyPath_Copy/SotoS3FileTransfer/S3FileTransferManager.swift" }))
        }
    }

    func testSyncPathLocalToS3() async throws {
        try await testBucket { bucket in
            try await Self.s3FileTransfer.sync(
                from: Self.rootPath + "/Tests",
                to: S3Folder(bucket: bucket, key: "testSyncPathLocalToS3"),
                delete: true
            )
            try await Self.s3FileTransfer.sync(
                from: S3Folder(bucket: bucket, key: "testSyncPathLocalToS3"),
                to: S3Folder(bucket: bucket, key: "testSyncPathLocalToS3_v2"),
                delete: true
            )
            try await Self.s3FileTransfer.sync(
                from: S3Folder(bucket: bucket, key: "testSyncPathLocalToS3_v2"),
                to: Self.tempFolder + "/Tests2",
                delete: true,
                progress: { print($0) }
            )

            let files = try await Self.s3FileTransfer.listFiles(in: Self.tempFolder + "/Tests2")
            XCTAssertNotNil(files.first(where: { $0.name == "\(Self.tempFolder)/Tests2/SotoS3FileTransferTests/S3PathTests.swift" }))
            // remove folder
            XCTAssertNoThrow(try FileManager.default.removeItem(atPath: Self.tempFolder + "/Tests2"))
        }
    }

    /// Test cancelling download and resuming it
    ///
    /// Used by testCancelledSyncWithCancel and testCancelledSyncWithFlush
    func testCancelledSync(_ s3FileTransfer: S3FileTransferManager, bucket: String, folderName: String) async throws {
        let folder = S3Folder(url: "s3://\(bucket)/\(folderName)")!
        let localFolder = "\(Self.tempFolder)/\(#function)"
        let originalFiles = try await s3FileTransfer.listFiles(in: Self.rootPath + "/Sources")
        XCTAssertNotEqual(originalFiles.count, 0)

        // copy files to S3
        try await s3FileTransfer.sync(from: Self.rootPath + "/Sources", to: folder, delete: true)
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
                XCTAssertNotEqual(localFiles.map { $0.size }.reduce(0, +), originalFiles.map { $0.size }.reduce(0, +))
            }
            let error = try XCTUnwrap(error as? S3FileTransferManager.Error)
            if case .downloadFailed(_, let download) = error {
                try await s3FileTransfer.resume(download: download)
            }
        }
        let localFiles = try await s3FileTransfer.listFiles(in: localFolder)
        // check total file size for folder is different
        XCTAssertEqual(localFiles.map { $0.size }.reduce(0, +), originalFiles.map { $0.size }.reduce(0, +))
    }

    /// Test cancelling download with cancelOnError set to true
    func testCancelledSyncWithCancel() async throws {
        try await testBucket { bucket in
            let s3FileTransfer = S3FileTransferManager(
                s3: Self.s3,
                configuration: .init(cancelOnError: true, maxConcurrentTasks: 2),
                logger: Logger(label: "S3TransferTests")
            )
            try await self.testCancelledSync(s3FileTransfer, bucket: bucket, folderName: "testCancelledSyncWithCancel")
        }
    }

    /// Test cancelling download with cancelOnError set to false
    func testCancelledSyncWithFlush() async throws {
        try await testBucket { bucket in
            let s3FileTransfer = S3FileTransferManager(
                s3: Self.s3,
                configuration: .init(cancelOnError: false, maxConcurrentTasks: 2),
                logger: Logger(label: "S3TransferTests")
            )
            try await self.testCancelledSync(s3FileTransfer, bucket: bucket, folderName: "testCancelledSyncWithFlush")
        }
    }

    /// Used by testCancelledDownloadWithCancel
    func testCancelledDownload(_ s3FileTransfer: S3FileTransferManager, bucket: String) async throws {
        let folder = S3Folder(url: "s3://\(bucket)/testCancelledDownload")!
        let localFolder = "\(Self.tempFolder)/\(#function)"
        let originalFiles = try await s3FileTransfer.listFiles(in: Self.rootPath + "/Sources")
        XCTAssertNotEqual(originalFiles.count, 0)

        try await s3FileTransfer.copy(from: Self.rootPath + "/Sources", to: folder)
        do {
            let cancelled = ManagedAtomic(false)
            try await s3FileTransfer.copy(from: folder, to: localFolder) { progress in
                print(progress)
                // make sure we only cancel one task
                if progress > 0.2, cancelled.exchange(true, ordering: .relaxed) == false {
                    throw CancellationError()
                }
            }
        } catch {
            let localFiles = try await s3FileTransfer.listFiles(in: localFolder)
            // check total file size for folder is different
            XCTAssertNotEqual(localFiles.map { $0.size }.reduce(0, +), originalFiles.map { $0.size }.reduce(0, +))
            let error = try XCTUnwrap(error as? S3FileTransferManager.Error)
            if case .downloadFailed(_, let download) = error {
                try? await s3FileTransfer.resume(download: download)
            }
        }
        let localFiles = try await s3FileTransfer.listFiles(in: localFolder)
        // check total file size for folder is different
        XCTAssertEqual(localFiles.map { $0.size }.reduce(0, +), originalFiles.map { $0.size }.reduce(0, +))
    }

    /// Test cancelling download with cancelOnError set to true
    func testCancelledDownloadWithCancel() async throws {
        try await testBucket { bucket in
            let s3FileTransfer = S3FileTransferManager(
                s3: Self.s3,
                configuration: .init(cancelOnError: true, maxConcurrentTasks: 4),
                logger: Logger(label: "S3TransferTests")
            )
            try await self.testCancelledDownload(s3FileTransfer, bucket: bucket)
        }
    }

    /// Test deleting a folder works
    func testDeleteFolder() async throws {
        try await testBucket { bucket in
            let folder = S3Folder(url: "s3://\(bucket)/testDeleteFolder")!
            try await Self.s3FileTransfer.sync(from: Self.rootPath + "/Tests", to: folder, delete: true)
            let files = try await Self.s3FileTransfer.listFiles(in: folder)
            XCTAssertTrue(files.count > 0)
            try await Self.s3FileTransfer.delete(folder)
            let files2 = try await Self.s3FileTransfer.listFiles(in: folder)
            XCTAssertEqual(files2.count, 0)
        }
    }

    /// Test sync'ing folder, deleting subfolder and then sync'ing back to client. This uses source code
    /// as the data, so if source code changes structure it may start failing
    func testDeleteFolderAndSync() async throws {
        try await testBucket { bucket in
            let folder = S3Folder(url: "s3://\(bucket)/testDeleteFolderAndSync")!
            let subfolder = folder.subFolder("_subFolder")
            let localFolder = "\(Self.tempFolder)/\(#function)"
            let originalFiles = try await Self.s3FileTransfer.listFiles(in: Self.rootPath + "/Sources")
            XCTAssertNotEqual(originalFiles.count, 0)

            try await Self.s3FileTransfer.sync(from: Self.rootPath + "/Sources", to: folder, delete: true)
            try await Self.s3FileTransfer.copy(from: folder, to: subfolder)
            try await Self.s3FileTransfer.sync(from: folder, to: localFolder, delete: true)
            var localFiles = try await Self.s3FileTransfer.listFiles(in: localFolder)
            XCTAssertEqual(localFiles.count, originalFiles.count * 2)

            try await Self.s3FileTransfer.delete(subfolder)

            try await Self.s3FileTransfer.sync(from: folder, to: localFolder, delete: true)
            localFiles = try await Self.s3FileTransfer.listFiles(in: localFolder)
            XCTAssertEqual(localFiles.count, originalFiles.count)
        }
    }

    /// test we get an error when trying to download a folder on top of a file
    func testDownloadFileToFolder() async throws {
        try await testBucket { bucket in
            let filename = "\(Self.tempFolder)/\(#function)"
            let s3File = S3File(bucket: bucket, key: "testDownload/test.dat")
            let buffer = self.createRandomBuffer(size: 202_400)
            XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
            defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }
            try await Self.s3FileTransfer.copy(from: filename, to: s3File)
            try await Self.s3FileTransfer.copy(from: s3File, to: Self.tempFolder)
            XCTAssertNoThrow(try FileManager.default.removeItem(atPath: "\(Self.tempFolder)/test.dat"))
        }
    }

    /// test we get an error when trying to download a folder on top of a file
    func testDownloadFolderToFile() async throws {
        try await testBucket { bucket in
            let folder1 = S3Folder(bucket: bucket, key: "testDownloadFolderToFile")
            try await Self.s3FileTransfer.copy(from: Self.rootPath + "/Sources", to: folder1)
            do {
                _ = try await Self.s3FileTransfer.copy(from: folder1, to: "\(Self.rootPath)/Package.swift")
                XCTFail("Was expected to throw an error but it didn't")
            } catch let error as S3FileTransferManager.Error {
                if case .downloadFailed = error {
                    // XCTAssertEqual((error as NSError).code, 512)
                } else {
                    XCTFail("\(error)")
                }
            } catch {
                XCTFail("Expected error S3FileTransferManager.Error.downloadFailed but got \(error)")
            }
        }
    }

    /// check the correct error is thrown when trying to download a file on top of a folder
    func testFileFolderClash() async throws {
        try await testBucket { bucket in
            let folder = S3Folder(bucket: bucket, key: "testFileFolderClash")
            _ = try await Self.s3.putObject(.init(body: .init(string: "folder"), bucket: bucket, key: "testFileFolderClash/fold"))
            _ = try await Self.s3.putObject(.init(body: .init(string: "folder"), bucket: bucket, key: "testFileFolderClash/folder"))
            _ = try await Self.s3.putObject(.init(body: .init(string: "folder"), bucket: bucket, key: "testFileFolderClash/folder*"))
            _ = try await Self.s3.putObject(.init(body: .init(string: "file"), bucket: bucket, key: "testFileFolderClash/folder/file"))
            _ = try await Self.s3.putObject(.init(body: .init(string: "file"), bucket: bucket, key: "testFileFolderClash/folder/file2"))

            await XCTAsyncExpectError(
                S3FileTransferManager.Error.fileFolderClash("testFileFolderClash/folder", "testFileFolderClash/folder/file")
            ) {
                try await Self.s3FileTransfer.copy(from: folder, to: Self.tempFolder)
            }
        }
    }

    /// check no error is thrown when trying to download a file with the same prefix
    func testIgnoreFileFolderClash() async throws {
        try await testBucket { bucket in

            let folder = S3Folder(bucket: bucket, key: "testIgnoreFileFolderClash")
            _ = try await Self.s3.putObject(.init(body: .init(string: "folder"), bucket: bucket, key: "testIgnoreFileFolderClash/fold"))
            _ = try await Self.s3.putObject(.init(body: .init(string: "folder"), bucket: bucket, key: "testIgnoreFileFolderClash/folder"))
            _ = try await Self.s3.putObject(.init(body: .init(string: "folder"), bucket: bucket, key: "testIgnoreFileFolderClash/folder*"))
            _ = try await Self.s3.putObject(.init(body: .init(string: "file"), bucket: bucket, key: "testIgnoreFileFolderClash/folder/file"))
            _ = try await Self.s3.putObject(.init(body: .init(string: "file"), bucket: bucket, key: "testIgnoreFileFolderClash/folder/file2"))
            try await Self.s3FileTransfer.copy(
                from: folder,
                to: Self.tempFolder + "/testIgnoreFileFolderClash",
                options: .init(ignoreFileFolderClashes: true)
            )
            let files = try await Self.s3FileTransfer.listFiles(in: Self.tempFolder + "/testIgnoreFileFolderClash")
            XCTAssertEqual(files.count, 4)
        }
    }

    func testMultipartUploadDownload() async throws {
        try await testBucket { bucket in
            let filename = "\(Self.tempFolder)/\(#function)"
            let filename2 = "\(Self.tempFolder)/\(#function)2"
            let buffer = self.createRandomBuffer(size: 10_202_400)
            XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
            defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

            try await Self.s3FileTransfer.copy(
                from: filename,
                to: S3File(bucket: bucket, key: "testMultipartUploadDownload")
            ) { print($0) }
            try await Self.s3FileTransfer.copy(
                from: S3File(bucket: bucket, key: "testMultipartUploadDownload"),
                to: filename2
            ) { print($0) }

            defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

            var buffer2: Data?
            XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
            XCTAssertEqual(buffer, buffer2)
        }
    }

    func testS3MultipartCopy() async throws {
        try await testBucket { bucket in

            let filename = "\(Self.tempFolder)/\(#function)"
            let filename2 = "\(Self.tempFolder)/\(#function)2"
            let buffer = self.createRandomBuffer(size: 10_202_400)
            XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
            defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

            try await Self.s3FileTransfer.copy(
                from: filename,
                to: S3File(bucket: bucket, key: "testS3MultipartCopy")
            ) { print($0) }
            try await Self.s3FileTransfer.copy(
                from: S3File(bucket: bucket, key: "testS3MultipartCopy"),
                to: S3File(bucket: bucket, key: "testS3MultipartCopy_Copy")
            )
            try await Self.s3FileTransfer.copy(
                from: S3File(bucket: bucket, key: "testS3MultipartCopy_Copy"),
                to: filename2
            ) { print($0) }

            defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

            var buffer2: Data?
            XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
            XCTAssertEqual(buffer, buffer2)
        }
    }

    func testBigFolderUpload() async throws {
        try await testBucket { bucket in

            let folder = S3Folder(bucket: bucket, key: "testBigFolderUpload")
            let folder2 = S3Folder(bucket: bucket, key: "testBigFolderUpload_Copy")
            let tempFolder = Self.tempFolder + "/testBigFolderUpload"
            let fileCount = try await Self.s3FileTransfer.listFiles(in: "\(Self.rootPath)/.build/checkouts/soto/Sources/Soto/Services/S3/").count
            XCTAssertNotNil(fileCount)
            try await Self.s3FileTransfer.sync(
                from: "\(Self.rootPath)/.build/checkouts/soto/Sources/Soto/Services",
                to: folder,
                delete: true,
                progress: { print($0) }
            )
            try await Self.s3FileTransfer.sync(from: folder, to: folder2, delete: true)
            try await Self.s3FileTransfer.sync(from: folder2.subFolder("DynamoDB"), to: tempFolder, delete: true)
            try await Self.s3FileTransfer.sync(from: folder2.subFolder("S3"), to: tempFolder, delete: true) { print($0) }
            let files = try await Self.s3FileTransfer.listFiles(in: tempFolder)
            XCTAssertEqual(files.count, fileCount)
        }
    }
}

/// Equtable conformance required by tests
extension S3FileTransferManager.Error: Equatable {
    public static func == (_ lhs: S3FileTransferManager.Error, _ rhs: S3FileTransferManager.Error) -> Bool {
        switch (lhs, rhs) {
        case (.fileDoesNotExist(let lhs), .fileDoesNotExist(let rhs)):
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
