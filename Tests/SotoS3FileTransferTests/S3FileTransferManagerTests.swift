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

import SotoS3
@testable import SotoS3FileTransfer
import XCTest

final class S3FileTransferManagerTests: XCTestCase {
    static let bucketName = "soto-transfermanagertests"
    static var client: AWSClient!
    static var s3: S3!
    static var s3FileTransfer: S3FileTransferManager!

    override class func setUp() {
        self.client = AWSClient(httpClientProvider: .createNew)
        self.s3 = S3(client: self.client, region: .euwest1) // .with(middlewares: [AWSLoggingMiddleware()])
        self.s3FileTransfer = .init(s3: self.s3, threadPoolProvider: .createNew, logger: Logger(label: "S3TransferTests"))

        XCTAssertNoThrow(try FileManager.default.createDirectory(atPath: tempFolder, withIntermediateDirectories: false))
        XCTAssertNoThrow(try self.s3.createBucket(.init(bucket: self.bucketName)).wait())
    }

    override class func tearDown() {
        // delete contents of bucket and then the bucket
        let request = S3.ListObjectsV2Request(bucket: self.bucketName)
        let response = self.s3.listObjectsV2Paginator(request, []) { result, response, eventLoop in
            let newResult: [S3.Object] = result + (response.contents ?? [])
            return eventLoop.makeSucceededFuture((true, newResult))
        }
        .flatMap { (objects: [S3.Object]) -> EventLoopFuture<Void> in
            let eventLoop = s3.client.eventLoopGroup.next()
            let taskQueue = TaskQueue<S3.DeleteObjectOutput>(maxConcurrentTasks: 8, on: eventLoop)
            let deleteFutureResults = objects.compactMap { $0.key.map { key in taskQueue.submitTask { s3.deleteObject(.init(bucket: bucketName, key: key)) } } }
            return EventLoopFuture.andAllSucceed(deleteFutureResults, on: eventLoop)
        }
        .flatMap { _ -> EventLoopFuture<Void> in
            let request = S3.DeleteBucketRequest(bucket: bucketName)
            return s3.deleteBucket(request).map { _ in }
        }
        XCTAssertNoThrow(try response.wait())
        XCTAssertNoThrow(try self.client.syncShutdown())
        XCTAssertNoThrow(try FileManager.default.removeItem(atPath: tempFolder))

    }

    func testUploadDownload() {
        let filename = "\(Self.tempFolder)/\(#function)"
        let filename2 = "\(Self.tempFolder)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 202_400)
        XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: filename, to: S3File(bucket: Self.bucketName, key: "testFile"), options: .init(metadata: ["test": "1,2,3"])) { print($0) }.wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: S3File(bucket: Self.bucketName, key: "testFile"), to: filename2) { print($0) }.wait())

        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

        var buffer2: Data?
        XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
        XCTAssertEqual(buffer, buffer2)
    }

    func testMultipartUploadDownload() {
        let filename = "\(Self.tempFolder)/\(#function)"
        let filename2 = "\(Self.tempFolder)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 10_202_400)
        XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: filename, to: S3File(bucket: Self.bucketName, key: "testMultipartUploadDownload")) { print($0) }.wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: S3File(bucket: Self.bucketName, key: "testMultipartUploadDownload"), to: filename2) { print($0) }.wait())

        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

        var buffer2: Data?
        XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
        XCTAssertEqual(buffer, buffer2)
    }

    func testS3Copy() {
        let filename = "\(Self.tempFolder)/\(#function)"
        let filename2 = "\(Self.tempFolder)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 202_400)
        XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: filename, to: S3File(bucket: Self.bucketName, key: "testFile")) { print($0) }.wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: S3File(bucket: Self.bucketName, key: "testFile"), to: S3File(bucket: Self.bucketName, key: "testFile2")).wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: S3File(bucket: Self.bucketName, key: "testFile2"), to: filename2) { print($0) }.wait())

        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

        var buffer2: Data?
        XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
        XCTAssertEqual(buffer, buffer2)
    }

    func testS3MultipartCopy() {
        let filename = "\(Self.tempFolder)/\(#function)"
        let filename2 = "\(Self.tempFolder)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 10_202_400)
        XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: filename, to: S3File(bucket: Self.bucketName, key: "testS3MultipartCopy")) { print($0) }.wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: S3File(bucket: Self.bucketName, key: "testS3MultipartCopy"), to: S3File(bucket: Self.bucketName, key: "testS3MultipartCopy_Copy")).wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: S3File(bucket: Self.bucketName, key: "testS3MultipartCopy_Copy"), to: filename2) { print($0) }.wait())

        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

        var buffer2: Data?
        XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
        XCTAssertEqual(buffer, buffer2)
    }

    func testListFiles() {
        var files: [S3FileTransferManager.FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3FileTransfer.listFiles(in: Self.rootPath).wait())
        XCTAssertNotNil(files?.firstIndex { $0.name == "\(Self.rootPath)/Tests/SotoS3FileTransferTests/S3FileTransferManagerTests.swift" })
        XCTAssertNil(files?.firstIndex { $0.name == "\(Self.rootPath)/Tests/SotoS3FileTransferTests" })
    }

    func testListS3Files() {
        var files: [S3FileTransferManager.S3FileDescriptor]?
        let test = "testListS3Files"
        XCTAssertNoThrow(try Self.s3.putObject(.init(body: .string(test), bucket: Self.bucketName, key: "testListS3Files/2.txt")).wait())
        XCTAssertNoThrow(try Self.s3.putObject(.init(body: .string(test), bucket: Self.bucketName, key: "testListS3Files2/1.txt")).wait())

        XCTAssertNoThrow(files = try Self.s3FileTransfer.listFiles(in: S3Folder(url: "s3://\(Self.bucketName)/testListS3Files")!).wait())
        XCTAssertNotNil(files?.firstIndex { $0.file.key == "testListS3Files/2.txt" })
        XCTAssertNil(files?.firstIndex { $0.file.key == "testListS3Files2/1.txt" })
    }

    /// test the list of target files is calculated correctly
    func testTargetFiles() {
        let files: [S3FileTransferManager.FileDescriptor] = [
            .init(name: "/User/JohnSmith/Documents/test.doc", modificationDate: Date()),
            .init(name: "/User/JohnSmith/Documents/hello.doc", modificationDate: Date())
        ]
        let s3Files = S3FileTransferManager.targetFiles(files: files, from: "/User/JohnSmith/Documents/", to: S3Folder(url: "s3://my-bucket/")!)
        XCTAssertEqual(s3Files[0].to.key, "test.doc")
        XCTAssertEqual(s3Files[1].to.key, "hello.doc")

        let s3Files2 = S3FileTransferManager.targetFiles(files: files, from: "/User/JohnSmith", to: S3Folder(url: "s3://my-bucket/test")!)
        XCTAssertEqual(s3Files2[0].to.key, "test/Documents/test.doc")
        XCTAssertEqual(s3Files2[1].to.key, "test/Documents/hello.doc")
    }

    /// test the list of s3 target files is calculated correctly
    func testS3TargetFiles() {
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

    func testCopyPathLocalToS3() {
        let folder = S3Folder(bucket: Self.bucketName, key: "testCopyPathLocalToS3")
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: Self.rootPath, to: folder).wait())
        var files: [S3FileTransferManager.S3FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3FileTransfer.listFiles(in: folder).wait())
        XCTAssertNotNil(files?.first(where: { $0.file.key == "testCopyPathLocalToS3/Sources/SotoS3FileTransfer/S3Path.swift" }))
    }

    func testS3toS3CopyPath() {
        let folder1 = S3Folder(bucket: Self.bucketName, key: "testS3toS3CopyPath")
        let folder2 = S3Folder(bucket: Self.bucketName, key: "testS3toS3CopyPath_Copy")
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: Self.rootPath + "/Sources", to: folder1).wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: folder1, to: folder2).wait())
        var files: [S3FileTransferManager.S3FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3FileTransfer.listFiles(in: folder2).wait())
        XCTAssertNotNil(files?.first(where: { $0.file.key == "testS3toS3CopyPath_Copy/SotoS3FileTransfer/S3FileTransferManager.swift" }))
    }

    func testSyncPathLocalToS3() {
        XCTAssertNoThrow(try Self.s3FileTransfer.sync(from: Self.rootPath + "/Tests", to: S3Folder(bucket: Self.bucketName, key: "testSyncPathLocalToS3"), delete: true).wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.sync(from: S3Folder(bucket: Self.bucketName, key: "testSyncPathLocalToS3"), to: S3Folder(bucket: Self.bucketName, key: "testSyncPathLocalToS3_v2"), delete: true).wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.sync(from: S3Folder(bucket: Self.bucketName, key: "testSyncPathLocalToS3_v2"), to: Self.tempFolder + "/Tests2", delete: true).wait())

        var files: [S3FileTransferManager.FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3FileTransfer.listFiles(in: Self.tempFolder + "/Tests2").wait())
        XCTAssertNotNil(files?.first(where: { $0.name == "\(Self.tempFolder)/Tests2/SotoS3FileTransferTests/S3PathTests.swift" }))
        // remove folder
        XCTAssertNoThrow(try FileManager.default.removeItem(atPath: Self.tempFolder + "/Tests2"))
    }

    func testDeleteFolder() {
        let folder = S3Folder(url: "s3://\(Self.bucketName)/testDeleteFolder")!
        XCTAssertNoThrow(try Self.s3FileTransfer.sync(from: Self.rootPath + "/Tests", to: folder, delete: true).wait())
        var files: [S3FileTransferManager.S3FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3FileTransfer.listFiles(in: folder).wait())
        XCTAssertTrue(files!.count > 0)
        XCTAssertNoThrow(try Self.s3FileTransfer.delete(folder).wait())
        XCTAssertNoThrow(files = try Self.s3FileTransfer.listFiles(in: folder).wait())
        XCTAssertEqual(files?.count, 0)
    }

    func testBigFolderUpload() {
        let folder = S3Folder(bucket: Self.bucketName, key: "testBigFolderUpload")
        let folder2 = S3Folder(bucket: Self.bucketName, key: "testBigFolderUpload_Copy")
        let tempFolder = Self.tempFolder + "/testBigFolderUpload"
        var fileCount: Int?
        XCTAssertNoThrow(fileCount = try Self.s3FileTransfer.listFiles(in: "\(Self.rootPath)/.build/checkouts/soto/Sources/Soto/Services/S3/").wait().count)
        XCTAssertNotNil(fileCount)
        XCTAssertNoThrow(try Self.s3FileTransfer.sync(from: "\(Self.rootPath)/.build/checkouts/soto/Sources/Soto/Services", to: folder, delete: true).wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.sync(from: folder, to: folder2, delete: true).wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.sync(from: folder2.subFolder("DynamoDB"), to: tempFolder, delete: true).wait())
        XCTAssertNoThrow(try Self.s3FileTransfer.sync(from: folder2.subFolder("S3"), to: tempFolder, delete: true).wait())
        var files: [S3FileTransferManager.FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3FileTransfer.listFiles(in: tempFolder).wait())
        XCTAssertEqual(files?.count, fileCount)
    }

    /// test we get an error when trying to download a folder on top of a file
    func testDownloadFileToFolder() {
        let filename = "\(Self.tempFolder)/\(#function)"
        let s3File = S3File(bucket: Self.bucketName, key: "testDownload/test.dat")
        let buffer = self.createRandomBuffer(size: 202_400)
        do {
            XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
            defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }
            XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: filename, to: s3File).wait())
        }
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: s3File, to: Self.tempFolder).wait())
        XCTAssertNoThrow(try FileManager.default.removeItem(atPath: "\(Self.tempFolder)/test.dat"))
    }

    /// test we get an error when trying to download a folder on top of a file
    func testDownloadFolderToFile() {
        let folder1 = S3Folder(bucket: Self.bucketName, key: "testDownloadFolderToFile")
        XCTAssertNoThrow(try Self.s3FileTransfer.copy(from: Self.rootPath + "/Sources", to: folder1).wait())
        XCTAssertThrowsError(try Self.s3FileTransfer.copy(from: folder1, to: "\(Self.rootPath)/Package.swift").wait()) { error in
            switch error {
            case let error as NSError:
                XCTAssertEqual(error.code, 512)
            default:
                XCTFail("\(error)")
            }
        }
    }

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

    func createRandomBuffer(size: Int) -> Data {
        // create buffer
        var data = Data(count: size)
        for i in 0..<size {
            data[i] = UInt8.random(in: 0...255)
        }
        return data
    }
}
