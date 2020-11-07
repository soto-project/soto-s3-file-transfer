//===----------------------------------------------------------------------===//
//
// This source file is part of the Soto for AWS open source project
//
// Copyright (c) 2017-2020 the Soto project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Soto project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

import SotoS3
@testable import SotoS3Transfer
import XCTest

final class TransferManagerTests: XCTestCase {
    static let bucketName = "soto-transfermanagertests"
    static var client: AWSClient!
    static var s3: S3!
    static var s3Transfer: S3TransferManager!

    override class func setUp() {
        self.client = AWSClient(httpClientProvider: .createNew)
        self.s3 = S3(client: self.client, region: .euwest1) // .with(middlewares: [AWSLoggingMiddleware()])
        self.s3Transfer = .init(s3: self.s3, threadPoolProvider: .createNew, logger: Logger(label: "S3TransferTests"))

        XCTAssertNoThrow(try self.s3.createBucket(.init(bucket: self.bucketName)).wait())
    }

    override class func tearDown() {
        // delete contents of bucket and then the bucket
        let request = S3.ListObjectsV2Request(bucket: self.bucketName)
        let response = self.s3.listObjectsV2(request)
            .flatMap { response -> EventLoopFuture<Void> in
                let eventLoop = s3.client.eventLoopGroup.next()
                guard let objects = response.contents else { return eventLoop.makeSucceededFuture(()) }
                let deleteFutureResults = objects.compactMap { $0.key.map { s3.deleteObject(.init(bucket: bucketName, key: $0)) } }
                return EventLoopFuture.andAllSucceed(deleteFutureResults, on: eventLoop)
            }
            .flatMap { _ -> EventLoopFuture<Void> in
                let request = S3.DeleteBucketRequest(bucket: bucketName)
                return s3.deleteBucket(request).map { _ in }
            }
        XCTAssertNoThrow(try response.wait())
        XCTAssertNoThrow(try self.client.syncShutdown())
    }

    func testUploadDownload() {
        let filename = "\(rootPath)/\(#function)"
        let filename2 = "\(rootPath)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 202_400)
        XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

        XCTAssertNoThrow(try Self.s3Transfer.copy(from: filename, to: S3File(bucket: Self.bucketName, path: "testFile"), options: .init(metadata: ["test": "1,2,3"])) { print($0) }.wait())
        XCTAssertNoThrow(try Self.s3Transfer.copy(from: S3File(bucket: Self.bucketName, path: "testFile"), to: filename2) { print($0) }.wait())

        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

        var buffer2: Data?
        XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
        XCTAssertEqual(buffer, buffer2)
    }

    func testMultipartUploadDownload() {
        let filename = "\(rootPath)/\(#function)"
        let filename2 = "\(rootPath)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 10_202_400)
        XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

        XCTAssertNoThrow(try Self.s3Transfer.copy(from: filename, to: S3File(bucket: Self.bucketName, path: "testMultipartUploadDownload")) { print($0) }.wait())
        XCTAssertNoThrow(try Self.s3Transfer.copy(from: S3File(bucket: Self.bucketName, path: "testMultipartUploadDownload"), to: filename2) { print($0) }.wait())

        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

        var buffer2: Data?
        XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
        XCTAssertEqual(buffer, buffer2)
    }

    func testS3Copy() {
        let filename = "\(rootPath)/\(#function)"
        let filename2 = "\(rootPath)/\(#function)2"
        let buffer = self.createRandomBuffer(size: 202_400)
        XCTAssertNoThrow(try buffer.write(to: URL(fileURLWithPath: filename)))
        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename)) }

        XCTAssertNoThrow(try Self.s3Transfer.copy(from: filename, to: S3File(bucket: Self.bucketName, path: "testFile")) { print($0) }.wait())
        XCTAssertNoThrow(try Self.s3Transfer.copy(from: S3File(bucket: Self.bucketName, path: "testFile"), to: S3File(bucket: Self.bucketName, path: "testFile2")) { print($0) }.wait())
        XCTAssertNoThrow(try Self.s3Transfer.copy(from: S3File(bucket: Self.bucketName, path: "testFile2"), to: filename2) { print($0) }.wait())

        defer { XCTAssertNoThrow(try FileManager.default.removeItem(atPath: filename2)) }

        var buffer2: Data?
        XCTAssertNoThrow(try buffer2 = Data(contentsOf: URL(fileURLWithPath: filename2)))
        XCTAssertEqual(buffer, buffer2)
    }

    func testListFiles() {
        var files: [S3TransferManager.FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3Transfer.listFiles(in: self.rootPath).wait())
        XCTAssertNotNil(files?.firstIndex { $0.name == "\(rootPath)/Tests/SotoS3TransferTests/S3TransferManagerTests.swift" })
        XCTAssertNil(files?.firstIndex { $0.name == "\(rootPath)/Tests/SotoS3TransferTests" })
    }

    func testListS3Files() {
        var files: [S3TransferManager.S3FileDescriptor]?
        let test = "testListS3Files"
        XCTAssertNoThrow(try Self.s3.putObject(.init(body: .string(test), bucket: Self.bucketName, key: "testListS3Files/2.txt")).wait())
        XCTAssertNoThrow(try Self.s3.putObject(.init(body: .string(test), bucket: Self.bucketName, key: "testListS3Files2/1.txt")).wait())

        XCTAssertNoThrow(files = try Self.s3Transfer.listFiles(in: S3Folder(url: "s3://\(Self.bucketName)/testListS3Files")!).wait())
        XCTAssertNotNil(files?.firstIndex { $0.file.path == "testListS3Files/2.txt" })
        XCTAssertNil(files?.firstIndex { $0.file.path == "testListS3Files2/1.txt" })
    }

    func testTargetFiles() {
        let files: [S3TransferManager.FileDescriptor] = [
            .init(name: "/User/JohnSmith/Documents/test.doc", modificationDate: Date()),
            .init(name: "/User/JohnSmith/Documents/hello.doc", modificationDate: Date())
        ]
        let s3Files = S3TransferManager.targetFiles(files: files, from: "/User/JohnSmith/Documents/", to: S3Folder(url: "s3://my-bucket/")!)
        XCTAssertEqual(s3Files[0].to.path, "test.doc")
        XCTAssertEqual(s3Files[1].to.path, "hello.doc")

        let s3Files2 = S3TransferManager.targetFiles(files: files, from: "/User/JohnSmith", to: S3Folder(url: "s3://my-bucket/test")!)
        XCTAssertEqual(s3Files2[0].to.path, "test/Documents/test.doc")
        XCTAssertEqual(s3Files2[1].to.path, "test/Documents/hello.doc")
    }

    func testS3TargetFiles() {
        let s3Files: [S3TransferManager.S3FileDescriptor] = [
            .init(file: S3File(url: "s3://my-bucket/User/JohnSmith/Documents/test.doc")!, modificationDate: Date()),
            .init(file: S3File(url: "s3://my-bucket/User/JohnSmith/Documents/hello.doc")!, modificationDate: Date())
        ]
        let files = S3TransferManager.targetFiles(files: s3Files, from: S3Folder(url: "s3://my-bucket")!, to: "/User/JohnSmith/Downloads")
        XCTAssertEqual(files[0].to, "/User/JohnSmith/Downloads/User/JohnSmith/Documents/test.doc")
        XCTAssertEqual(files[1].to, "/User/JohnSmith/Downloads/User/JohnSmith/Documents/hello.doc")

        let files2 = S3TransferManager.targetFiles(files: s3Files, from: S3Folder(url: "s3://my-bucket/User/JohnSmith")!, to: "/User/JohnSmith/Downloads")
        XCTAssertEqual(files2[0].to, "/User/JohnSmith/Downloads/Documents/test.doc")
        XCTAssertEqual(files2[1].to, "/User/JohnSmith/Downloads/Documents/hello.doc")
    }

    func testCopyPathLocalToS3() {
        let folder = S3Folder(bucket: Self.bucketName, path: "testCopyPathLocalToS3")
        XCTAssertNoThrow(try Self.s3Transfer.copy(from: self.rootPath + "/Sources", to: folder).wait())
        var files: [S3TransferManager.S3FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3Transfer.listFiles(in: folder).wait())
        XCTAssertNotNil(files?.first(where: { $0.file.path == "testCopyPathLocalToS3/SotoS3Transfer/S3Path.swift" }))
    }

    func testS3toS3CopyPath() {
        let folder1 = S3Folder(bucket: Self.bucketName, path: "testS3toS3CopyPath")
        let folder2 = S3Folder(bucket: Self.bucketName, path: "testS3toS3CopyPath_Copy")
        XCTAssertNoThrow(try Self.s3Transfer.copy(from: self.rootPath + "/Sources", to: folder1).wait())
        XCTAssertNoThrow(try Self.s3Transfer.copy(from: folder1, to: folder2).wait())
        var files: [S3TransferManager.S3FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3Transfer.listFiles(in: folder2).wait())
        XCTAssertNotNil(files?.first(where: { $0.file.path == "testS3toS3CopyPath_Copy/SotoS3Transfer/S3TransferManager.swift" }))
    }

    func testSyncPathLocalToS3() {
        XCTAssertNoThrow(try Self.s3Transfer.sync(from: self.rootPath + "/Tests", to: S3Folder(bucket: Self.bucketName, path: "testSyncPathLocalToS3"), delete: true).wait())
        XCTAssertNoThrow(try Self.s3Transfer.sync(from: S3Folder(bucket: Self.bucketName, path: "testSyncPathLocalToS3"), to: self.rootPath + "/Tests2", delete: true).wait())
        XCTAssertNoThrow(try Self.s3Transfer.sync(from: S3Folder(bucket: Self.bucketName, path: "testSyncPathLocalToS3"), to: S3Folder(bucket: Self.bucketName, path: "testSyncPathLocalToS3_v2"), delete: true).wait())
    }

    func testDeleteFolder() {
        let folder = S3Folder(url: "s3://\(Self.bucketName)/testDeleteFolder")!
        XCTAssertNoThrow(try Self.s3Transfer.sync(from: self.rootPath + "/Tests", to: folder, delete: true).wait())
        var files: [S3TransferManager.S3FileDescriptor]?
        XCTAssertNoThrow(files = try Self.s3Transfer.listFiles(in: folder).wait())
        XCTAssertTrue(files!.count > 0)
        XCTAssertNoThrow(try Self.s3Transfer.delete(folder).wait())
        XCTAssertNoThrow(files = try Self.s3Transfer.listFiles(in: folder).wait())
        XCTAssertEqual(files?.count, 0)
    }

    var rootPath: String {
        return #file
            .split(separator: "/", omittingEmptySubsequences: false)
            .dropLast(3)
            .map { String(describing: $0) }
            .joined(separator: "/")
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
