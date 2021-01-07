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

final class S3PathTests: XCTestCase {
    func testS3Folder() {
        let folder1 = S3Folder(url: "s3://my-bucket")
        let folder2 = S3Folder(url: "s3://my-bucket/")
        let folder3 = S3Folder(url: "S3://my-bucket/folder")
        let folder4 = S3Folder(url: "s3://my-bucket/folder/")
        let folder5 = S3Folder(url: "S3://my-bucket/folder/folder2")
        let folder6 = S3Folder(url: "S4://my-bucket/folder/folder2")

        XCTAssertEqual(folder1?.bucket, "my-bucket")
        XCTAssertEqual(folder1?.path, "")
        XCTAssertEqual(folder2?.bucket, "my-bucket")
        XCTAssertEqual(folder2?.path, "")
        XCTAssertEqual(folder3?.bucket, "my-bucket")
        XCTAssertEqual(folder3?.path, "folder/")
        XCTAssertEqual(folder4?.bucket, "my-bucket")
        XCTAssertEqual(folder4?.path, "folder/")
        XCTAssertEqual(folder5?.bucket, "my-bucket")
        XCTAssertEqual(folder5?.path, "folder/folder2/")
        XCTAssertNil(folder6)
    }

    func testS3File() {
        let file1 = S3File(url: "s3://my-bucket/file")
        let file2 = S3File(url: "S3://my-bucket/folder/file")
        let file3 = S3File(url: "s3://my-bucket/file/")

        XCTAssertEqual(file1?.bucket, "my-bucket")
        XCTAssertEqual(file1?.path, "file")
        XCTAssertEqual(file2?.bucket, "my-bucket")
        XCTAssertEqual(file2?.path, "folder/file")
        XCTAssertNil(file3)
    }

    func testURL() {
        let file = S3File(url: "s3://bucket/folder/file")
        XCTAssertEqual(file?.url, "s3://bucket/folder/file")
    }

    func testSubFolder() {
        let folder = S3Folder(url: "s3://bucket/folder")
        let subfolder = folder?.subFolder("folder2")
        XCTAssertEqual(subfolder?.url, "s3://bucket/folder/folder2/")
    }

    func testFileInFolder() {
        let folder = S3Folder(url: "s3://bucket/folder")
        let file = folder?.file("file")
        XCTAssertEqual(file?.url, "s3://bucket/folder/file")
    }

    func testFileNameExtension() {
        let file = S3File(url: "s3://bucket/folder/file.txt")
        let name = file?.name
        let nameWithoutExtension = file?.nameWithoutExtension
        let `extension` = file?.extension

        XCTAssertEqual(name, "file.txt")
        XCTAssertEqual(nameWithoutExtension, "file")
        XCTAssertEqual(`extension`, "txt")
    }
}
