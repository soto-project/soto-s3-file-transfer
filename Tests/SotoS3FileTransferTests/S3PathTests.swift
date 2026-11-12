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
import Testing

@testable import SotoS3FileTransfer

struct S3PathTests {
    @Test func testS3Folder() {
        let folder1 = S3Folder(url: "s3://my-bucket")
        let folder2 = S3Folder(url: "s3://my-bucket/")
        let folder3 = S3Folder(url: "S3://my-bucket/folder")
        let folder4 = S3Folder(url: "s3://my-bucket/folder/")
        let folder5 = S3Folder(url: "S3://my-bucket/folder/folder2")
        let folder6 = S3Folder(url: "S4://my-bucket/folder/folder2")

        #expect(folder1?.bucket == "my-bucket")
        #expect(folder1?.key == "")
        #expect(folder2?.bucket == "my-bucket")
        #expect(folder2?.key == "")
        #expect(folder3?.bucket == "my-bucket")
        #expect(folder3?.key == "folder/")
        #expect(folder4?.bucket == "my-bucket")
        #expect(folder4?.key == "folder/")
        #expect(folder5?.bucket == "my-bucket")
        #expect(folder5?.key == "folder/folder2/")
        #expect(folder6 == nil)
    }

    @Test func testS3File() {
        let file1 = S3FilePath(url: "s3://my-bucket/file")
        let file2 = S3FilePath(url: "S3://my-bucket/folder/file")
        let file3 = S3FilePath(url: "s3://my-bucket/file/")

        #expect(file1?.bucket == "my-bucket")
        #expect(file1?.key == "file")
        #expect(file2?.bucket == "my-bucket")
        #expect(file2?.key == "folder/file")
        #expect(file3 == nil)
    }

    @Test func testURL() {
        let file = S3FilePath(url: "s3://bucket/folder/file")
        #expect(file?.url == "s3://bucket/folder/file")
    }

    @Test func testSubFolder() {
        let folder = S3Folder(url: "s3://bucket/folder")
        let subfolder = folder?.subFolder("folder2")
        #expect(subfolder?.url == "s3://bucket/folder/folder2/")
    }

    @Test func testFileInFolder() {
        let folder = S3Folder(url: "s3://bucket/folder")
        let file = folder?.file("file")
        #expect(file?.url == "s3://bucket/folder/file")
    }

    @Test func testFileNameExtension() {
        let file = S3FilePath(url: "s3://bucket/folder/file.txt")
        let name = file?.name
        let nameWithoutExtension = file?.nameWithoutExtension
        let `extension` = file?.extension

        #expect(name == "file.txt")
        #expect(nameWithoutExtension == "file")
        #expect(`extension` == "txt")
    }
}
