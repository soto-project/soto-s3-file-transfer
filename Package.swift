// swift-tools-version:6.2.3
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

import PackageDescription

let swiftSettings: [SwiftSetting] = [
    .enableExperimentalFeature("StrictConcurrency=complete")
]

let package = Package(
    name: "soto-s3-file-transfer",
    platforms: [
        .macOS(.v15),
        .iOS(.v18),
        .tvOS(.v18),
        .watchOS(.v11),
    ],
    products: [
        .library(name: "SotoS3FileTransfer", targets: ["SotoS3FileTransfer"])
    ],
    dependencies: [
        .package(url: "https://github.com/soto-project/soto.git", branch: "8.x.x"),
        .package(url: "https://github.com/apple/swift-atomics.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.101.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0"),
    ],
    targets: [
        .target(
            name: "SotoS3FileTransfer",
            dependencies: [
                .product(name: "SotoS3", package: "soto"),
                .product(name: "_NIOFileSystem", package: "swift-nio"),
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: swiftSettings
        ),
        .testTarget(
            name: "SotoS3FileTransferTests",
            dependencies: [
                "SotoS3FileTransfer",
                .product(name: "Atomics", package: "swift-atomics"),
            ]
        ),
    ]
)
