// swift-tools-version:5.3
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

import PackageDescription

let package = Package(
    name: "soto-s3-transfer",
    products: [
        .library(name: "SotoS3Transfer", targets: ["SotoS3Transfer"]),
    ],
    dependencies: [
        .package(url: "https://github.com/soto-project/soto.git", from: "5.0.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.4.0")
    ],
    targets: [
        .target(name: "SotoS3Transfer", dependencies: [
            .product(name: "SotoS3", package: "soto"),
            .product(name: "Logging", package: "swift-log"),
        ]),
        .testTarget(name: "SotoS3TransferTests", dependencies: ["SotoS3Transfer"]),
    ]
)
