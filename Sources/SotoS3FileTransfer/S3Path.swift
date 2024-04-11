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

/// protocol for S3Path descriptor
public protocol S3Path: Equatable, CustomStringConvertible, Sendable {
    /// s3 bucket name
    var bucket: String { get }
    /// path inside s3 bucket. Without leading forward slash
    var key: String { get }
}

public extension S3Path {
    /// return in URL form `s3://<bucketname>/<path>`
    var url: String { return "s3://\(bucket)/\(key)" }

    /// return parent folder
    func parent() -> S3Folder? {
        let path = self.key.removingSuffix("/")
        guard path.count > 0 else { return nil }
        guard let slash: String.Index = path.lastIndex(of: "/") else { return S3Folder(bucket: bucket, key: "") }
        return S3Folder(bucket: bucket, key: String(path[path.startIndex...slash]))
    }

    /// CustomStringConvertible protocol requirement
    var description: String { return self.url }
}

/// S3 file descriptor
public struct S3File: S3Path {
    /// s3 bucket name
    public let bucket: String
    /// path inside s3 bucket
    public let key: String

    internal init(bucket: String, key: String) {
        self.bucket = bucket
        self.key = key.removingPrefix("/")
    }

    /// initialiizer
    /// - Parameter url: Construct file descriptor from url of form `s3://<bucketname>/<key>`
    public init?(url: String) {
        guard url.hasPrefix("s3://") || url.hasPrefix("S3://") else { return nil }
        guard !url.hasSuffix("/") else { return nil }
        let path = url.dropFirst(5)
        guard let slash = path.firstIndex(of: "/") else { return nil }
        self.init(bucket: String(path[path.startIndex..<slash]), key: String(path[slash..<path.endIndex]))
    }

    /// file name without path
    public var name: String {
        guard let slash = key.lastIndex(of: "/") else { return self.key }
        return String(self.key[self.key.index(after: slash)..<self.key.endIndex])
    }

    /// file name without path or extension
    public var nameWithoutExtension: String {
        let name = self.name
        guard let dot = name.lastIndex(of: ".") else { return name }
        return String(name[name.startIndex..<dot])
    }

    /// file extension of file
    public var `extension`: String? {
        let name = self.name
        guard let dot = name.lastIndex(of: ".") else { return nil }
        return String(name[name.index(after: dot)..<name.endIndex])
    }
}

/// S3 folder descriptor
public struct S3Folder: S3Path {
    /// s3 bucket name
    public let bucket: String
    /// path inside s3 bucket
    public let key: String

    internal init(bucket: String, key: String) {
        self.bucket = bucket
        self.key = key.appendingSuffixIfNeeded("/").removingPrefix("/")
    }

    /// initialiizer
    /// - Parameter url: Construct folder descriptor from url of form `s3://<bucketname>/<key>`
    public init?(url: String) {
        guard url.hasPrefix("s3://") || url.hasPrefix("S3://") else { return nil }
        let path = String(url.dropFirst(5))
        if let slash = path.firstIndex(of: "/") {
            self.init(bucket: String(path[path.startIndex..<slash]), key: String(path[slash..<path.endIndex]))
        } else {
            self.init(bucket: path, key: "")
        }
    }

    /// Return sub folder of folder
    /// - Parameter name: sub folder name
    public func subFolder(_ name: String) -> S3Folder {
        S3Folder(bucket: self.bucket, key: "\(self.key)\(name)")
    }

    /// Return file inside folder
    /// - Parameter name: file name
    public func file(_ name: String) -> S3File {
        guard name.firstIndex(of: "/") == nil else {
            preconditionFailure("Filename \(name) cannot include '/'")
        }
        return S3File(bucket: self.bucket, key: "\(self.key)\(name)")
    }
}

internal extension String {
    func removingPrefix(_ prefix: String) -> String {
        guard hasPrefix(prefix) else { return self }
        return String(dropFirst(prefix.count))
    }

    func appendingPrefixIfNeeded(_ prefix: String) -> String {
        guard !hasPrefix(prefix) else { return self }
        return prefix + self
    }

    func removingSuffix(_ suffix: String) -> String {
        guard hasSuffix(suffix) else { return self }
        return String(dropLast(suffix.count))
    }

    func appendingSuffixIfNeeded(_ suffix: String) -> String {
        guard !hasSuffix(suffix) else { return self }
        return self + suffix
    }
}
