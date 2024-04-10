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

#if os(Linux)
import Glibc
#else
import Darwin.C
#endif
import Logging
import SotoCore
import XCTest

internal enum Environment {
    internal static subscript(_ name: String) -> String? {
        guard let value = getenv(name) else {
            return nil
        }
        return String(cString: value)
    }
}

/// Provide various test environment variables
enum TestEnvironment {
    /// are we using Localstack to test. Also return use localstack if we are running a github action and don't have an access key if
    static var isUsingLocalstack: Bool {
        return Environment["AWS_DISABLE_LOCALSTACK"] != "true" ||
            (Environment["GITHUB_ACTIONS"] == "true" && Environment["AWS_ACCESS_KEY_ID"] == "")
    }

    static var credentialProvider: CredentialProviderFactory { return isUsingLocalstack ? .static(accessKeyId: "foo", secretAccessKey: "bar") : .default }

    /// current list of middleware
    /// current list of middleware
    static var middlewares: AWSMiddlewareProtocol {
        return (Environment["AWS_ENABLE_LOGGING"] == "true")
            ? AWSLoggingMiddleware(logger: TestEnvironment.logger, logLevel: .info)
            : AWSMiddleware { request, context, next in
                try await next(request, context)
            }
    }

    /// return endpoint
    static func getEndPoint(environment: String) -> String? {
        guard self.isUsingLocalstack == true else { return nil }
        return Environment[environment] ?? "http://localhost:4566"
    }

    public static var logger: Logger = {
        if let loggingLevel = Environment["AWS_LOG_LEVEL"] {
            if let logLevel = Logger.Level(rawValue: loggingLevel.lowercased()) {
                var logger = Logger(label: "soto")
                logger.logLevel = logLevel
                return logger
            }
        }
        return AWSClient.loggingDisabled
    }()
}

/// Run some test code for a specific asset
func XCTTestAsset<T>(
    create: () async throws -> T,
    test: (T) async throws -> Void,
    delete: (T) async throws -> Void
) async throws {
    let asset = try await create()
    do {
        try await test(asset)
    } catch {
        XCTFail("\(error)")
    }
    try await delete(asset)
}

/// Test for specific error being thrown when running some code
func XCTAsyncExpectError<E: Error & Equatable>(
    _ expectedError: E,
    _ expression: () async throws -> Void,
    _ message: @autoclosure () -> String = "",
    file: StaticString = #filePath,
    line: UInt = #line
) async {
    do {
        _ = try await expression()
        XCTFail("\(file):\(line) was expected to throw an error but it didn't")
    } catch let error as E where error == expectedError {
    } catch {
        XCTFail("\(file):\(line) expected error \(expectedError) but got \(error)")
    }
}
