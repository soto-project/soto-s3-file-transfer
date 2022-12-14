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
    static var middlewares: [AWSServiceMiddleware] {
        return (Environment["AWS_ENABLE_LOGGING"] == "true") ? [AWSLoggingMiddleware()] : []
    }

    /// return endpoint
    static func getEndPoint(environment: String) -> String? {
        guard self.isUsingLocalstack == true else { return nil }
        return Environment[environment] ?? "http://localhost:4566"
    }

    /// get name to use for AWS resource
    static func generateResourceName(_ function: String = #function) -> String {
        let prefix = Environment["AWS_TEST_RESOURCE_PREFIX"] ?? ""
        return "soto-" + (prefix + function).filter { $0.isLetter || $0.isNumber }.lowercased()
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
