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

import AsyncHTTPClient
import NIO
import NIOHTTP1

public class ResponseStreamer: HTTPClientResponseDelegate {
    public typealias Response = HTTPClient.Response

    enum State {
        case idle
        case head(HTTPResponseHead)
        case end
        case error(Error)
    }

    var state = State.idle
    let request: HTTPClient.Request
    let stream: (ByteBuffer, EventLoop) -> EventLoopFuture<Void>
    // temporary stored future while AHC still doesn't sync to futures returned from `didReceiveBodyPart`
    // See https://github.com/swift-server/async-http-client/issues/274
    var bodyPartFuture: EventLoopFuture<Void>?

    public init(request: HTTPClient.Request, stream: @escaping (ByteBuffer, EventLoop) -> EventLoopFuture<Void>) {
        self.request = request
        self.stream = stream
    }

    public func didReceiveHead(task: HTTPClient.Task<Response>, _ head: HTTPResponseHead) -> EventLoopFuture<Void> {
        switch self.state {
        case .idle:
            self.state = .head(head)
        case .head:
            preconditionFailure("head already set")
        case .end:
            preconditionFailure("request already processed")
        case .error:
            break
        }
        return task.eventLoop.makeSucceededFuture(())
    }

    public func didReceiveBodyPart(task: HTTPClient.Task<Response>, _ part: ByteBuffer) -> EventLoopFuture<Void> {
        switch self.state {
        case .idle:
            preconditionFailure("no head received before body")
        case .head(let head):
            if (200..<300).contains(head.status.code) {
                let futureResult = self.stream(part, task.eventLoop)
                self.bodyPartFuture = futureResult
                return futureResult
            }
        case .end:
            preconditionFailure("request already processed")
        case .error:
            break
        }
        return task.eventLoop.makeSucceededFuture(())
    }

    public func didReceiveError(task: HTTPClient.Task<Response>, _ error: Error) {
        self.state = .error(error)
    }

    public func didFinishRequest(task: HTTPClient.Task<Response>) throws -> Response {
        switch self.state {
        case .idle:
            preconditionFailure("no head received before end")
        case .head(let head):
            return Response(host: self.request.host, status: head.status, version: head.version, headers: head.headers, body: nil)
        case .end:
            preconditionFailure("request already processed")
        case .error(let error):
            throw error
        }
    }
}

