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

import Foundation
import NIO

/// Manage a queue of tasks, ensuring only so many tasks are running concurrently. Based off code posted by
/// Cory Benfield on Vapor Discord. https://discord.com/channels/431917998102675485/448584561845338139/766320821206908959
class TaskQueue<Value> {
    struct PendingTask<Value> {
        let id: UUID
        let task: () -> EventLoopFuture<Value>
        let promise: EventLoopPromise<Value>

        init(_ task: @escaping () -> EventLoopFuture<Value>, on eventLoop: EventLoop) {
            self.id = UUID()
            self.task = task
            self.promise = eventLoop.makePromise(of: Value.self)
        }
    }

    struct Cancelled: Error {}

    let maxConcurrentTasks: Int
    let eventLoop: EventLoop

    private var currentTasks: Int
    private var queue: CircularBuffer<PendingTask<Value>>
    private var inflightTasks: [PendingTask<Value>]

    init(maxConcurrentTasks: Int, on eventLoop: EventLoop) {
        self.maxConcurrentTasks = maxConcurrentTasks
        self.eventLoop = eventLoop
        self.queue = CircularBuffer(initialCapacity: maxConcurrentTasks)
        self.currentTasks = 0
        self.inflightTasks = []
    }

    @discardableResult func submitTask(_ task: @escaping () -> EventLoopFuture<Value>) -> EventLoopFuture<Value> {
        self.eventLoop.flatSubmit {
            let task = PendingTask(task, on: self.eventLoop)

            if self.currentTasks < self.maxConcurrentTasks {
                self.invoke(task)
            } else {
                self.queue.append(task)
            }

            return task.promise.futureResult
        }
    }

    /// Return EventLoopFuture that succeeds only when all the tasks succeed.
    func andAllSucceed() -> EventLoopFuture<Void> {
        self.eventLoop.flatSubmit {
            let futures = self.queue.map { $0.promise.futureResult } + self.inflightTasks.map { $0.promise.futureResult }
            return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
        }
    }

    /// Return EventLoopFuture that succeeds when all the tasks have completed
    func flush() -> EventLoopFuture<Void> {
        self.eventLoop.flatSubmit {
            let futures = self.queue.map { $0.promise.futureResult } + self.inflightTasks.map { $0.promise.futureResult }
            return EventLoopFuture.andAllComplete(futures, on: self.eventLoop)
        }
    }

    /// Cancel all tasks in the queue and return EventLoopFuture that succeeds when all in flight tasks have completed.
    func cancel() -> EventLoopFuture<Void> {
        self.eventLoop.flatSubmit { () -> EventLoopFuture<Void> in
            self.clearQueue()
            return EventLoopFuture.whenAllComplete(self.inflightTasks.map { $0.promise.futureResult }, on: self.eventLoop)
                .map { _ in }
                .always { _ in }
        }
    }

    private func invoke(_ task: PendingTask<Value>) {
        self.eventLoop.preconditionInEventLoop()
        precondition(self.currentTasks < self.maxConcurrentTasks)

        self.currentTasks += 1
        self.inflightTasks.append(task)
        assert(self.inflightTasks.count == self.currentTasks)
        task.task().hop(to: self.eventLoop).whenComplete { result in

            let taskIndex = self.inflightTasks.firstIndex { $0.id == task.id }!
            self.inflightTasks.remove(at: taskIndex)

            self.currentTasks -= 1
            self.invokeIfNeeded()
            task.promise.completeWith(result)
        }
    }

    private func invokeIfNeeded() {
        self.eventLoop.preconditionInEventLoop()

        if let first = self.queue.popFirst() {
            self.invoke(first)
        }
    }

    private func clearQueue() {
        self.eventLoop.preconditionInEventLoop()
        while let task = self.queue.popFirst() {
            task.promise.fail(Cancelled())
        }
    }
}
