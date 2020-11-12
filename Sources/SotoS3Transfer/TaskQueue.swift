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

import NIO

/// Manage a queue of tasks, ensuring only so many tasks are running concurrently. Based off code posted by
/// Cory Benfield on Vapor Discord. https://discord.com/channels/431917998102675485/448584561845338139/766320821206908959
class TaskQueue<Value> {
    struct PendingTask<Value> {
        let task: () -> EventLoopFuture<Value>
        let promise: EventLoopPromise<Value>

        init(_ task: @escaping () -> EventLoopFuture<Value>, on eventLoop: EventLoop) {
            self.task = task
            self.promise = eventLoop.makePromise(of: Value.self)
        }
    }

    let maxConcurrentTasks: Int
    var currentTasks: Int
    let eventLoop: EventLoop
    var queue: CircularBuffer<PendingTask<Value>>
    
    init(maxConcurrentTasks: Int, on eventLoop: EventLoop) {
        self.maxConcurrentTasks = maxConcurrentTasks
        self.eventLoop = eventLoop
        self.queue = CircularBuffer(initialCapacity: maxConcurrentTasks)
        self.currentTasks = 0
    }
    
    func submitTask(_ task: @escaping () -> EventLoopFuture<Value>) -> EventLoopFuture<Value> {
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

    private func invoke(_ task: PendingTask<Value>) {
        self.eventLoop.preconditionInEventLoop()
        precondition(self.currentTasks < self.maxConcurrentTasks)

        self.currentTasks += 1
        task.task().hop(to: self.eventLoop).whenComplete { result in
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

}
