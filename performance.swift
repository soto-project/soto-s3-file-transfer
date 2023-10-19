// compare creating dictionary against using first
import Foundation

let a = (0..<70000).map {
    let v = UInt64.random(in: 0...UInt64.max)
    return (key: v, value: ($0, v))
}

let date = Date()
let d = Dictionary(uniqueKeysWithValues: a)
let b = d.keys.compactMap {
    d[$0]?.0
}

print(b.count)
print(-date.timeIntervalSinceNow)

let b1 = d.keys.compactMap { key in
    a.first { $0.key == key }
}

print(b1.count)
print(-date.timeIntervalSinceNow)
