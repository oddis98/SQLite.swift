import XCTest
import Dispatch
@testable import SQLite

// Stress test for the NSRecursiveLock fork change: reentrant sync
// (transaction -> run -> prepare -> step) under heavy cross-thread contention.
class ForkConcurrencyStressTests: XCTestCase {

    func testConcurrentReentrantTransactionsNoLossNoCrash() throws {
        let path = NSTemporaryDirectory() + "stress-\(UUID().uuidString).sqlite3"
        let db = try Connection(path)
        try db.run("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, thread INTEGER, i INTEGER)")

        let threads = 8
        let iterations = 250
        let group = DispatchGroup()

        for thread in 0..<threads {
            DispatchQueue.global().async(group: group) {
                for iteration in 0..<iterations {
                    // transaction holds the connection lock; the inner run/scalar
                    // calls re-enter Connection.sync on the same thread (the exact
                    // path that trapped with queue.sync on iOS 26+).
                    try! db.transaction {
                        _ = try db.run("INSERT INTO t (thread, i) VALUES (?, ?)", thread, iteration)
                        _ = try db.scalar("SELECT count(*) FROM t")
                    }
                }
            }
        }

        let result = group.wait(timeout: .now() + 120)
        XCTAssertEqual(result, .success, "deadlock: workers did not finish")

        XCTAssertEqual(try db.scalar("SELECT count(*) FROM t") as? Int64, Int64(threads * iterations))
        XCTAssertEqual(try db.scalar("PRAGMA integrity_check") as? String, "ok")

        try? FileManager.default.removeItem(atPath: path)
    }

    func testTypedInsertRowidAtomicUnderContention() throws {
        let path = NSTemporaryDirectory() + "stress-\(UUID().uuidString).sqlite3"
        let db = try Connection(path)
        let table = Table("rows")
        let id = SQLite.Expression<Int64>("id")
        let value = SQLite.Expression<Int>("value")
        try db.run(table.create { t in
            t.column(id, primaryKey: .autoincrement)
            t.column(value)
        })

        let threads = 8
        let iterations = 250
        let group = DispatchGroup()
        let collected = NSMutableSet()
        let collectLock = NSLock()

        for _ in 0..<threads {
            DispatchQueue.global().async(group: group) {
                for iteration in 0..<iterations {
                    // run(Insert) reads lastInsertRowid inside sync; every thread
                    // must get its own row's id back, never a neighbor's.
                    let rowid = try! db.run(table.insert(value <- iteration))
                    collectLock.lock()
                    XCTAssertFalse(collected.contains(rowid), "duplicate rowid returned: lock did not serialize insert+lastInsertRowid")
                    collected.add(rowid)
                    collectLock.unlock()
                }
            }
        }

        let result = group.wait(timeout: .now() + 120)
        XCTAssertEqual(result, .success, "deadlock: workers did not finish")
        XCTAssertEqual(collected.count, threads * iterations)

        try? FileManager.default.removeItem(atPath: path)
    }
}
