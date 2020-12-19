import XCTest
@testable import RecordStore

final class RecordStoreTests: XCTestCase {
    func testCreatingConnection() throws {
        let conn = Connection(source: .memory)
        
        try conn.open()
        try conn.close()
    }
    
    func testCallingExecuteOnUnopenedConnection() {
        do {
            let conn = Connection(source: .memory)
            
            try conn.execute(sql: "CREATE TABLE testing (id TEXT);")
            
            XCTFail()
        } catch let error as ConnectionError {
            XCTAssertEqual(ConnectionError.unopened, error)
        } catch {
            XCTFail(error.localizedDescription)
        }
    }
    
    func testCallingQueryOnUnopenedConnection() {
        do {
            let conn = Connection(source: .memory)
            
            _ = try conn.query(sql: "SELECT 1;")
            
            XCTFail()
        } catch let error as ConnectionError {
            XCTAssertEqual(ConnectionError.unopened, error)
        } catch {
            XCTFail(error.localizedDescription)
        }
    }
    
    func testRawExecute() throws {
        let conn = Connection(source: .memory)
        
        try conn.open()
        
        try conn.execute(sql: "CREATE TABLE \"testing\" (id INTEGER PRIMARY KEY AUTOINCREMENT);")
    }
    
    func testSavepoint() throws {
        let conn = Connection(source: .memory)
        
        try conn.open()
        
        try conn.savepoint {
            try $0.execute(sql: "CREATE TABLE \"testing\" (id INTEGER PRIMARY KEY AUTOINCREMENT);")
        }
    }
    
    func testSavepointOnUnopenedConnection() {
        let conn = Connection(source: .memory)
        
        do {
            try conn.savepoint { _ in }
            
            XCTFail()
        } catch let error as ConnectionError {
            XCTAssertEqual(ConnectionError.unopened, error)
        } catch {
            XCTFail(error.localizedDescription)
        }
    }
    
    func testNestedSavepoint() throws {
        let conn = Connection(source: .memory)
        
        try conn.open()
        
        try conn.savepoint { s1 in
            try s1.execute(sql: "CREATE TABLE \"testing\" (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);")
            
            try s1.savepoint { s2 in
                try s2.execute(sql: "INSERT INTO \"testing\" (name) VALUES (?);", parameters: [.string("FOO BAR")])
                
                let row = try s2.query(sql: "SELECT COUNT(*) FROM \"testing\";").first()
                
                XCTAssertEqual(1, row.integer(forColumn: "COUNT(*)"))
                
                try s2.rollback()
            }
            
            let row1 = try s1.query(sql: "SELECT COUNT(*) FROM \"testing\";").first()
            
            XCTAssertEqual(0, row1.integer(forColumn: "COUNT(*)"))
            
            try s1.savepoint { s2 in
                try s2.execute(sql: "INSERT INTO \"testing\" (name) VALUES (?);", parameters: [.string("FOO BAR")])
                
                let row = try s2.query(sql: "SELECT COUNT(*) FROM \"testing\";").first()
                
                XCTAssertEqual(1, row.integer(forColumn: "COUNT(*)"))
            }
            
            let row2 = try s1.query(sql: "SELECT COUNT(*) FROM \"testing\";").first()
            
            XCTAssertEqual(1, row2.integer(forColumn: "COUNT(*)"))
        }
        
        let row = try conn.query(sql: "SELECT COUNT(*) FROM \"testing\";").first()
        
        XCTAssertEqual(1, row.integer(forColumn: "COUNT(*)"))
    }
    
    func testStatementBindingNames() throws {
        let conn = Connection(source: .memory)
        
        try conn.open()
        
        try conn.execute(sql: "CREATE TABLE \"testing\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"name\" TEXT);")
        let stmt = try conn.prepare(sql: "INSERT INTO \"testing\" (name) VALUES (:name);")
        try stmt.bind(["name": .string("Foo Bar")])
        try conn.execute(statement: stmt)
    }
    
    func testBasicQuery() throws {
        let conn = Connection(source: .memory)
        
        try conn.open()
        
        try conn.savepoint { db in
            try db.execute(sql: "CREATE TABLE \"testing\" (\"id\" INTEGER PRIMARY KEY AUTOINCREMENT, \"name\" TEXT);")
            try db.execute(sql: "INSERT INTO \"testing\" (name) VALUES (?);", parameters: ["Foo Bar"])
            let result = try db.query(sql: "SELECT * FROM \"testing\";")
            
            while try result.next() {
                let row = try result.row()
                
                XCTAssertEqual(1, row.integer(forColumn: "id"))
                XCTAssertEqual("Foo Bar", row.string(forColumn: "name"))
            }
        }
    }
    
    func testMigration() throws {
        let conn = Connection(source: .memory)
        
        try conn.open()
        
        let m1 = BlockMigration(name: "mig-1") {
            try $0.execute(sql: "CREATE TABLE \"test\" (id INTEGER PRIMARY KEY AUTOINCREMENT);")
        }
        let m2 = BlockMigration(name: "mig-1") {
            try $0.execute(sql: "CREATE TABLE \"test\" (id INTEGER PRIMARY KEY AUTOINCREMENT);")
        }
        
        try conn.perform(operation: m1)
        try conn.perform(operation: m2)
    }
    
    func testBackup() throws {
        let c1 = Connection(source: .memory)
        try c1.open()
        
        let c2 = Connection(source: .memory)
        try c2.open()
        
        try c1.execute(sql: "CREATE TABLE testing_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);")
        try c1.execute(sql: "INSERT INTO testing_table (name) VALUES (?)", parameters: ["Maddie Schipper"])
        
        try c1.backup(to: c2)
        
        let row = try c2.query(sql: "SELECT name FROM testing_table;").first()
        
        XCTAssertEqual(row.string(forColumn: "name"), "Maddie Schipper")
    }
}
