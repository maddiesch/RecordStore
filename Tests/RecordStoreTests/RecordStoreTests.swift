import XCTest
@testable import RecordStore

final class RecordStoreTests: XCTestCase {
    func testCreatingConnection() throws {
        let conn = Connection(source: .memory)
        
        try conn.open()
        try conn.close()
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
}
