//
//  CoordinatorTests.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import XCTest
import RecordStore

final class CoordinatorTests: XCTestCase {
    override func setUpWithError() throws {
        try Coordinator.global.open(.memory)
        try Coordinator.global.apply(schema: Schema(Person.createTableSchema(), Email.createTableSchema()))
    }
    
    override func tearDownWithError() throws {
        try Coordinator.global.close()
    }
    
    func testSavingRecord() throws {
        let query = try Coordinator.global.query(for: Person.self, "SELECT * FROM Person;")
        
        try query.fetch()
        
        try Coordinator.global.perform { c in
            let person = Person()
            person.set(value: "Maddie", forKey: "firstName")
            person.set(value: "Schipper", forKey: "lastName")
            
            try c.save(person)
        }
        
        XCTAssertEqual(1, query.objects.count)
    }
}
