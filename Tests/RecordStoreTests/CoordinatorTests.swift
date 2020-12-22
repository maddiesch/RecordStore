//
//  CoordinatorTests.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import XCTest
import RecordStore

final class CoordinatorTests: XCTestCase {
    var coordinator = Coordinator()
    
    override func setUpWithError() throws {
        try coordinator.open(.memory)
        try coordinator.apply(schema: Schema(Person.createTableSchema(), Email.createTableSchema()))
    }
    
    override func tearDownWithError() throws {
        try coordinator.close()
    }
    
    func testSavingRecord() throws {
        let query = coordinator.query(Query(for: Person.self))
        
        try query.fetch()
        
        try coordinator.perform { c in
            let person = Person()
            person.set(value: "Maddie", forKey: "firstName")
            person.set(value: "Schipper", forKey: "lastName")
            
            try c.save(person)
        }
        
        XCTAssertEqual(1, query.objects.count)
    }
}
