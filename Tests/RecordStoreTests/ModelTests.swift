//
//  ModelTests.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import XCTest
import RecordStore

fileprivate class Person : Record {
    override class func createTableSchema() -> TableSchema {
        var schema = super.createTableSchema()
        
        schema.add(columnWithName: "firstName", type: .text)
        schema.add(columnWithName: "lastName", type: .text)
        
        schema.add(indexWithName: "person_name_index", columns: ["firstName", "lastName"])
        
        return schema
    }
    
    override class func validators() -> Array<Validator> {
        return [
            ColumnValidate("firstName").presence(),
            ColumnValidate("lastName").presence()
        ]
    }
}

fileprivate class Email : Record {
    override class func createTableSchema() -> TableSchema {
        var schema = super.createTableSchema()
        
        schema.add(columnWithName: "person_id", type: .integer)
        schema.add(columnWithName: "value", type: .text)
        
        schema.add(indexWithName: "person_email_index", columns: ["person_id", "value"], isUnique: true)
        
        schema.add(foreignKey: "person_id", parent: Person.tableName, onDelete: .cascade)
        
        return schema
    }
    
    override class func validators() -> Array<Validator> {
        return [
            ColumnValidate("value").presence(),
            ColumnValidate("person_id").presence().foreignKey(for: Person.tableName),
        ]
    }
}

final class ModelTests: XCTestCase {
    var connection = Connection(source: .memory)
    
    override func setUpWithError() throws {
        try connection.open()
        try connection.register(record: Person.self)
        try connection.register(record: Email.self)
    }
    
    override func tearDownWithError() throws {
        try connection.close()
    }
    
    func testInsertingModel() throws {
        let person = Person()
        
        try connection.savepoint { s in
            person.set(value: "Maddie", forKey: "firstName")
            person.set(value: "Schipper", forKey: "lastName")
            
            try s.insert(model: person)
        }
        
        XCTAssertNotEqual(0, person.id)
        
        let found = try connection.find(type: Person.self, person.id)
        
        XCTAssertEqual(person.createdAt, found.createdAt)
        
        try connection.savepoint { s in
            let email = Email()
            email.set(value: .int64(person.id), forKey: "person_id")
            email.set(value: "testing@example.com", forKey: "value")
            
            try s.insert(model: email)
        }
    }
    
    func testUpdatingModel() throws {
        let person = Person()
        
        try connection.savepoint { s in
            person.set(value: "Maddie", forKey: "firstName")
            person.set(value: "Schipper", forKey: "lastName")
            
            try s.insert(model: person)
        }
        
        XCTAssertNotEqual(0, person.id)
        
        let found = try connection.find(type: Person.self, person.id)
        
        XCTAssertEqual(person.createdAt, found.createdAt)
        
        found.set(value: "Madison", forKey: "firstName")
        
        try connection.save(model: found)
        
        try person.reload(for: connection)
        
        XCTAssertEqual(person.value(forKey: "firstName")?.string, "Madison")
    }
}

final class SchemaTests : XCTestCase {
    func testSchemaEncoding() throws {
        let schema = Schema(tables: [Person.createTableSchema(), Email.createTableSchema()])
        
        let encoder = JSONEncoder()
        encoder.outputFormatting = .prettyPrinted
        
        let data = try encoder.encode(schema)
        
        print(String(data: data, encoding: .utf8)!)
    }
    
    func testSchemaDecoding() throws {
        let json = """
        {
          "tables": [
            {
              "tableName": "TestTable",
              "foreignKeys": [],
              "columns": [
                {
                  "name": "id",
                  "storage": "INTEGER",
                  "options": [
                    "not null",
                    "primary key auto"
                  ]
                },
                {
                  "name": "value",
                  "storage": "TEXT",
                  "options": [
                    "not null",
                    "unique"
                  ]
                }
              ],
              "indices": [
                {
                  "name": "test_table_value_index",
                  "columns": [
                    "value"
                  ],
                  "isUnique": true
                }
              ]
            }
          ]
        }
        """
        
        let decoder = JSONDecoder()
        
        let schema = try decoder.decode(Schema.self, from: json.data(using: .utf8)!)
        
        let conn = Connection(source: .memory)
        
        try conn.open()
        
        try conn.apply(schema: schema)
    }
    
    func testSchemaApplying() throws {
        let schema = Schema(tables: [Person.createTableSchema(), Email.createTableSchema()])
        
        let conn = Connection(source: .memory)
        
        try conn.open()
        
        try conn.apply(schema: schema)
        
        try conn.register(record: Person.self)
    }
}