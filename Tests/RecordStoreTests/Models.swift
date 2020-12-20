//
//  Models.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import Foundation
import RecordStore

class Person : Record {
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

class Email : Record {
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
