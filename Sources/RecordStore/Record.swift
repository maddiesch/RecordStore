//
//  Record.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import Foundation

open class Record : Model {
    public static var tableName: String {
        return self.createTableSchema().tableName
    }
    
    public var primaryKey: Int64 {
        return self.value(forKey: "id")?.integer ?? 0
    }
    
    open class func createTableSchema() -> TableSchema {
        var schema = TableSchema(tableName: String(describing: self))
        
        schema.add(columnWithName: "id", type: .integer, options: [.notNull, .primaryKeyAutoIncrement])
        schema.add(columnWithName: "createdAt", type: .integer, options: .notNull)
        schema.add(columnWithName: "updatedAt", type: .integer, options: .notNull)
        
        return schema
    }
    
    open class func registered(inConnection connection: Connection) throws {
    }
    
    internal func _willInsert(_ db: Database) {
        self.willSave()
    }
    
    internal func _willUpdate(_ db: Database) {
        self.set(value: Value(date: Date()), forKey: "updatedAt")
        
        self.willSave()
    }
    
    open func willSave() {
        
    }
    
    private var storage: Result.Row
    
    public convenience init() {
        try! self.init(withRow: ["createdAt": Value(date: Date()), "updatedAt": Value(date: Date())])
    }
    
    public required init(withRow row: Result.Row) throws {
        self.storage = row
    }
    
    public func row() throws -> Result.Row {
        return self.storage
    }
    
    public func set(value: Value, forKey key: String) {
        self.storage[key] = value
    }
    
    public func value(forKey key: String) -> Value? {
        return self.storage[key]
    }
    
    subscript(dynamicMember member: String) -> Value? {
        return self.value(forKey: member)
    }
    
    public func inserted(withRowID id: Int64) {
        self.set(value: .int64(id), forKey: "id")
    }
    
    private class func _validators() -> Array<Validator> {
        var v: Array<Validator> = [
            ColumnValidate("createdAt").presence(),
            ColumnValidate("updatedAt").presence(),
        ]
        
        v.append(contentsOf: self.validators())
        
        return v
    }
    
    open class func validators() -> Array<Validator> {
        return []
    }
    
    public func validate(for db: Database) throws {
        var context = ValidationContext(db)
        
        for validator in type(of: self)._validators() {
            try validator.validate(record: self, withContext: &context)
        }
        
        try context.finalize()
    }
}

extension Record : CustomDebugStringConvertible {
    public var debugDescription: String {
        return "\(String(describing: type(of: self)))(\(self.primaryKey))"
    }
}

extension Record {
    @discardableResult
    public func reload(for db: Database) throws -> Record {
        let found = try db.find(type: type(of: self), self.primaryKey)
        self.storage = found.storage
        return self
    }
}

extension Record {
    public var id: Int64 {
        return self.primaryKey
    }
    
    public var createdAt: Date {
        return self.value(forKey: "createdAt")?.date ?? Date()
    }
    
    public var updatedAt: Date {
        return self.value(forKey: "updatedAt")?.date ?? Date()
    }
}

extension Connection {
    public func register(record: Record.Type) throws {
        try self.perform(operation: record.createTableSchema().migration)
    }
    
    public func apply(schema: Schema) throws {
        for table in schema.tables {
            try self.perform(operation: table.migration)
        }
    }
}
