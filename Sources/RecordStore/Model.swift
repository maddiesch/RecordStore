//
//  Model.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import Foundation

public protocol Model {
    static var tableName: String { get }
    
    var primaryKey: Int64 { get }
    
    init(withRow row: Result.Row) throws
    
    func row() throws -> Result.Row
    
    func inserted(withRowID: Int64)
    
    func validate(for db: Database) throws
}

extension Model {
    internal func insertStatement(forDatabase db: Database) throws -> Statement {
        let row = try self.row()
        
        var columns = Array<String>()
        var placeholders = Array<String>()
        var values = Array<Value>()
        
        for (column, value) in row {
            columns.append(column)
            values.append(value)
            placeholders.append("?")
        }
        
        columns = columns.map { "\"\($0)\"" }
        
        let sql = "INSERT INTO \"\(type(of: self).tableName)\" (\(columns.joined(separator: ", "))) VALUES (\(placeholders.joined(separator: ", ")));"
        
        let statement = try db.prepare(sql: sql)
        try statement.bind(values)
        
        return statement
    }
    
    internal func updateStatement(forDatabase db: Database) throws -> Statement {
        let row = try self.row()
        
        var columns = Array<String>()
        var values = Array<Value>()
        
        for (column, value) in row {
            columns.append("\"\(column)\" = ?")
            values.append(value)
        }
        
        values.append(.int64(self.primaryKey))
        
        let sql = "UPDATE \"\(type(of: self).tableName)\" SET \(columns.joined(separator: ", ")) WHERE \"rowid\" = ?;"
        
        let statement = try db.prepare(sql: sql)
        try statement.bind(values)
        
        return statement
    }
    
    internal func deleteStatement(forDatabase db: Database) throws -> Statement {
        let sql = "DELETE FROM \(type(of: self).tableName.escape()) WHERE \"rowid\" = :id;"
        
        let statement = try db.prepare(sql: sql)
        try statement.bind(["id": .int64(self.primaryKey)])
        
        return statement
    }
}

extension Result {
    func models<T : Model>(_ klass: T.Type) throws -> Array<T> {
        var models = Array<T>()
        
        while try self.next() {
            let row = try self.row()
            
            let model = try klass.init(withRow: row)
            
            models.append(model)
        }
        
        return models
    }
}

public struct NotFoundError : RecordError {
    public let type: Model.Type
    public let id: Int64
    
    fileprivate init(_ type: Model.Type, _ id: Int64) {
        self.type = type
        self.id = id
    }
}

extension Database {
    public func insert(model: Model) throws {
        Log.context.debug("Insert \(String(describing: type(of: model)))")
        
        if let record = model as? Record {
            record._willInsert(self)
        }
        
        try model.validate(for: self)
        
        let statement = try model.insertStatement(forDatabase: self)
        
        try self.execute(statement: statement)
        
        let id = try self.lastInsertedRowID()
        
        model.inserted(withRowID: id)
    }
    
    public func update(model: Model) throws {
        Log.context.debug("Update \(String(describing: type(of: model)))(\(model.primaryKey))")
        
        if let record = model as? Record {
            record._willUpdate(self)
        }
        
        try model.validate(for: self)
        
        let statement = try model.updateStatement(forDatabase: self)
        
        try self.execute(statement: statement)
    }
    
    public func save(model: Model) throws {
        if model.primaryKey > 0 {
            try self.update(model: model)
        } else {
            try self.insert(model: model)
        }
    }
    
    public func delete(model: Model) throws {
        if model.primaryKey <= 0 {
            return
        }
        
        Log.context.debug("Delete \(String(describing: type(of: model)))(\(model.primaryKey))")
        
        if let record = model as? Record {
            record._willDelete(self)
        }
        
        let statement = try model.deleteStatement(forDatabase: self)
        
        try self.execute(statement: statement)
    }
    
    public func find<T : Model>(type: T.Type, _ id: Int64) throws -> T {
        let statement = try self.prepare(sql: "SELECT * FROM \"\(type.tableName)\" WHERE \"rowid\" = ? LIMIT 1;")
        try statement.bind([.int64(id)])
        
        let result = try self.query(statement: statement)
        do {
            let row = try result.first()
            return try type.init(withRow: row)
        } catch let err as ResultError {
            if err == .noRows {
                throw NotFoundError(type, id)
            }
            throw err
        } catch {
            throw error
        }
    }
}
