//
//  Savepoint.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation

public protocol Savepoint : Database {
    var name: String { get }
    
    func savepoint<T>(block: (Savepoint) throws -> T) throws -> T
    
    func begin() throws
    
    func release() throws
    
    func rollback() throws
}

extension Connection.Event.Name {
    public static let savepointBegin = Connection.Event.Name("sp_begin")
    public static let savepointRelease = Connection.Event.Name("sp_release")
    public static let savepointRollback = Connection.Event.Name("sp_rollback")
}

internal final class _Savepoint : Savepoint {
    internal let name: String
    
    internal weak var connection: Connection!
    
    internal init(_ connection: Connection, _ name: String) {
        self.connection = connection
        self.name = name
    }
    
    func query(statement: Statement) throws -> Result {
        return try self.connection._query(statement: statement)
    }
    
    func execute(statement: Statement) throws {
        try self.connection._execute(statement: statement)
    }
    
    func prepare(sql: String) throws -> Statement {
        return try self.connection._prepare(sql: sql)
    }
    
    func lastInsertedRowID() throws -> Int64 {
        return try self.connection._lastInsertedRowID()
    }
    
    func savepoint<T>(block: (Savepoint) throws -> T) throws -> T {
        return try self.connection._savepoint(block: block)
    }
    
    private var isCompleted: Bool = false
    
    func begin() throws {
        let stmt = try self.prepare(sql: "SAVEPOINT \(self.name);")
        try self.execute(statement: stmt)
        
        self.connection.publish(event: Connection.Event(.savepointBegin))
    }
    
    func release() throws {
        guard self.isCompleted == false else {
            return
        }
        let stmt = try self.prepare(sql: "RELEASE SAVEPOINT \(self.name);")
        try self.execute(statement: stmt)
        self.isCompleted = true
        
        self.connection.publish(event: Connection.Event(.savepointRelease))
    }
    
    func rollback() throws {
        guard self.isCompleted == false else {
            return
        }
        let stmt = try self.prepare(sql: "ROLLBACK TO SAVEPOINT \(self.name);")
        try self.execute(statement: stmt)
        self.isCompleted = true
        
        self.connection.publish(event: Connection.Event(.savepointRollback))
    }
}
