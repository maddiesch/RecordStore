//
//  Database.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation

public protocol Database {
    func prepare(sql: String) throws -> Statement
    
    func execute(statement: Statement) throws
    
    func query(statement: Statement) throws -> Result
    
    func lastInsertedRowID() throws -> Int64
}

extension Database {
    public func execute(sql: String, parameters: Array<Value> = []) throws {
        let stmt = try self.prepare(sql: sql)
        try stmt.reset()
        try stmt.bind(parameters)
        try self.execute(statement: stmt)
    }
    
    public func execute(sql: String, parameters: Dictionary<String, Value>) throws {
        let stmt = try self.prepare(sql: sql)
        try stmt.reset()
        try stmt.bind(parameters)
        try self.execute(statement: stmt)
    }
    
    public func query(sql: String, parameters: Array<Value> = []) throws -> Result {
        let stmt = try self.prepare(sql: sql)
        try stmt.reset()
        try stmt.bind(parameters)
        return try self.query(statement: stmt)
    }
    
    public func query(sql: String, parameters: Dictionary<String, Value>) throws -> Result {
        let stmt = try self.prepare(sql: sql)
        try stmt.reset()
        try stmt.bind(parameters)
        return try self.query(statement: stmt)
    }
}

extension String {
    func escape(prefix: String = "\"", suffix: String = "\"") -> String {
        return "\(prefix)\(self)\(suffix)"
    }
}

extension Array where Element == String {
    func escape(prefix: String = "\"", suffix: String = "\"") -> Array<Element> {
        return self.map { $0.escape(prefix: prefix, suffix: suffix) }
    }
}
