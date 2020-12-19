//
//  Statement.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation
import SQLite3

internal typealias StmtPtr = OpaquePointer

public enum StatementError : RecordError {
    case sqlGenerationError
    case unexpectedParameterCount(Int)
    case unnamedParameter(Int)
    case missingValueForNamedParameter(String)
}

public final class Statement {
    private static let cleanupCharSet = CharacterSet(charactersIn: ";").union(.whitespacesAndNewlines)
    
    internal var stmt: StmtPtr!
    internal let db: DatabasePtr!
    
    public let parameterCount: Int32
    public let sql: String
    
    internal init(sql: String, db: DatabasePtr!) throws {
        var stmt: StmtPtr?
        
        let status = sqlite3_prepare_v3(db, sql, -1, 0, &stmt, nil)
        
        try SQLError.check(status: status, ptr: db)
        
        self.stmt = stmt
        self.db = db
        self.parameterCount = sqlite3_bind_parameter_count(stmt)
        
        guard let cSql = sqlite3_sql(stmt) else {
            throw StatementError.sqlGenerationError
        }
        
        self.sql = String(cString: cSql).trimmingCharacters(in: Statement.cleanupCharSet)
    }
    
    public func reset() throws {
        let status = sqlite3_reset(self.stmt)
        
        try SQLError.check(status: status, ptr: self.db)
    }
    
    public func bind(_ values: Dictionary<String, Value>) throws {
        guard Int32(values.count) == self.parameterCount else {
            throw StatementError.unexpectedParameterCount(Int(self.parameterCount))
        }
        
        try self.unbind()
        
        for i in (0..<self.parameterCount) {
            guard let cName = sqlite3_bind_parameter_name(self.stmt, i + 1) else {
                throw StatementError.unnamedParameter(Int(i))
            }
            let name = String(String(cString: cName).dropFirst())
            
            guard let value = values[name] else {
                throw StatementError.missingValueForNamedParameter(name)
            }
            
            try value.bind(index: i + 1, stmt: self.stmt, db: self.db)
        }
    }
    
    public func bind(_ values: Array<Value>) throws {
        guard Int32(values.count) == self.parameterCount else {
            throw StatementError.unexpectedParameterCount(Int(self.parameterCount))
        }
        
        try self.unbind()
        
        for (index, value) in values.enumerated() {
            try value.bind(index: Int32(index) + 1, stmt: self.stmt, db: self.db)
        }
    }
    
    private func unbind() throws {
        sqlite3_clear_bindings(self.stmt)
    }
    
    internal func finalize() {
        sqlite3_finalize(self.stmt)
        self.stmt = nil
    }
    
    deinit {
        self.finalize()
    }
}
