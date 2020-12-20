//
//  Validator.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import Foundation
import OSLog

fileprivate let ValidationLog = Logger(subsystem: Log.subsystem, category: "validation")

public protocol Validator {
    func validate(record: Record, withContext ctx: inout ValidationContext) throws
}

public struct ValidationError : Swift.Error {
    public let errors: Array<Error>
    
    fileprivate init(_ errors: Array<Error>) {
        self.errors = errors
    }
}

public struct ValidationContext {
    public let database: Database
    
    internal init(_ db: Database) {
        self.database = db
    }
    
    private var errors: Array<Error> = []
    
    public mutating func add(error: Error) {
        self.errors.append(error)
    }
    
    internal func finalize() throws {
        guard self.errors.count > 0 else {
            return
        }
        throw ValidationError(self.errors)
    }
}

public class ColumnValidate : Validator {
    public struct Error : Swift.Error {
        public let name: String
        
        public private(set) var errors = Array<String>()
        
        fileprivate init(_ name: String) {
            self.name = name
        }
        
        fileprivate mutating func add(error: String) {
            self.errors.append(error)
        }
        
        fileprivate func check() throws {
            guard self.errors.count > 0 else {
                return
            }
            throw self
        }
    }
    
    private let name: String
    
    private var isRequired: Bool = false
    
    private var foreignKey: (String, String)?
    
    public init(_ name: String) {
        self.name = name
    }
    
    public func validate(record: Record, withContext ctx: inout ValidationContext) throws {
        ValidationLog.trace("Validate: \(self.name, privacy: .public)")
        
        let value = record.value(forKey: self.name)
        
        var error = Error(self.name)
        
        if self.isRequired && value == nil {
            error.add(error: "is required")
        }
        
        if let value = value {
            if let (table, row) = self.foreignKey {
                let statement = try ctx.database.prepare(sql: "SELECT EXISTS(SELECT 1 FROM \"\(table)\" WHERE \"\(row)\" = ? LIMIT 1) AS \"itExists\";")
                try statement.bind([value])
                do {
                    let result = try ctx.database.query(statement: statement).first()
                    if result.bool(forColumn: "itExists") == false {
                        error.add(error: "does not exist in \(table)")
                    }
                } catch _ {
                    error.add(error: "does not exist in \(table)")
                }
            }
        }
        
        if error.errors.count > 0 {
            ctx.add(error: error)
        }
    }
    
    public func presence(required: Bool = true) -> ColumnValidate {
        self.isRequired = required
        
        return self
    }
    
    public func foreignKey(for tableName: String, foreignKey key: String = "rowid") -> ColumnValidate {
        self.foreignKey = (tableName, key)
        
        return self
    }
}
