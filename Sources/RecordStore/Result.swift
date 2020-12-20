//
//  Result.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation
import SQLite3

public enum ResultError : RecordError, Equatable {
    case columnNameReadError(Int32)
    case unknownColumnType(Int32, Int32)
    case nullColumn(Int32)
    case noRows
}

public final class Result {
    public typealias Column = (index: Int32, name: String)
    
    public typealias Row = Dictionary<String, Value>
    
    private let statement: Statement
    
    public let columns: Array<Column>
    
    internal init(statement: Statement) throws {
        self.statement = statement
        let count = sqlite3_column_count(statement.stmt)
        
        var columns = Array<Column>()
        
        for i in (0..<count) {
            guard let cStr = sqlite3_column_name(statement.stmt, i) else {
                throw ResultError.columnNameReadError(i)
            }
            
            columns.append((index: i, name: String(cString: cStr)))
        }
        
        self.columns = columns
    }
    
    public func first() throws -> Row {
        while try self.next() {
            return try self.row()
        }
        
        throw ResultError.noRows
    }
    
    public func next() throws -> Bool {
        let status = sqlite3_step(self.statement.stmt)
        
        if status == SQLITE_DONE || status == SQLITE_OK {
            return false
        }
        if status == SQLITE_ROW {
            return true
        }
        
        try SQLError.check(status: status, ptr: self.statement.db)
        
        return false
    }
    
    public func rows() throws -> Array<Row> {
        var rows = Array<Row>()
        
        while try self.next() {
            rows.append(try self.row())
        }
        
        return rows
    }
    
    public func row() throws -> Row {
        var row = Row()
        
        for (index, name) in self.columns {
            let type = sqlite3_column_type(statement.stmt, index)
            
            switch type {
            case SQLITE_INTEGER:
                let i = sqlite3_column_int64(self.statement.stmt, index)
                row[name] = .int64(i)
            case SQLITE_FLOAT:
                let f = sqlite3_column_double(self.statement.stmt, index)
                row[name] = .real(f)
            case SQLITE_BLOB:
                let count = sqlite3_column_bytes(self.statement.stmt, index)
                guard let ptr = sqlite3_column_blob(self.statement.stmt, index) else {
                    throw ResultError.nullColumn(index)
                }
                row[name] = .binary(Data(bytes: ptr, count: Int(count)))
            case SQLITE3_TEXT:
                guard let cStr = sqlite3_column_text(self.statement.stmt, index) else {
                    throw ResultError.nullColumn(index)
                }
                
                row[name] = .string(String(cString: cStr))
            case SQLITE_NULL:
                row[name] = .null
            default:
                throw ResultError.unknownColumnType(index, type)
            }
        }
        
        return row
    }
}

extension Result.Row {
    public func string(forColumn name: String) -> String? {
        return self[name]?.string
    }
    
    public func data(forColumn name: String) -> Data? {
        switch self[name] {
        case .binary(let data):
            return data
        default:
            return nil
        }
    }
    
    public func integer(forColumn name: String) -> Int64? {
        return self[name]?.integer
    }
    
    public func double(forColumn name: String) -> Double? {
        switch self[name] {
        case .real(let d):
            return d
        default:
            return nil
        }
    }
    
    public func bool(forColumn name: String) -> Bool {
        guard let integer = self.integer(forColumn: name) else {
            return false
        }
        return integer == 1
    }
}
