//
//  Value.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation
import SQLite3

fileprivate let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
fileprivate let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

public enum Value {
    case null
    case int64(Int64)
    case real(Double)
    case string(String)
    case binary(Data)
    
    internal func bind(index: Int32, stmt: OpaquePointer!, db: OpaquePointer!) throws {
        switch self {
        case .null:
            let status = sqlite3_bind_null(stmt, index)
            try SQLError.check(status: status, ptr: db)
        case .int64(let i):
            let status = sqlite3_bind_int64(stmt, index, i)
            try SQLError.check(status: status, ptr: db)
        case .string(let str):
            let status = sqlite3_bind_text(stmt, index, NSString(string: str).utf8String, -1, SQLITE_TRANSIENT)
            try SQLError.check(status: status, ptr: db)
        case .binary(let data):
            let status = data.withUnsafeBytes {
                sqlite3_bind_blob(stmt, index, $0.baseAddress, Int32($0.count), SQLITE_TRANSIENT)
            }
            try SQLError.check(status: status, ptr: db)
        case .real(let d):
            let status = sqlite3_bind_double(stmt, index, d)
            try SQLError.check(status: status, ptr: db)
        }
    }
}

extension Value : ExpressibleByNilLiteral {
    public init(nilLiteral: ()) {
        self = .null
    }
}

extension Value : ExpressibleByFloatLiteral {
    public init(floatLiteral value: Double) {
        self = .real(value)
    }
}

extension Value : ExpressibleByIntegerLiteral {
    public init(integerLiteral value: Int64) {
        self = .int64(value)
    }
}

extension Value : ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: BooleanLiteralType) {
        self = .int64(value ? 1 : 0)
    }
}

extension Value : ExpressibleByStringLiteral {
    public typealias StringLiteralType = String
    
    public init(stringLiteral value: String) {
        self = .string(value)
    }
}

extension Value {
    public init(date: Date) {
        self.init(integerLiteral: Int64(date.timeIntervalSince1970))
    }
    
    public init(uuid: UUID) {
        self.init(stringLiteral: uuid.uuidString)
    }
}
