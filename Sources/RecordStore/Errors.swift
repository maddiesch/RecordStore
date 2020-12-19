//
//  Errors.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation
import SQLite3

public protocol RecordError : Swift.Error {
}

public enum ConnectionError : RecordError {
    case open(String)
    case openFailure(Int32)
    case closeFailure(Int32)
    case unopened
    case noConnection
    case completed
}

public struct SQLError : RecordError {
    public static func check(status: Int32, ptr: OpaquePointer!, success: Set<Int32> = [SQLITE_OK]) throws {
        guard success.contains(status) == false else {
            return
        }
        
        let message = String(cString: sqlite3_errmsg(ptr))
        let code = sqlite3_extended_errcode(ptr)
        
        throw SQLError(message, status, code)
    }
    
    public let errCode: Int32
    public let extendedErrCode: Int32
    public let message: String
    
    internal init(_ message: String, _ code: Int32, _ eCode: Int32) {
        self.message = message
        self.errCode = code
        self.extendedErrCode = eCode
    }
}
