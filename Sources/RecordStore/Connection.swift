//
//  Connection.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation
import SQLite3
import Combine

public enum Source {
    case memory
    case location(URL)
}

typealias DatabasePtr = OpaquePointer

public final class Connection {
    private let source: Source
    
    private let queue = DispatchQueue(label: "dev.schipper.RecordStore-Connection")
    
    public init(source: Source) {
        self.source = source
    }
    
    deinit {
        try? self.close()
    }
    
    private var _ptr: OpaquePointer!
    
    private var _spCounter: Int = 0
    
    // MARK: - Publisher
    
    public struct Event {
        public struct Name : Equatable {
            public static let opened = Name("conn_opened")
            public static let closed = Name("conn_closed")
            public static let updated = Name("db_updated")
            public static let committed = Name("tx_committed")
            public static let rolledback = Name("tx_rolledback")
            
            private let rawValue: String
            
            public init(_ rawValue: String) {
                self.rawValue = rawValue
            }
            
            public static func ==(lhs: Name, rhs: Name) -> Bool {
                return lhs.rawValue == rhs.rawValue
            }
        }
        
        public let name: Name
        
        internal init(_ name: Name) {
            self.name = name
        }
    }
    
    internal func publish(event: Event) {
        self._eventPublisher.send(event)
    }
    
    private let _eventPublisher = PassthroughSubject<Event, Never>()
    
    public func publisherForEvents() -> AnyPublisher<Event, Never> {
        return self._eventPublisher.eraseToAnyPublisher()
    }
}

extension Connection : Database {
    public func query(statement: Statement) throws -> Result {
        return try self.queue.sync {
            return try self._query(statement: statement)
        }
    }
    
    public func prepare(sql: String) throws -> Statement {
        return try self.queue.sync {
            return try self._prepare(sql: sql)
        }
    }
    
    public func execute(statement: Statement) throws {
        try self.queue.sync {
            try self._execute(statement: statement)
        }
    }
    
    internal func _execute(statement: Statement) throws {
        guard self._ptr != nil else {
            throw ConnectionError.unopened
        }
        
        try statement._update()
        
        self.publish(event: Event(.updated))
    }
    
    internal func _prepare(sql: String) throws -> Statement {
        guard let ptr = self._ptr else {
            throw ConnectionError.unopened
        }
        
        return try Statement(sql: sql, db: ptr)
    }
    
    internal func _query(statement: Statement) throws -> Result {
        guard self._ptr != nil else {
            throw ConnectionError.unopened
        }
        
        return try statement._query()
    }
}

extension Connection {
    public func open() throws {
        try self.queue.sync {
            guard self._ptr == nil else {
                return
            }
            
            var unsafePath: String?
            
            switch self.source {
            case .memory:
                unsafePath = ":memory:"
            case .location(let url):
                unsafePath = url.absoluteString
            }
            
            guard let path = unsafePath else {
                throw ConnectionError.open("Failed to create a valid Database Path")
            }
            
            Log.connection.debug("Opening database connection: `\(path)`")
            
            var ptr: OpaquePointer?
            
            let status = sqlite3_open_v2(path, &ptr, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
            
            guard status == SQLITE_OK else {
                throw ConnectionError.openFailure(status)
            }
            
            self._ptr = ptr
        }
        
        self.publish(event: Event(.opened))
    }
    
    public func close() throws {
        try self.queue.sync {
            guard let ptr = self._ptr else {
                return
            }
            self._ptr = nil
            
            Log.connection.debug("Closing database connection")
            
            let status = sqlite3_close_v2(ptr)
            
            guard status == SQLITE_OK else {
                throw ConnectionError.closeFailure(status)
            }
        }
        
        self.publish(event: Event(.closed))
    }
}

extension Connection {
    public func savepoint<T>(block: (Savepoint) throws -> T) throws -> T {
        return try self.queue.sync {
            return try self._savepoint(block: block)
        }
    }
    
    internal func _savepoint<T>(block: (Savepoint) throws -> T) throws -> T {
        guard self._ptr != nil else {
            throw ConnectionError.unopened
        }
        
        self._spCounter += 1
        
        let savepoint = _Savepoint(self, "SP_0\(self._spCounter)")
        
        try savepoint.begin()
        
        do {
            let value = try block(savepoint)
            try savepoint.release()
            return value
        } catch {
            try savepoint.rollback()
            throw error
        }
    }
}

extension Connection {
    public func perform(operation: Operation) throws {
        try self.savepoint {
            try operation.perform(in: $0)
        }
    }
}

extension Statement {
    fileprivate func _update() throws {
        Log.sql.debug("\(self.sql)")
        
        let status = sqlite3_step(self.stmt)
        
        try SQLError.check(status: status, ptr: self.db, success: [SQLITE_DONE])
    }
    
    fileprivate func _query() throws -> Result {
        Log.sql.debug("\(self.sql)")
        
        return try Result(statement: self)
    }
}

public protocol Conn {
    func execute(sql: String, values: Array<Value>) throws
}