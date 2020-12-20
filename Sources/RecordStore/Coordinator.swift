//
//  Coordinator.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import Foundation
import Combine

public final class Coordinator {
    public static let global = Coordinator()
    
    fileprivate var connection: Connection!
    
    public func open(_ source: Source) throws {
        let conn = Connection(source: source)
        
        try conn.open()
        
        self.connection = conn
    }
    
    public func close() throws {
        try self.connection.close()
    }
    
    public func apply(schema: Schema) throws {
        try self.connection.apply(schema: schema)
    }
    
    public func perform<T>(block: (Coordinated) throws -> T) throws -> T {
        return try self.connection.savepoint { sp in
            let c = Coordinated(self, sp)
            
            defer {
                self._didCommit.send()
            }
            
            return try block(c)
        }
    }
    
    private let _willSave = PassthroughSubject<Model, Never>()
    private let _didSave = PassthroughSubject<Model, Never>()
    private let _didCommit = PassthroughSubject<Void, Never>()
    
    fileprivate func willSave(_ model: Model) {
        self._willSave.send(model)
    }
    
    fileprivate func didSave(_ model: Model) {
        self._didSave.send(model)
    }
    
    public func query<T : Model>(for type: T.Type, _ sql: String, parameters: Array<Value> = []) throws -> CoordinatedQuery<T> {
        let statement = try self.connection.prepare(sql: sql)
        try statement.bind(parameters)
        
        return self.query(for: type, statement: statement)
    }
    
    public func query<T : Model>(for type: T.Type, statement: Statement) -> CoordinatedQuery<T> {
        return CoordinatedQuery(self, statement, self._didSave.eraseToAnyPublisher(), self._didCommit.eraseToAnyPublisher())
    }
}

public struct Coordinated {
    private weak var coordinator: Coordinator!
    
    private let db: Savepoint
    
    fileprivate init(_ coordinator: Coordinator, _ sp: Savepoint) {
        self.db = sp
        self.coordinator = coordinator
    }
    
    public func savepoint<T>(block: (Savepoint) throws -> T) throws -> T {
        return try self.db.savepoint(block: block)
    }
}

extension Coordinated {
    public func save(_ model: Model) throws {
        self.coordinator.willSave(model)
        try self.db.save(model: model)
        self.coordinator.didSave(model)
    }
}

public class CoordinatedQuery<ModelType : Model> : ObservableObject, Cancellable {
    public var objects: Array<ModelType> {
        return self._queue.sync { self._objects }
    }
    
    private weak var _coordinator: Coordinator!
    
    private var _objects = Array<ModelType>()
    
    private var _observers: Set<AnyCancellable> = []
    
    private let statement: Statement
    
    private var _hasChanges: Bool
    
    private var _queue: DispatchQueue
    
    private var _error = PassthroughSubject<Error, Never>()
    
    public var errorPublisher: AnyPublisher<Error, Never> {
        return self._error.eraseToAnyPublisher()
    }
    
    fileprivate init(_ coord: Coordinator, _ statement: Statement, _ didSave: AnyPublisher<Model, Never>, _ didCommit: AnyPublisher<Void, Never>) {
        self.statement = statement
        self._coordinator = coord
        self._hasChanges = false
        self._queue = DispatchQueue(label: "dev.schipper.RecordStore-CoordinatedQuery")
        
        didSave.receive(on: self._queue).sink(receiveValue: { [weak self] model in
            self?.didSave(model)
        }).store(in: &_observers)
        
        didCommit.receive(on: self._queue).sink(receiveValue: { [weak self] in
            self?.didCommit()
        }).store(in: &_observers)
    }
    
    public func fetch() throws {
        try self._queue.sync {
            
            try self._fetch()
        }
    }
    
    private func _fetch() throws {
        self.objectWillChange.send()
        
        try self._coordinator.connection.onQueue {
            try self.statement.reset()
            
            Log.coordinator.debug("Performing CoordinatedQuery for \(String(describing: ModelType.self))")
            
            let results = try self._coordinator.connection._query(statement: self.statement)
            
            self._objects = try results.rows().map { try ModelType.init(withRow: $0) }
            
            self._hasChanges = false
        }
    }
    
    public func cancel() {
        self._observers.cancelAll()
    }
    
    private func didSave(_ model: Model) {
        guard model is ModelType else {
            return
        }
        
        self._hasChanges = true
    }
    
    private func didCommit() {
        guard self._hasChanges == true else {
            return
        }
        
        do {
            try self._fetch()
        } catch {
            self._error.send(error)
        }
    }
    
    deinit {
        self._observers.cancelAll()
        self._error.send(completion: .finished)
    }
}

extension Set where Element == AnyCancellable {
    mutating func cancelAll() {
        for c in self {
            c.cancel()
        }
        self.removeAll()
    }
}
