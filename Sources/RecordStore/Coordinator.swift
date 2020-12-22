//
//  Coordinator.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import Foundation
import Combine

public final class Coordinator {
    fileprivate var connection: Connection!
    
    public init() {}
    
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
    private let _willDelete = PassthroughSubject<Model, Never>()
    private let _didDelete = PassthroughSubject<Model, Never>()
    private let _didCommit = PassthroughSubject<Void, Never>()
    
    fileprivate func willSave(_ model: Model) {
        self._willSave.send(model)
    }
    
    fileprivate func didSave(_ model: Model) {
        self._didSave.send(model)
    }
    
    fileprivate func willDelete(_ model: Model) {
        self._willDelete.send(model)
    }
    
    fileprivate func didDelete(_ model: Model) {
        self._didDelete.send(model)
    }
    
    public func query<T : Model>(_ query: Query<T>) -> CoordinatedQuery<T> {
        return CoordinatedQuery(
            self,
            query,
            self._didSave.eraseToAnyPublisher(),
            self._didDelete.eraseToAnyPublisher(),
            self._didCommit.eraseToAnyPublisher()
        )
    }
    
    public func run(_ operation: Operation) throws {
        try self.connection.perform(operation: operation)
    }
    
    public func withConnection<T>(block: (Connection) throws -> T) rethrows -> T {
        defer {
            self.invalidate()
        }
        
        return try block(self.connection)
    }
    
    public func invalidate() {
        NotificationCenter.default.post(name: .coordinatedObjectsShouldInvalidate, object: self)
    }
}

extension Notification.Name {
    fileprivate static let coordinatedObjectsShouldInvalidate = Notification.Name("coordinatedObjectsShouldInvalidate")
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
    
    public func delete(_ model: Model) throws {
        self.coordinator.willDelete(model)
        try self.db.delete(model: model)
        self.coordinator.didDelete(model)
    }
    
    public func run(_ sql: String, parameters: Array<Value> = []) throws {
        try db.execute(sql: sql, parameters: parameters)
    }
    
    public func run(_ sql: String, parameters: Dictionary<String, Value>) throws {
        try db.execute(sql: sql, parameters: parameters)
    }
}

public class CoordinatedQuery<ModelType : Model> : ObservableObject, Cancellable {
    public var objects: Array<ModelType> {
        return self._queue.sync { self._objects }
    }
    
    private weak var _coordinator: Coordinator!
    
    private var _objects = Array<ModelType>()
    
    private var _observers: Set<AnyCancellable> = []
    
    private var _hasChanges: Bool
    
    private var _queue: DispatchQueue
    
    private var _error = PassthroughSubject<Error, Never>()
    
    private var query: Query<ModelType>
    
    public var errorPublisher: AnyPublisher<Error, Never> {
        return self._error.eraseToAnyPublisher()
    }
    
    fileprivate init(_ coord: Coordinator, _ query: Query<ModelType>, _ didSave: AnyPublisher<Model, Never>, _ didDelete: AnyPublisher<Model, Never>, _ didCommit: AnyPublisher<Void, Never>) {
        self._coordinator = coord
        self._hasChanges = false
        self.query = query
        self._queue = DispatchQueue(label: "dev.schipper.RecordStore-CoordinatedQuery")
        
        didSave.receive(on: self._queue).sink(receiveValue: { [weak self] model in
            self?.didSave(model)
        }).store(in: &_observers)
        
        didDelete.receive(on: self._queue).sink(receiveValue: { [weak self] model in
            self?.didDelete(model)
        }).store(in: &_observers)
        
        didCommit.receive(on: self._queue).sink(receiveValue: { [weak self] in
            self?.didCommit()
        }).store(in: &_observers)
        
        NotificationCenter.default.publisher(for: .coordinatedObjectsShouldInvalidate, object: coord).sink { [weak self] (_) in
            do {
                try self?._fetch()
            } catch {
                self?._error.send(error)
            }
        }.store(in: &_observers)
    }
    
    public func fetch() throws {
        try self._queue.sync {
            try self._fetch()
        }
    }
    
    private func _fetch() throws {
        DispatchQueue.main.async {
            self.objectWillChange.send()
        }
        
        try self._coordinator.connection.onQueue {
            Log.coordinator.debug("Performing CoordinatedQuery for \(String(describing: ModelType.self))")
            
            let (sql, values) = self.query.generate()
            
            let statement = try self._coordinator.connection._prepare(sql: sql)
            try statement.bind(values)
            
            let results = try self._coordinator.connection._query(statement: statement)
            
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
    
    private func didDelete(_ model: Model) {
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


#if canImport(SwiftUI)

import SwiftUI

public struct CoordinatedView<Content : View, ViewModel : Model> : View {
    private let content: (Array<ViewModel>) -> Content
    
    @ObservedObject var query: CoordinatedQuery<ViewModel>
    
    public init(coordinator: Coordinator, query: Query<ViewModel>, @ViewBuilder content: @escaping (Array<ViewModel>) -> Content) {
        self.content = content
        self.query = coordinator.query(query)
    }
    
    public var body: some View {
        return ZStack {
            self.content(self.query.objects)
        }.onAppear {
            do {
                try self.query.fetch()
            } catch {
                Log.coordinator.critical("Failed to perform fetch for CoordinatedView \(error.localizedDescription)")
            }
        }
    }
}

#endif
