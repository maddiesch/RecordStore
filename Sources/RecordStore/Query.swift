//
//  Query.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

import Foundation

public final class Query<Element : Model> {
    private typealias WhereClause = (name: String, value: Array<Value>, comparitor: String, qualifier: String)
    private typealias OrderClause = (name: String, direction: OrderDirection)
    
    public enum OrderDirection : String {
        case ascending = "ASC"
        case descending = "DESC"
    }
    
    public let modelType: Element.Type
    
    public init(for element: Element.Type) {
        self.modelType = element
    }
    
    private var select: Array<String> = []
    
    private var wheres: Array<WhereClause> = []
    
    private var orders: Array<OrderClause> = []
    
    private var limit: UInt64 = 0
    
    public func limit(_ limit: UInt64) -> Self {
        self.limit = limit
        
        return self
    }
    
    public func `where`(_ column: String, eq v: Value) -> Self {
        return self.where(column: column, value: v, comparitor: "=", qualifier: "")
    }
    
    public func `where`(_ column: String, gt v: Value) -> Self {
        return self.where(column: column, value: v, comparitor: ">", qualifier: "")
    }
    
    public func `where`(_ column: String, lt v: Value) -> Self {
        return self.where(column: column, value: v, comparitor: "<", qualifier: "")
    }
    
    public func `where`(_ column: String, gte v: Value) -> Self {
        return self.where(column: column, value: v, comparitor: ">=", qualifier: "")
    }
    
    public func `where`(_ column: String, lte v: Value) -> Self {
        return self.where(column: column, value: v, comparitor: "<=", qualifier: "")
    }
    
    public func `where`(_ column: String, not v: Value) -> Self {
        return self.where(column: column, value: v, comparitor: "=", qualifier: "")
    }
    
    public func `where`(_ column: String, in v: [Value]) -> Self {
        self.wheres.append((name: column, value: v, comparitor: "", qualifier: ""))
        
        return self
    }
    
    public func `where`(_ column: String, notIn v: [Value]) -> Self {
        self.wheres.append((name: column, value: v, comparitor: "", qualifier: "NOT"))
        
        return self
    }
    
    public func `where`(column: String, value: Value, comparitor: String, qualifier: String) -> Self {
        self.wheres.append((name: column, value: [value], comparitor: comparitor, qualifier: qualifier))
        
        return self
    }
    
    public func order(by: String, direction: OrderDirection = .ascending) -> Self {
        self.orders.append((name: by, direction: direction))
        
        return self
    }
    
    internal func generate() -> (String, Dictionary<String, Value>) {
        var sql = "SELECT"
        if self.select.count == 0 {
            sql += " *"
        } else {
            sql += " \(self.select.escape().joined(separator: ", "))"
        }
        sql += " FROM \(self.modelType.tableName.escape())"
        
        var values = Dictionary<String, Value>()
        
        if self.wheres.count > 0 {
            sql += " WHERE "
            
            var clauses = Array<String>()
            for (index, (name, value, compare, qualifier)) in self.wheres.enumerated() {
                if compare == "" {
                    var names = Array<String>()
                    for (vi, value) in value.enumerated() {
                        names.append(":w\(index)v\(vi)")
                        values["w\(index)v\(vi)"] = value
                    }
                    clauses.append("\(qualifier) \(name.escape()) IN (\(names.joined(separator: ", ")))".trimmingCharacters(in: .whitespaces))
                } else {
                    clauses.append("\(qualifier) \(name.escape()) \(compare) :w\(index)".trimmingCharacters(in: .whitespaces))
                    values["w\(index)"] = value[0]
                }
            }
            sql += clauses.joined(separator: " AND ")
        }
        
        if self.orders.count > 0 {
            sql += " ORDER BY "
            sql += self.orders.map { "\($0.name.escape()) \($0.direction.rawValue)" }.joined(separator: ", ")
        }
        
        if self.limit > 0 {
            sql += " LIMIT \(self.limit)"
        }
        
        sql += ";"
        
        return (sql, values)
    }
}

extension Database {
    public func query<T>(_ query: Query<T>) throws -> Array<T> {
        let (sql, values) = query.generate()
        
        let statement = try self.prepare(sql: sql)
        try statement.bind(values)
        
        let results = try self.query(statement: statement)
        
        var models = Array<T>()
        
        while try results.next() {
            let row = try results.row()
            
            models.append(try T.init(withRow: row))
        }
        
        return models
    }
}
