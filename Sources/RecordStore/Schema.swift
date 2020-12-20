//
//  Schema.swift
//  
//
//  Created by Maddie Schipper on 12/20/20.
//

public struct Schema : Codable {
    public var tables: Array<TableSchema>
    
    public init(_ tables: TableSchema...) throws {
        self.init(tables: tables)
    }
    
    public init(tables: Array<TableSchema>) {
        self.tables = tables
    }
}

public struct TableSchema : Codable {
    public struct Column : Codable {
        public enum StorageClass : String, Codable {
            case null = "NULL"
            case integer = "INTEGER"
            case real = "REAL"
            case text = "TEXT"
            case blob = "BLOB"
        }
        
        public struct Options : OptionSet, Codable {
            public let rawValue: UInt8
            
            public init(rawValue: UInt8) {
                self.rawValue = rawValue
            }
            
            public func encode(to encoder: Encoder) throws {
                var container = encoder.singleValueContainer()
                
                var values = Array<String>()
                
                if self.contains(.notNull) {
                    values.append("not null")
                }
                if self.contains(.primaryKeyAutoIncrement) {
                    values.append("primary key auto")
                }
                if self.contains(.primaryKey) {
                    values.append("primary key")
                }
                if self.contains(.unique) {
                    values.append("unique")
                }
                
                try container.encode(values)
            }
            
            public init(from decoder: Decoder) throws {
                let container = try decoder.singleValueContainer()
                
                let encoded = try container.decode(Array<String>.self)
                
                print(encoded)
                
                var options = Options()
                
                if encoded.contains("not null") {
                    options.insert(.notNull)
                }
                
                if encoded.contains("primary key auto") {
                    options.insert(.primaryKeyAutoIncrement)
                }
                
                if encoded.contains("primary key") {
                    options.insert(.primaryKey)
                }
                
                if encoded.contains("unique") {
                    options.insert(.unique)
                }
                
                self = options
            }
            
            public static let notNull = Options(rawValue: 1 << 0)
            public static let unique = Options(rawValue: 1 << 1)
            public static let primaryKey = Options(rawValue: 1 << 2)
            public static let primaryKeyAutoIncrement = Options(rawValue: 1 << 3)
            
            public var sql: String {
                var parts = Array<String>()
                
                if self.contains(.notNull) {
                    parts.append("NOT NULL")
                }
                
                if self.contains(.primaryKeyAutoIncrement) {
                    parts.append("PRIMARY KEY AUTOINCREMENT")
                } else if self.contains(.primaryKey) {
                    parts.append("PRIMARY KEY")
                }
                
                if self.contains(.unique) {
                    parts.append("UNIQUE")
                }
                
                return parts.joined(separator: " ")
            }
        }
        
        public let name: String
        public let storage: StorageClass
        public let options: Options
        
        var sql: String {
            return "\"\(self.name)\" \(self.storage.rawValue) \(self.options.sql)".trimmingCharacters(in: .whitespacesAndNewlines)
        }
    }
    
    public struct Index : Codable {
        public let name: String
        public let columns: Array<String>
        public let isUnique: Bool
    }
    
    public struct ForeignKey : Codable {
        public enum Action : String, Codable {
            case noAction = "NO ACTION"
            case cascade = "CASCADE"
            case setNull = "SET NULL"
            case setDefault = "SET DEFAULT"
            case restrict = "RESTRICT"
        }
        
        public let parentTable: String
        public let parentKey: String
        public let childKey: String
        public let isDeferrable: Bool
        public let onDelete: Action
        public let onUpdate: Action
        
        var sql: String {
            var sql = "FOREIGN KEY(\"\(self.childKey)\") REFERENCES \"\(self.parentTable)\"(\"\(self.parentKey)\")"
            
            if self.isDeferrable {
                sql += " DEFERRABLE INITIALLY DEFERRED"
            }
            
            sql += " ON DELETE \(self.onDelete.rawValue)"
            sql += " ON UPDATE \(self.onUpdate.rawValue)"
            
            return sql
        }
    }
    
    public let tableName: String
    
    public private(set) var columns = Array<Column>()
    public private(set) var indices = Array<Index>()
    public private(set) var foreignKeys = Array<ForeignKey>()
    
    public mutating func add(columnWithName name: String, type: Column.StorageClass, options: Column.Options = []) {
        let col = Column(name: name, storage: type, options: options)
        
        self.columns.append(col)
    }
    
    public mutating func add(indexWithName name: String, columns: Array<String>, isUnique: Bool = false) {
        let index = Index(name: name, columns: columns, isUnique: isUnique)
        
        self.indices.append(index)
    }
    
    public mutating func add(foreignKey key: String, parent: String, parentKey: String = "rowid", isDeferrable: Bool = false, onDelete: ForeignKey.Action = .noAction, onUpdate: ForeignKey.Action = .noAction) {
        let fk = ForeignKey(parentTable: parent, parentKey: parentKey, childKey: key, isDeferrable: isDeferrable, onDelete: onDelete, onUpdate: onUpdate)
        
        self.foreignKeys.append(fk)
    }
    
    fileprivate func createTableStatement(_ db: Database) throws -> Statement {
        var definitions = Array<String>()
        
        definitions.append(contentsOf: self.columns.map { $0.sql })
        definitions.append(contentsOf: self.foreignKeys.map { $0.sql })
        
        let sql = "CREATE TABLE \"\(self.tableName)\" (\(definitions.joined(separator: ", ")));"
        return try db.prepare(sql: sql)
    }
    
    public func statements(_ db: Database) throws -> Array<Statement> {
        var statements = Array<Statement>()
        
        for index in self.indices {
            let columns = index.columns.map { "\"\($0)\"" }
            let sql = "CREATE\(index.isUnique ? " UNIQUE " : " ")INDEX \"\(index.name)\" ON \"\(self.tableName)\"(\(columns.joined(separator: ", ")));"
            statements.append(try db.prepare(sql: sql))
        }
        
        return statements
    }
    
    internal var migration: Migration {
        SchemaMigration(name: "CREATE_\(self.tableName)", schema: self)
    }
}


fileprivate class SchemaMigration : Migration {
    let schema: TableSchema
    
    init(name: String, schema: TableSchema) {
        self.schema = schema
        
        super.init(name: name)
    }
    
    override func migrate(in sp: Database) throws {
        try sp.execute(statement: try schema.createTableStatement(sp))
        
        for statement in try schema.statements(sp) {
            try sp.execute(statement: statement)
        }
    }
}
