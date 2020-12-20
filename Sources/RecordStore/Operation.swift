//
//  Operation.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation

public protocol Operation {
    func perform(in: Savepoint) throws
}

open class Migration : Operation {
    private let name: String
    
    public init(name: String) {
        self.name = name
    }
    
    public func perform(in sp: Savepoint) throws {
        try sp.execute(sql: "CREATE TABLE IF NOT EXISTS \"RS_MIGRATION\" (name TEXT PRIMARY KEY UNIQUE);")
        
        let result = try sp.query(sql: "SELECT EXISTS(SELECT 1 FROM \"RS_MIGRATION\" WHERE \"name\" = ? LIMIT 1) AS \"hasRun\";", parameters: [.string(self.name)]).first()
        
        guard result.bool(forColumn: "hasRun") == false else {
            return
        }
        
        try sp.savepoint { msp in
            Log.context.debug("Running Migration: \(self.name)")
            try self.migrate(in: msp)
        }
        
        try sp.execute(sql: "INSERT INTO \"RS_MIGRATION\" (\"name\") VALUES (?);", parameters: [.string(self.name)])
    }
    
    open func migrate(in sp: Database) throws { }
}

public final class BlockMigration : Migration {
    public let block: (Database) throws -> Void
    
    public init(name: String, block: @escaping (Database) throws -> Void) {
        self.block = block
        
        super.init(name: name)
    }
    
    public override func migrate(in sp: Database) throws {
        try self.block(sp)
    }
}
