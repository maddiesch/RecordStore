//
//  Log.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation
import OSLog

public struct Log {
    public static let subsystem: String = {
        if let identifier = Bundle.main.bundleIdentifier {
            return identifier
        }
        return "dev.schipper.RecordStore"
    }()
    
    internal static let connection = Logger(subsystem: Log.subsystem, category: "connection")
    
    internal static let sql = Logger(subsystem: Log.subsystem, category: "sql")
    
    internal static let context = Logger(subsystem: Log.subsystem, category: "context")
}
