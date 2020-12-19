//
//  Backup.swift
//  
//
//  Created by Maddie Schipper on 12/18/20.
//

import Foundation
import SQLite3
import OSLog

public enum BackupError : Swift.Error {
    case failedToInitializeBackup
    case failedToFinishBackup(Int32)
}

fileprivate let BackupLog = Logger(subsystem: Log.subsystem, category: "db-backup")

internal func online_db_backup(from: DatabasePtr!, to:  DatabasePtr!, fromName: String, toName: String) throws {
    let start = DispatchTime.now()
    defer {
        let end = DispatchTime.now()
        
        let delta = Double(end.uptimeNanoseconds - start.uptimeNanoseconds) / 1_000.0
        
        BackupLog.info("completed backup \(delta, privacy: .public)μs")
    }
    guard let backup = sqlite3_backup_init(to, toName, from, fromName) else {
        throw BackupError.failedToInitializeBackup
    }
    
    BackupLog.debug("Performing Database Backup")
    
    var rc: Int32 = SQLITE_OK
    repeat {
        rc = sqlite3_backup_step(backup, 5)
        
        BackupLog.debug("Step: \(sqlite3_backup_remaining(backup))/\(sqlite3_backup_pagecount(backup))")
    } while (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED)
    
    sqlite3_backup_finish(backup)
    
    rc = sqlite3_errcode(to)
    
    guard rc == SQLITE_OK else {
        throw BackupError.failedToFinishBackup(rc)
    }
}
