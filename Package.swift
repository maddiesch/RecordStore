// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "RecordStore",
    platforms: [
        .macOS(.v11),
        .iOS(.v14)
    ],
    products: [
        .library(name: "RecordStore", targets: ["RecordStore"]),
    ],
    dependencies: [],
    targets: [
        .target(name: "RecordStore", dependencies: []),
        .testTarget(name: "RecordStoreTests", dependencies: ["RecordStore"]),
    ]
)
