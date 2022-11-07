// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "fltrNode",
    platforms: [.iOS(.v13), .macOS(.v10_15)],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "fltrNode",
            targets: ["fltrNode"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", branch: "main"),
        .package(url: "https://github.com/apple/swift-nio-transport-services.git", branch: "main"),
        .package(url: "https://github.com/fltrWallet/bech32", branch: "main"),
        .package(url: "https://github.com/fltrWallet/FastrangeSipHash", branch: "main"),
        .package(url: "https://github.com/fltrWallet/FileRepo", branch: "main"),
        .package(url: "https://github.com/fltrWallet/fltrECC", branch: "main"),
// Experimental
//      .package(url: "https://github.com/fltrWallet/fltrJET", branch: "main"),
        .package(url: "https://github.com/fltrWallet/fltrTx", branch: "main"),
        .package(url: "https://github.com/fltrWallet/fltrWAPI", branch: "main"),
        .package(url: "https://github.com/fltrWallet/HaByLo", branch: "main"),
        .package(url: "https://github.com/fltrWallet/NameResolver", branch: "main"),
        .package(url: "https://github.com/fltrWallet/UserDefaultsClient", branch: "main"),
        .package(url: "https://github.com/fltrWallet/Stream64", branch: "main"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "fltrNode",
            dependencies: [ "bech32",
                            "FastrangeSipHash",
                            "FileRepo",
                            "fltrECC",
// Experimental
//                          "fltrJET",
                            "fltrTx",
                            "fltrWAPI",
                            "HaByLo",
                            "LoadNode",
                            "Stream64",
                            .product(name: "NameResolverAPI",
                                     package: "NameResolver"),
                            .product(name: "NameResolverPosix",
                                     package: "NameResolver"),
                            .product(name: "NIO",
                                     package: "swift-nio"),
                            .product(name: "NIOTransportServices",
                                     package: "swift-nio-transport-services"),
                            .product(name: "UserDefaultsClientLive",
                                     package: "UserDefaultsClient"), ]),
        .target(
            name: "LoadNode",
            dependencies: [ "HaByLo",
                            .product(name: "NIO", package: "swift-nio"), ],
            resources: [ .process("Resources"), ]),
        .target(
            name: "NodeTestLibrary",
            dependencies: [ "fltrNode",
                            .product(name: "UserDefaultsClientTest",
                                     package: "UserDefaultsClient"), ]),
        .testTarget(
            name: "fltrNodeTests",
            dependencies: [ "bech32",
                            "fltrNode",
                            "fltrTx",
                            "NodeTestLibrary",
                            .product(name: "fltrECCTesting", package: "fltrECC"),
                            .product(name: "NIOTestUtils", package: "swift-nio"), ]),
        .testTarget(
            name: "LoadNodeTests",
            dependencies: [ "fltrNode",
                            "LoadNode",
                            "bech32",
                            "fltrTx", ]),
    ]
)
