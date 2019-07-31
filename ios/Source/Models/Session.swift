// Copyright (c) 2017-2019 Coinbase Inc. See LICENSE

import CBStore

/// Represents a WalletLink session. A `Session` is created once a user scans a WalletLInk QR code
public struct Session: Hashable, Equatable, Codable {
    /// Session ID generated by the host
    public let id: String

    /// Secret used to encrypt/decrypt data between guest and host
    public let secret: String

    /// WalletLink server websocket URL
    public let url: URL
}

// Used to encode/decode array of `Session`s
struct SessionList: Storable, Codable {
    let items: [Session]
}
