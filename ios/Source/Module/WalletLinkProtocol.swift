// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

import RxSwift

public protocol WalletLinkProtocol: class {
    /// Incoming host requests
    var requests: Observable<HostRequest> { get }

    /// Get list of session
    var sessions: [Session] { get }

    /// Observe list of sessions
    ///
    /// - Returns: an observable of all sessions
    func observeSessions() -> Observable<[Session]>

    /// Starts WalletLink connection with the server if a stored session exists. Otherwise, this is a noop. This method
    /// should be called immediately on app launch.
    ///
    /// - Parameters:
    ///     - userId: User ID to deliver push notifications to
    ///     - metadata: client metadata forwarded to host once link is established
    func connect(userId: String, metadata: [ClientMetadataKey: String])

    /// Disconnect from WalletLink server and stop observing session ID updates to prevent reconnection.
    func disconnect()

    /// Connect to WalletLink server using parameters extracted from QR code scan
    ///
    /// - Parameters:
    ///     - sessionId: WalletLink host generated session ID
    ///     - secret: WalletLink host/guest shared secret
    ///     - url: WalletLink base URL
    ///     - userId: User ID to deliver push notifications to
    ///     - metadata: client metadata forwarded to host once link is established
    ///
    /// - Returns: A single wrapping `Void` if connection was successful. Otherwise, an exception is thrown
    func link(
        sessionId: String,
        secret: String,
        url: URL,
        userId: String,
        metadata: [ClientMetadataKey: String]
    ) -> Single<Void>

    /// Disconnect from given WalletLink session
    ///
    /// - Parameters:
    ///     - session: Session to unlink
    func unlink(session: Session)

    /// Set metadata in all active sessions. This metadata will be forwarded to all the hosts
    ///
    /// - Parameters:
    ///   - key: Metadata key
    ///   - value: Metadata value
    ///
    /// - Returns: A single wrapping `Void` if operation was successful. Otherwise, an exception is thrown
    func setMetadata(key: ClientMetadataKey, value: String) -> Single<Void>

    /// Send signature request approval to the requesting host
    ///
    /// - Parameters:
    ///     - requestId: WalletLink host generated request ID
    ///     - responseData: User signed data or any other response to the host
    ///
    /// - Returns: A single wrapping `Void` if operation was successful. Otherwise, an exception is thrown
    func approve(requestId: HostRequestId, responseData: Data) -> Single<Void>

    /// Send signature request rejection to the requesting host
    ///
    /// - Parameters:
    ///     - requestId: WalletLink host generated request ID
    ///
    /// - Returns: A single wrapping `Void` if operation was successful. Otherwise, an exception is thrown
    func reject(requestId: HostRequestId) -> Single<Void>

    /// Mark requests as seen to prevent future presentation
    ///
    /// - Parameters:
    ///     - requestIds: List of alletLink host generated request ID
    ///
    /// - Returns: A single wrapping `Void` if operation was successful. Otherwise, an exception is thrown
    func markAsSeen(requestIds: [HostRequestId]) -> Single<Void>

    /// Get an event
    ///
    /// - Parameters:
    ///   - eventId: The event ID
    ///   - sessionId: The session ID
    ///   - url: WalletLink base URL
    ///
    /// - Returns: A Single wrapping the HostRequest
    func getRequest(eventId: String, sessionId: String, url: URL) -> Single<HostRequest>
}
