// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

import CBCore
import CBHTTP
import RxSwift

public class WalletLink: WalletLinkProtocol {
    private let notificationUrl: URL
    private let requestsSubject = PublishSubject<HostRequest>()
    private let requestsScheduler = SerialDispatchQueueScheduler(internalSerialQueueName: "WalletLink.requests")
    private let processedRequestIds = BoundedSet<HostRequestId>(maxSize: 3000)
    private let linkRepository = LinkRepository()
    private var disposeBag = DisposeBag()
    private var connections = ConcurrentCache<URL, WalletLinkConnection>()

    public let requests: Observable<HostRequest>

    public var sessions: [Session] {
        return linkRepository.sessions
    }

    public required init(notificationUrl: URL) {
        self.notificationUrl = notificationUrl

        requests = requestsSubject.asObservable()
    }

    public func observeSessions() -> Observable<[Session]> {
        return linkRepository.observeSessions()
    }

    public func connect(userId: String, metadata: [ClientMetadataKey: String]) {
        let connections = ConcurrentCache<URL, WalletLinkConnection>()
        let sessionsByUrl: [URL: [Session]] = linkRepository.sessions
            .reduce(into: [:]) { $0[$1.url, default: []].append($1) }

        sessionsByUrl.forEach { url, sessions in
            let conn = WalletLinkConnection(
                url: url,
                userId: userId,
                notificationUrl: notificationUrl,
                metadata: metadata,
                linkRepository: linkRepository
            )

            self.observeConnection(conn)
            sessions.forEach { connections[$0.url] = conn }
        }

        self.connections = connections
    }

    public func disconnect() {
        disposeBag = DisposeBag()
        connections.removeAll()
    }

    public func link(
        sessionId: String,
        secret: String,
        url: URL,
        userId: String,
        metadata: [ClientMetadataKey: String]
    ) -> Single<Void> {
        if let connection = connections[url] {
            return connection.link(sessionId: sessionId, secret: secret)
        }

        let connection = WalletLinkConnection(
            url: url,
            userId: userId,
            notificationUrl: notificationUrl,
            metadata: metadata,
            linkRepository: linkRepository
        )

        connections[url] = connection

        return connection.link(sessionId: sessionId, secret: secret)
            .map { _ in self.observeConnection(connection) }
            .catchError { err in
                self.connections[url] = nil
                throw err
            }
    }

    public func unlink(session: Session) {
        linkRepository.delete(url: session.url, sessionId: session.id)
    }

    public func setMetadata(key: ClientMetadataKey, value: String) -> Single<Void> {
        return connections.values
            .map { $0.setMetadata(key: key, value: value).catchErrorJustReturn(()) }
            .zip()
            .asVoid()
    }

    public func approve(requestId: HostRequestId, responseData: Data) -> Single<Void> {
        guard let connection = connections[requestId.url] else { return .error(WalletLinkError.noConnectionFound) }

        return connection.approve(requestId: requestId, responseData: responseData)
    }

    public func reject(requestId: HostRequestId) -> Single<Void> {
        guard let connection = connections[requestId.url] else { return .error(WalletLinkError.noConnectionFound) }

        return connection.reject(requestId: requestId)
    }

    public func markAsSeen(requestIds: [HostRequestId]) -> Single<Void> {
        return requestIds
            .map { linkRepository.markAsSeen(requestId: $0, url: $0.url).catchErrorJustReturn(()) }
            .zip()
            .asVoid()
    }

    public func getRequest(eventId: String, sessionId: String, url: URL) -> Single<HostRequest> {
        guard let session = linkRepository.getSession(id: sessionId, url: url) else {
            return .error(WalletLinkError.sessionNotFound)
        }

        return linkRepository.getPendingRequests(session: session)
            .map { requests -> HostRequest in
                guard let request = requests.first(where: { eventId == $0.hostRequestId.eventId }) else {
                    throw WalletLinkError.eventNotFound
                }

                return request
            }
    }

    // MARK: - Helpers

    private func observeConnection(_ conn: WalletLinkConnection) {
        conn.requestsObservable
            .observeOn(requestsScheduler)
            .map { request -> HostRequest? in request }
            .catchErrorJustReturn(nil)
            .unwrap()
            .subscribe(onNext: { [weak self] request in
                let hostRequestId = request.hostRequestId

                guard let self = self, !self.processedRequestIds.has(hostRequestId) else { return }

                self.processedRequestIds.add(hostRequestId)
                self.requestsSubject.onNext(request)
            })
            .disposed(by: disposeBag)
    }
}
