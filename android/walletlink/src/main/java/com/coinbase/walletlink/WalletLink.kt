// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

package com.coinbase.walletlink

import android.content.Context
import com.coinbase.wallet.core.extensions.asUnit
import com.coinbase.wallet.core.extensions.reduceIntoMap
import com.coinbase.wallet.core.extensions.unwrap
import com.coinbase.wallet.core.extensions.zipOrEmpty
import com.coinbase.wallet.core.util.BoundedSet
import com.coinbase.wallet.core.util.Optional
import com.coinbase.wallet.core.util.toOptional
import com.coinbase.walletlink.apis.WalletLinkConnection
import com.coinbase.walletlink.exceptions.WalletLinkException
import com.coinbase.walletlink.models.HostRequest
import com.coinbase.walletlink.models.HostRequestId
import com.coinbase.walletlink.models.Session
import com.coinbase.walletlink.models.ClientMetadataKey
import com.coinbase.walletlink.repositories.LinkRepository
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.disposables.CompositeDisposable
import io.reactivex.rxkotlin.addTo
import io.reactivex.rxkotlin.subscribeBy
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import java.net.URL
import java.util.concurrent.ConcurrentHashMap

/**
 * WalletLink SDK interface
 *
 * @property notificationUrl Webhook URL used to push notifications to mobile client
 * @param context Android context
 */
class WalletLink(private val notificationUrl: URL, context: Context) : WalletLinkInterface {
    private val requestsSubject = PublishSubject.create<HostRequest>()
    private val requestsScheduler = Schedulers.single()
    private val processedRequestIds = BoundedSet<HostRequestId>(3000)
    private val linkRepository = LinkRepository(context)
    private val disposeBag = CompositeDisposable()
    private var connections = ConcurrentHashMap<URL, WalletLinkConnection>()

    override val requestsObservable: Observable<HostRequest> = requestsSubject.hide()

    override fun sessions(): List<Session> = linkRepository.sessions

    override fun observeSessions(): Observable<List<Session>> = linkRepository.observeSessions()

    override fun connect(userId: String, metadata: ConcurrentHashMap<ClientMetadataKey, String>) {
        val connections = ConcurrentHashMap<URL, WalletLinkConnection>()
        val sessionsByUrl = linkRepository.sessions.reduceIntoMap(HashMap<URL, List<Session>>()) { acc, session ->
            val sessions = acc[session.url]?.toMutableList()?.apply { add(session) }

            acc[session.url] = sessions?.toList() ?: mutableListOf(session)
        }

        for ((rpcUrl, sessions) in sessionsByUrl) {
            val conn = WalletLinkConnection(
                url = rpcUrl,
                userId = userId,
                notificationUrl = notificationUrl,
                linkRepository = linkRepository,
                metadata = metadata
            )

            observeConnection(conn)
            sessions.forEach { connections[it.url] = conn }
        }

        this.connections = connections
    }

    override fun disconnect() {
        disposeBag.clear()
        connections.values.forEach { it.disconnect() }
        connections.clear()
    }

    override fun link(
        sessionId: String,
        secret: String,
        version: String?,
        url: URL,
        userId: String,
        metadata: Map<ClientMetadataKey, String>
    ): Single<Unit> {
        connections[url]?.let { connection ->
            return connection.link(sessionId = sessionId, secret = secret, version = version)
        }

        val connection = WalletLinkConnection(
            url = url,
            userId = userId,
            notificationUrl = notificationUrl,
            linkRepository = linkRepository,
            metadata = metadata
        )

        connections[url] = connection

        return connection.link(sessionId = sessionId, secret = secret, version = version)
            .map { observeConnection(connection) }
            .doOnError { connections.remove(url) }
    }

    override fun unlink(session: Session): Single<Unit> {
        val connection = connections[session.url]
            ?: return Single.error(WalletLinkException.NoConnectionFound(session.url))
        return connection.destroySession(sessionId = session.id)
            .map { linkRepository.delete(url = session.url, sessionId = session.id) }
    }

    override fun setMetadata(key: ClientMetadataKey, value: String): Single<Unit> = connections.values
            .map { it.setMetadata(key = key, value = value).asUnit().onErrorReturn { Single.just(Unit) } }
            .zipOrEmpty()
            .asUnit()

    override fun approve(requestId: HostRequestId, signedData: ByteArray): Single<Unit> {
        val connection = connections[requestId.url] ?: return Single.error(
            WalletLinkException.NoConnectionFound(requestId.url)
        )

        return connection.approve(requestId, signedData)
    }

    override fun reject(requestId: HostRequestId): Single<Unit> {
        val connection = connections[requestId.url] ?: return Single.error(
            WalletLinkException.NoConnectionFound(requestId.url)
        )

        return connection.reject(requestId)
    }

    override fun markAsSeen(requestIds: List<HostRequestId>): Single<Unit> = requestIds
        .map { linkRepository.markAsSeen(it, it.url).onErrorReturn { Unit } }
        .zipOrEmpty()
        .asUnit()

    override fun getRequest(eventId: String, sessionId: String, url: URL): Single<HostRequest> {
        val session = linkRepository.getSession(sessionId, url)
            ?: return Single.error(WalletLinkException.SessionNotFound)

        return linkRepository.getPendingRequests(session)
            .map { requests ->
                requests.firstOrNull { eventId == it.hostRequestId.eventId } ?: throw WalletLinkException.EventNotFound
            }
    }

    // MARK: - Helpers

    private fun observeConnection(conn: WalletLinkConnection) {
        conn.requestsObservable
            .observeOn(requestsScheduler)
            .map { Optional(it) }
            .onErrorReturn { Optional(null) }
            .unwrap()
            .subscribe { request ->
                val hostRequestId = request.hostRequestId

                if (processedRequestIds.has(hostRequestId)) {
                    return@subscribe
                }

                processedRequestIds.add(hostRequestId)
                requestsSubject.onNext(request)
            }
            .addTo(disposeBag)

        conn.disconnectSessionObservable
            .observeOn(requestsScheduler)
            .map { request -> request.toOptional() }
            .onErrorReturn { null }
            .unwrap()
            .subscribeBy(onNext = { sessionId -> linkRepository.delete(url = conn.url, sessionId = sessionId) })
            .addTo(disposeBag)
    }
}
