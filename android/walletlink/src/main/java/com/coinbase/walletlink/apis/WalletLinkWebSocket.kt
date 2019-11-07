// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

package com.coinbase.walletlink.apis

import com.coinbase.wallet.core.extensions.asJsonMap
import com.coinbase.wallet.core.extensions.logError
import com.coinbase.wallet.core.extensions.retryWithDelay
import com.coinbase.wallet.core.extensions.takeSingle
import com.coinbase.wallet.core.util.ConcurrentLruCache
import com.coinbase.wallet.http.connectivity.Internet
import com.coinbase.wallet.http.models.WebIncomingDataType
import com.coinbase.wallet.http.websocket.WebSocket
import com.coinbase.walletlink.dtos.ClientResponseDTO
import com.coinbase.walletlink.dtos.JoinSessionMessageDTO
import com.coinbase.walletlink.dtos.PublishEventDTO
import com.coinbase.walletlink.dtos.ServerRequestDTO
import com.coinbase.walletlink.dtos.SetMetadataMessageDTO
import com.coinbase.walletlink.dtos.SetSessionConfigMessageDTO
import com.coinbase.wallet.core.interfaces.JsonSerializable
import com.coinbase.walletlink.models.ClientMetadataKey
import com.coinbase.walletlink.models.EventType
import com.coinbase.walletlink.models.ServerMessageType
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.disposables.CompositeDisposable
import io.reactivex.rxkotlin.addTo
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import io.reactivex.subjects.ReplaySubject
import java.net.URL
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

private data class WalletLinkCallback(val requestId: Int, val subject: ReplaySubject<ClientResponseDTO>)

/**
 * Represents a WalletLink WebSocket connection
 */
internal class WalletLinkWebSocket(val url: URL) {
    private val serialScheduler = Schedulers.single()
    private val disposeBag = CompositeDisposable()
    private val connection = WebSocket(url)
    private var callbackSequence = AtomicInteger()
    private var incomingRequestsSubject = PublishSubject.create<ServerRequestDTO>()
    private var pendingCallbacks = ConcurrentLruCache<Int, ReplaySubject<ClientResponseDTO>>(maxSize = 300)

    /**
     * Incoming WalletLink requests
     */
    val incomingRequestsObservable: Observable<ServerRequestDTO> = incomingRequestsSubject.hide()

    /**
     * WalletLink Connection state
     */
    val connectionStateObservable = connection.connectionStateObservable

    /**
     * Connect to WalletLink server
     */
    fun connect(): Single<Unit> {
        disposeBag.clear()

        connection.incomingObservable
            .observeOn(serialScheduler)
            .subscribe { processIncomingData(it) }
            .addTo(disposeBag)

        return connection.connect()
    }

    /**
     * Disconnect from WalletLink server
     */
    fun disconnect(): Single<Unit> {
        disposeBag.clear()
        return connection.disconnect()
    }

    /**
     * Join a WalletLink session
     *
     * @param sessionKey sha256(session+secret) hash
     * @param sessionId session ID scanned offline (QR code, NFC, etc)
     *
     * @return A single wrapping `Boolean` to indicate operation was successful
     */
    fun joinSession(sessionKey: String, sessionId: String): Single<Boolean> {
        val callback = createCallback()
        val message = JoinSessionMessageDTO(id = callback.requestId, sessionId = sessionId, sessionKey = sessionKey)

        return send(message, callback = callback)
    }
    /**
    * Set metadata in the current session
    *
    *  @param key Metadata key on WalletLink server
    *  @param value Metadata value stored on WalletLink server. This data may be encrypted.
    *  @param sessionId Session ID scanned offline (QR code, NFC, etc)
    *
     * @return A single wrapping `Boolean` to indicate operation was successful
     */
    fun setMetadata(key: ClientMetadataKey, value: String, sessionId: String): Single<Boolean> {
        val callback = createCallback()
        val id = callback.requestId
        val message = SetMetadataMessageDTO(id = id, sessionId = sessionId, key = key.rawValue, value = value)

        return send(message, callback = callback)
    }

    /**
     * Set session config once a link is established
     *
     * @param webhookId Webhook ID used to push notifications to mobile client
     * @param webhookUrl Webhook URL used to push notifications to mobile client
     * @param metadata Metadata forwarded to host
     * @param sessionId Session ID scanned offline (QR code, NFC, etc)
     *
     * @return A single wrapping `Boolean` to indicate operation was successful
     */
    fun setSessionConfig(
        webhookId: String,
        webhookUrl: URL,
        metadata: Map<String, String>,
        sessionId: String
    ): Single<Boolean> {
        val callback = createCallback()
        val message = SetSessionConfigMessageDTO(
            id = callback.requestId,
            sessionId = sessionId,
            webhookId = webhookId,
            webhookUrl = webhookUrl.toString(),
            metadata = metadata
        )

        return send(message, callback = callback)
    }

    /**
     * Publish new event to WalletLink server
     *
     * @param event Event type to publish
     * @param data The encrypted data sent to host
     * @param sessionId Session ID scanned offline (QR code, NFC, etc)
     *
     * @return A single wrapping `Boolean` to indicate operation was successful
     */
    fun publishEvent(event: EventType, data: String, sessionId: String): Single<Boolean> {
        val callback = createCallback()
        val message = PublishEventDTO(id = callback.requestId, sessionId = sessionId, event = event, data = data)

        return send(message, callback = callback)
    }

    // Send message helper(s)

    private fun send(message: JsonSerializable, callback: WalletLinkCallback): Single<Boolean> {
        val jsonString = message.asJsonString()

        return Internet.statusChanges
            .filter { it.isOnline }
            .takeSingle()
            .flatMap { connection.sendString(jsonString) }
            .flatMap { callback.subject.takeSingle() }
            .map { it.type.isOK }
            .retryWithDelay(3, 1, TimeUnit.SECONDS)
            .timeout(15, TimeUnit.SECONDS)
            .logError()
            .map { success ->
                pendingCallbacks.remove(callback.requestId)
                success
            }
            .onErrorResumeNext { err ->
                pendingCallbacks.remove(callback.requestId)
                throw err
            }
    }

    // Pending requests callback management

    private fun createCallback(): WalletLinkCallback {
        val requestId = callbackSequence.incrementAndGet()
        val subject = ReplaySubject.createWithSize<ClientResponseDTO>(1)

        pendingCallbacks[requestId] = subject

        return WalletLinkCallback(requestId = requestId, subject = subject)
    }

    /**
     * This is called when the host sends a response to a request initiated by the signer
     */
    private fun receivedClientResponse(response: ClientResponseDTO) {
        val requestId = response.id ?: return
        val subject = pendingCallbacks[requestId]

        pendingCallbacks.remove(requestId)

        subject?.onNext(response)
    }

    /**
     * this is called when the host sends a request initiated by the host
     */
    private fun receivedServerRequest(request: ServerRequestDTO) {
        incomingRequestsSubject.onNext(request)
    }

    private fun processIncomingData(incoming: WebIncomingDataType) {
        val incomingText = incoming as? WebIncomingDataType.WebIncomingText ?: return
        val jsonString = incomingText.string
        val json: Map<String, Any> = jsonString.asJsonMap() ?: return
        val typeString: String = json["type"] as? String ?: return

        when (ServerMessageType.fromRawValue(typeString) ?: return) {
            ServerMessageType.OK, ServerMessageType.Fail, ServerMessageType.PublishEventOK -> {
                val response = ClientResponseDTO.fromJsonString(jsonString) ?: return
                receivedClientResponse(response)
            }
            ServerMessageType.Event -> {
                val request = ServerRequestDTO.fromJsonString(jsonString) ?: return
                receivedServerRequest(request)
            }
        }
    }
}
