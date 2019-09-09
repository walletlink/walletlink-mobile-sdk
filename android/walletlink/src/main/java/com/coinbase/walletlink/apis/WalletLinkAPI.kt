// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

package com.coinbase.walletlink.apis

import com.coinbase.wallet.core.extensions.asUnit
import com.coinbase.wallet.core.extensions.logError
import com.coinbase.wallet.http.HTTP
import com.coinbase.wallet.http.models.Credentials
import com.coinbase.wallet.http.models.HTTPService
import com.coinbase.walletlink.dtos.GetEventsDTO
import com.coinbase.walletlink.dtos.ServerRequestDTO
import com.coinbase.walletlink.extensions.create
import com.coinbase.walletlink.models.ServerMessageType
import com.coinbase.walletlink.models.Session
import io.reactivex.Single
import java.net.URL

internal class WalletLinkAPI {
    /**
     * Mark a given event as seen
     *
     * @param eventId The event ID
     * @param sessionId The session ID
     * @param secret The session secret
     *
     * @return A Single wrapping a ServerRequestDTO
     */
    fun markEventAsSeen(eventId: String, sessionId: String, secret: String, url: URL): Single<Unit> = HTTP
        .post<ByteArray>(
            service = HTTPService(url),
            path = "/events/$eventId/seen",
            credentials = Credentials.create(sessionId = sessionId, secret = secret)
        )
        .asUnit()
        .logError()
        .onErrorReturn { Unit }

    /**
     * Fetch all unseen events
     *
     * @param session WalletLink connection session
     *
     * @return A Single wrapping a list of encrypted host requests
     */
    fun getUnseenEvents(session: Session): Single<List<ServerRequestDTO>> = HTTP
        .get<GetEventsDTO>(
            service = HTTPService(session.url),
            path = "/events",
            credentials = Credentials.create(sessionId = session.id, secret = session.secret),
            parameters = mapOf("unseen" to "true")
        )
        .map { response ->
            val events = response.body.events ?: emptyList()
            events.map { event ->
                ServerRequestDTO(
                    sessionId = session.id,
                    type = ServerMessageType.Event,
                    event = event.event,
                    eventId = event.id,
                    data = event.data
                )
            }
        }
}
