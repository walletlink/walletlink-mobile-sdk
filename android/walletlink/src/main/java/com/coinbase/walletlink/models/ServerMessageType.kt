// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

package com.coinbase.walletlink.models

import com.squareup.moshi.Json

/**
 * Type of incoming server message
 */
internal enum class ServerMessageType(val rawValue: String) {
    /**
     * New server initiated event
     */
    @Json(name = "Event") Event("Event"),

    /**
     * A successful response to a client initiated request
     */
    @Json(name = "OK") OK("OK"),

    /**
     * A successful response to a client `PublishEvent`
     */
    @Json(name = "PublishEventOK") PublishEventOK("PublishEventOK"),

    /**
     * Session configuration were updated
     */
    @Json(name = "SessionConfigUpdated") SessionConfigUpdated("SessionConfigUpdated"),

    /**
     * A successful response for session configuration request
     */
    @Json(name = "GetSessionConfigOK") GetSessionConfigOK("GetSessionConfigOK"),

    /**
     * An error response to a client initiated request
     */
    @Json(name = "Fail") Fail("Fail");

    /**
     * Determine whether server message is a successful one
     */
    val isOK: Boolean get() = when (this) {
        OK, PublishEventOK, SessionConfigUpdated, GetSessionConfigOK -> true
        Fail, Event -> false
    }

    companion object {
        private val mapping = values().map { it.rawValue to it }.toMap()

        fun fromRawValue(rawValue: String): ServerMessageType? {
            return mapping[rawValue]
        }
    }
}
