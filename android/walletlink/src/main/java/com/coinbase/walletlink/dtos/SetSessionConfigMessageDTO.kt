// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

package com.coinbase.walletlink.dtos

import com.coinbase.wallet.core.interfaces.JsonSerializable
import com.coinbase.wallet.core.util.JSON
import com.coinbase.walletlink.models.ClientMessageType

/**
 * Client message to set session config visble to host
 *
 * @property type Type of message
 * @property id Client generated request ID
 * @property sessionId Server generated session ID
 * @property webhookId Push notification webhook ID
 * @property webhookUrl Push notification webhook URL
 * @property metadata client metadata forwarded to host once link is established
 */
internal data class SetSessionConfigMessageDTO(
    val type: ClientMessageType = ClientMessageType.SetSessionConfig,
    val id: Int,
    val sessionId: String,
    val webhookId: String?,
    val webhookUrl: String?,
    val metadata: Map<String, String>
) : JsonSerializable {
    @ExperimentalUnsignedTypes
    override fun asJsonString(): String = JSON.toJsonString(this)
}
