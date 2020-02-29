// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

package com.coinbase.walletlink.models

import com.squareup.moshi.Json

enum class ClientMetadataKey(val rawValue: String) {
    // Client ethereum address metadata key
    @Json(name = "EthereumAddress") EthereumAddress("EthereumAddress"),

    // Flag to destroy secure session on both client and host side
    @Json(name = "__destroyed") Destroyed("__destroyed")
}
