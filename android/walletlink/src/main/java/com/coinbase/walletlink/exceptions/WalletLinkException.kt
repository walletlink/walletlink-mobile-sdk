// Copyright (c) 2018-2019 Coinbase, Inc. <https://coinbase.com/>
// Licensed under the Apache License, version 2.0

package com.coinbase.walletlink.exceptions

import java.net.URL

sealed class WalletLinkException(msg: String) : Exception(msg) {
    /**
     * Unable to encrypt message to send to host
     */
    object UnableToEncryptData : WalletLinkException("Unable to encrypt data")

    /**
     * Thrown when trying to conenct with an invalid session
     */
    object InvalidSession : WalletLinkException("Unable to encrypt data")

    /**
     * Thrown if unable to find connection for given sessionId
     */
    class NoConnectionFound(val url: URL) : WalletLinkException("Unable to find for url $url")

    /**
     * A session with the given ID was not found
     */
    object SessionNotFound : WalletLinkException("Unable to find session")

    /**
     * An event for the session ID was not found
     */
    object EventNotFound : WalletLinkException("Event not found")

    /**
     * Thrown when attempted to approve a host request that doesn't support it
     */
    object UnsupportedRequestMethodApproval : WalletLinkException("Unsupported request method approval")

    /**
     * Thrown if unable to approve or reject signature request. This generally happens if no internet or internal
     * server error
     */
    object UnableToSendSignatureRequestConfirmation : WalletLinkException("Unable to send signature confirmation")
}
