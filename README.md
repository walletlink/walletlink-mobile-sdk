

WalletLink Mobile Wallet SDK
============================

WalletLink is an open protocol that lets users connect their mobile wallets to your DApp. For more details checkout https://github.com/CoinbaseWallet/walletlink

With the WalletLink SDK, your mobile wallet will be able to interact with DApps on the desktop and be able to sign web3 transactions and messages.

Use WalletLink mobile wallet SDK to support WalletLink in your mobile wallet.

## iOS

### Installation

```ruby
# add WalletLink as a CocoaPod
pod 'CBWalletLink', git: 'git@github.com:CoinbaseWallet/walletlink-mobile.git', commit: "89008b18f23f0fcfddfb00069385f06da77fbc58"
```

### Usage

```swift
// Create an instance of WalletLink class
let walletLink = WalletLink(notificationUrl: notificationUrl)

// To pair the device with a browser after scanning WalletLink QR code
walletLink.link(
    sessionId: sessionId,
    secret: secret,
    url: serverUrl,
    userId: userId,
    metadata: [.ethereumAddress: ethereumAddress]
)
.subscribe(onSuccess: { _ in
    // New WalletLink connection was established
}, onError: { error in
    // Error while connecting to WalletLink server (walletlinkd)
})
.disposed(by: disposeBag)

// Listen on incoming requests
walletLink.requests
    .observeOn(MainScheduler.instance)
    .subscribe(onNext: { request in
        // New unseen request
    })
    .disposed(by: disposeBag)

// Approve DApp permission request (EIP-1102)
walletLink.approveDappPermission(requestId: request.hostRequestId)
    .subscribe(onSuccess: { _ in
        // Browser received EIP-1102 approval
    }, onError: { error in
        // Browser failed to receive EIP-1102 approval
    })
    .disposed(by: disposeBag)

// Approve a given transaction/message signing request
walletLink.approve(requestId: request.hostRequestId, signedData: signedData)
    .subscribe(onSuccess: { _ in
        // Browser received request approval
    }, onError: { error in
        // Browser failed to receive request approval
    })
    .disposed(by: disposeBag)

// Reject transaction/message/EIP-1102 request
walletLink.reject(requestId: request.hostRequestId)
    .subscribe(onSuccess: { _ in
        // Browser received request rejection
    }, onError: { error in
        // Browser failed to receive request rejection
    })
    .disposed(by: disposeBag)
```

## Android

### Installation

```bash
# add WalletLink as a submodule
$ git submodule add git@github.com:CoinbaseWallet/walletlink-mobile.git
```

### Usage

```kotlin
// Create an instance of WalletLink class
val walletLink = WalletLink(notificationUrl, context)

// To pair the device with a browser after scanning WalletLink QR code
walletLink.link(
    sessionId = sessionId,
    secret = secret,
    url = serverUrl,
    userId = userId,
    metadata = mapOf(ClientMetadataKey.EthereumAddress to wallet.primaryAddress)
)
.subscribeBy(onSuccess = {
    // New WalletLink connection was established
}, onError = { error ->
    // Error while connecting to WalletLink server (walletlinkd)
})
.addTo(disposeBag)

// Listen on incoming requests
walletLink.requests
    .observeOn(AndroidSchedulers.mainThread())
    .subscribeBy(onNext = { request ->
        // New unseen request
    })
    .addTo(disposeBag)

// Approve DApp permission request (EIP-1102)
walletLink.approveDappPermission(request.hostRequestId)
    .subscribeBy(onSuccess = {
        // Browser received EIP-1102 approval
    }, onError = { error ->
        // Browser failed to receive EIP-1102 approval
    })
    .addTo(disposeBag)

// Approve a given transaction/message signing request
walletLink.approve(request.hostRequestId, signedData)
    .subscribeBy(onSuccess = {
        // Browser received request approval
    }, onError = { error ->
        // Browser failed to receive request approval
    })
    .addTo(disposeBag)

// Reject transaction/message/EIP-1102 request
walletLink.reject(request.hostRequestId)
    .subscribeBy(onSuccess = {
        // Browser received request rejection
    }, onError = { error ->
        // Browser failed to receive request rejection
    })
    .addTo(disposeBag)
```