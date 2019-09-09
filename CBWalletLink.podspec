Pod::Spec.new do |s|
  s.name             = 'CBWalletLink'
  s.version          = '0.1.0'
  s.summary          = 'Coinbase WalletLink iOS SDK'
  s.description      = s.summary

  s.homepage         = 'https://github.com/CoinbaseWallet/walletlink-mobile'
  s.license          = { :type => "AGPL-3.0-only", :file => 'LICENSE' }
  s.author           = { 'Coinbase' => 'developer@toshi.org' }
  s.source           = { :git => 'https://github.com/CoinbaseWallet/walletlink-mobile.git', :tag => s.version.to_s }
  s.social_media_url = 'https://twitter.com/coinbase'

  s.ios.deployment_target = '10.0'
  s.swift_version = '4.2'
  s.source_files = 'ios/Source/**/*.swift'
  s.resource_bundles = {
    'CBWalletLink' => ['ios/Source/Databases/*.xcdatamodeld']
  }

  s.dependency 'CBStore'
  s.dependency 'CBCrypto'
  s.dependency 'CBHTTP'
  s.dependency 'BigInt'
  s.dependency 'CBCore'
  s.dependency 'CBDatabase'
  s.dependency 'RxSwift'
end
