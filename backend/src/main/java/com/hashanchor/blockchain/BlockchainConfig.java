package com.hashanchor.blockchain;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

/**
 * Wires up the generic web3j connection: {@link Web3j} is the JSON-RPC
 * client (think of it as the JDBC connection to the chain), and
 * {@link Credentials} wraps the private key used to sign transactions. Both
 * are plain Spring beans, built once and reused everywhere a contract call
 * is made — see {@link AnchorClient}, which is where the HashAnchor-specific
 * logic lives.
 */
@Configuration
public class BlockchainConfig {

    @Bean(destroyMethod = "shutdown")
    public Web3j web3j(@Value("${hashanchor.blockchain.rpc-url}") String rpcUrl) {
        return Web3j.build(new HttpService(rpcUrl));
    }

    @Bean
    public Credentials blockchainCredentials(@Value("${hashanchor.blockchain.private-key}") String privateKey) {
        return Credentials.create(privateKey);
    }
}
