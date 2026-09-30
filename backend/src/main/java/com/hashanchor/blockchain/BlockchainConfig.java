package com.hashanchor.blockchain;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
 *
 * <p>{@code @EnableConfigurationProperties} registers
 * {@link BlockchainProperties} as a bean (bound and validated from
 * {@code hashanchor.blockchain.*}), so {@link AnchorClient} can simply ask
 * for it in its constructor. The RPC URL and private key are read here
 * instead, to keep the key out of that record (see its Javadoc).
 */
@Configuration
@EnableConfigurationProperties(BlockchainProperties.class)
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
