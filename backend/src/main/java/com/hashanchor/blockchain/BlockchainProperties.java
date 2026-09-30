package com.hashanchor.blockchain;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The {@code hashanchor.blockchain.*} settings that shape how transactions
 * are sent and confirmed, bound from application.yml (and the active
 * profile's overrides, e.g. application-amoy.yml) in one go.
 *
 * <p>{@code @ConfigurationProperties} is the typed alternative to a pile of
 * {@code @Value("${...}")} parameters: Spring maps each kebab-case key onto
 * the matching record component ({@code receipt-timeout} →
 * {@code receiptTimeout}), converting "30s" to a {@link Duration} and so
 * on. {@code @Validated} runs the constraint annotations at startup, so a
 * missing or malformed setting stops the app before it sends anything.
 *
 * <p>{@code rpc-url} and {@code private-key} live under the same prefix but
 * are deliberately <i>not</i> components here — they're read directly in
 * {@link BlockchainConfig}. A record's generated {@code toString()} prints
 * every component, and this object is the kind of thing that ends up in a
 * log line or a startup failure report; the private key must never be.
 *
 * @param contractAddress the deployed HashAnchor contract. The pattern also
 *     catches an unset {@code HASHANCHOR_CONTRACT_ADDRESS}, which the binder
 *     would otherwise pass through as the literal text "${...}".
 * @param chainId the chain transactions are signed for (EIP-155). A node on
 *     any other chain rejects them instead of executing them.
 * @param gasLimit gas limit for each {@code anchor()} transaction.
 * @param maxGasPriceGwei upper bound on the gas price we'll pay; the node's
 *     {@code eth_gasPrice} suggestion is used when it's lower. Null means no
 *     cap.
 * @param confirmations blocks that must be mined on top of the anchoring
 *     block before a record is marked {@code ANCHORED}. 0 = trust the
 *     receipt as soon as it arrives.
 * @param confirmationTimeout how long one attempt waits for those
 *     confirmations before failing (and being retried).
 */
@Validated
@ConfigurationProperties("hashanchor.blockchain")
public record BlockchainProperties(
        @NotNull @Pattern(regexp = "0x[0-9a-fA-F]{40}", message = "must be a 0x-prefixed 20-byte address")
                String contractAddress,
        @Positive long chainId,
        @Positive long gasLimit,
        @Positive Long maxGasPriceGwei,
        @NotNull Duration receiptPollInterval,
        @NotNull Duration receiptTimeout,
        @Min(0) int confirmations,
        @NotNull Duration confirmationTimeout) {}
