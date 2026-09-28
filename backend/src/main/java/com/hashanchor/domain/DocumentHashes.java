package com.hashanchor.domain;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The one definition of "a document's hash", shared by the submit path and
 * the verify path — if they ever hashed differently, every verification
 * would come back "not anchored".
 */
public final class DocumentHashes {

    private DocumentHashes() {}

    /** SHA-256 of {@code input} as a {@code 0x}-prefixed, lowercase hex string (the bytes32 the contract stores). */
    public static String sha256Hex(byte[] input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return "0x" + HexFormat.of().formatHex(digest.digest(input));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a standard algorithm every JVM implementation is
            // required to provide, so this is unreachable in practice.
            throw new IllegalStateException(e);
        }
    }
}
