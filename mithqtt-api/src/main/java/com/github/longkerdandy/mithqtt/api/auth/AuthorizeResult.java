package com.github.longkerdandy.mithqtt.api.auth;

/**
 * Authorization Result from Authenticator
 */
@SuppressWarnings("unused")
public enum AuthorizeResult {
    OK(200),
    FORBIDDEN(403);

    private final int value;

    AuthorizeResult(int value) {
        this.value = value;
    }

    public static AuthorizeResult valueOf(int value) {
        for (AuthorizeResult r : values()) {
            if (r.value == value) {
                return r;
            }
        }
        throw new IllegalArgumentException("invalid authorize result: " + value);
    }

    public int value() {
        return value;
    }
}
