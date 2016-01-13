package com.github.longkerdandy.mithqtt.http.oauth;

import com.github.longkerdandy.mithqtt.api.auth.Authenticator;
import com.google.common.base.Optional;
import com.sun.security.auth.UserPrincipal;
import io.dropwizard.auth.AuthenticationException;
import org.apache.commons.lang3.StringUtils;

/**
 * OAuth2 Authenticator
 */
public class OAuthAuthenticator implements io.dropwizard.auth.Authenticator<String, UserPrincipal> {

    private final Authenticator authenticator;

    public OAuthAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
    }

    /**
     * Authenticate
     * <p>
     * The DropWizard OAuthFactory enables OAuth2 bearer-token authentication,
     * and requires an authenticator which takes an instance of String
     * Also the OAuthFactory needs to be parameterized with the type of the principal the authenticator produces.
     *
     * @param credentials OAuth2 bearer-token
     * @return User Id
     */
    @Override
    public Optional<UserPrincipal> authenticate(String credentials) throws AuthenticationException {
        if (StringUtils.isBlank(credentials)) {
            return Optional.absent();
        }
        // validate token
        String u = this.authenticator.oauth(credentials);
        return StringUtils.isBlank(u) ? Optional.absent() : Optional.of(new UserPrincipal(u));
    }
}