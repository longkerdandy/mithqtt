package com.github.longkerdandy.mithqtt.http.exception;

import com.github.longkerdandy.mithqtt.http.entity.ErrorEntity;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * Authorize Exception
 */
@SuppressWarnings("unused")
public class AuthorizeException extends WebApplicationException {

    /**
     * Create a HTTP 401 (Unauthorized) exception.
     */
    public AuthorizeException() {
        super(401);
    }

    /**
     * Create a HTTP 401 (Unauthorized) exception.
     *
     * @param entity the error response entity
     */
    public AuthorizeException(ErrorEntity entity) {
        super(Response.status(401).entity(entity).type("application/json").build());
    }
}