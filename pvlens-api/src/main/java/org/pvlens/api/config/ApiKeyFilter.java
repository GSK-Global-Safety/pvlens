/*
 * This file is part of PVLens.
 *
 * PVLens is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PVLens is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PVLens.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.pvlens.api.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Requires a valid {@code X-API-Key} header on every {@code /v1} request.
 *
 * <p>PVLens data includes MedDRA and UMLS content that is licensed, so the API
 * refuses to start without keys unless anonymous access is switched on
 * explicitly. Rate limiting is expected to happen at the API gateway.</p>
 */
@Component
public class ApiKeyFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-API-Key";

    private final List<byte[]> keys;
    private final boolean allowAnonymous;

    public ApiKeyFilter(ApiProperties props) {
        if (props.keys().isEmpty() && !props.allowAnonymous()) {
            throw new IllegalStateException(
                    "No API keys configured. Set pvlens.api.keys (PVLENS_API_KEYS), "
                    + "or pvlens.api.allow-anonymous=true for local development only.");
        }
        this.keys = props.keys().stream().map(k -> k.getBytes(StandardCharsets.UTF_8)).toList();
        this.allowAnonymous = props.allowAnonymous();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return allowAnonymous || !request.getRequestURI().startsWith(request.getContextPath() + "/v1/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String supplied = request.getHeader(HEADER);
        if (supplied != null && matches(supplied.getBytes(StandardCharsets.UTF_8))) {
            chain.doFilter(request, response);
            return;
        }
        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        response.getWriter().write(
                "{\"type\":\"about:blank\",\"title\":\"Unauthorized\",\"status\":401,"
                + "\"detail\":\"Missing or invalid " + HEADER + " header\"}");
    }

    /** Constant-time comparison against every configured key. */
    private boolean matches(byte[] supplied) {
        boolean ok = false;
        for (byte[] key : keys) {
            ok |= MessageDigest.isEqual(key, supplied);
        }
        return ok;
    }
}
