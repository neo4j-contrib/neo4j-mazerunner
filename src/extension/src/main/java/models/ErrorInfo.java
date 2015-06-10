package models;

import com.google.gson.Gson;
import org.springframework.http.HttpStatus;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Copyright (C) 2014 Kenny Bastani
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
public class ErrorInfo {
    private String message;
    private HttpStatus status;

    public ErrorInfo(String message, HttpStatus status) {
        this.message = message;
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public void setStatus(HttpStatus status) {
        this.status = status;
    }

    public Response toResponse() {
        return Response.status(status.value())
                .entity(new Gson().toJson(this))
                .type(MediaType.APPLICATION_JSON).build();
    }
}
