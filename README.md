**haupdown** is a server for the HAproxy Agent Check protocol. It is expected to run as a sidecar everywhere you run
HAproxy-managed services; configure HAProxy to talk to it via the
[agent-check](http://cbonte.github.io/haproxy-dconv/2.0/configuration.html#5.2-agent-check) flag. It accepts TCP
connections of the HAproxy agent protocol on the port specified by the environment variable `$PORT` (or `--port`) and binds a simple
admin socket at the UNIX domain socket path specified by `$SOCKET_BIND_PATH` (or `--socket-bind-path`).

[![Build Status](https://travis-ci.com/EasyPost/haupdown.svg?branch=master)](https://travis-ci.com/EasyPost/haupdown)

This is basically a replacement for [hacheck](https://github.com/uber/hacheck) that only does the agent-check parts.

Using the `--required-groups` option, you can require that connectors to the administrative socket be in one of the
listed Unix groups in order to change state. By default, everyone who can write to it can change its state.

If the `--global-down-file` (`-G`) option is provided with a path, `haupdown` will behave as through the "all" service
is down when that path exists.

This work is licensed under the ISC license, a copy of which can be found at [LICENSE.txt](LICENSE.txt).

## Admin Socket Commands

 * **`up servicename`**: mark `servicename` as administratively up (READY in agent parlance)
 * **`down servicename`**: mark `servicename` as administratively down (MAINT in agent parlance)
 * **`status servicename`**: show the status of `servicename`
 * **`ping`**: confirm that the socket works
 * **`showall`**: dump all downed services' state as JSON
 * **`quit`**: disconnect from the admin socket


## Logging / Monitoring

If the environment `$LOG_DGRAM_SYSLOG` is set to a domain socket path, this service will log some stuff using the RFC
3164 syslog protocol to that path. Otherwise, if `RUST_LOG` is set, it will log to stdout using the configuration in
`RUST_LOG` according to [env_logger](https://docs.rs/env_logger/0.7.1/env_logger/).
