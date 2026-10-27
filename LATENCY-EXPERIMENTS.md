# Kalshi latency experiments

I tried two small things to improve connection speed. Each test
made three sequential, unauthenticated `curl` requests. These numbers measure
the time until an HTTP response, not an authenticated WebSocket connection or
the first order-book message. Each endpoint returned HTTP 401, as expected.

## Trying Kalshi's supported hosts

Kalshi recommends the `external-api-ws` hosts and also supports the older
`api.elections` / `demo-api` hosts. I measured time to first byte:

| Environment | Recommended host median | Other supported host median | Faster in this test |
|---|---:|---:|---|
| Production | 101 ms | **76 ms** (`api.elections`) | `api.elections` |
| Demo | 237 ms | **148 ms** (`demo-api`) | `demo-api` |

**Fastest option by median:** the supported production `api.elections` host,
at 76 ms to first byte. The fastest individual request was 69 ms on that same
host. These are only three samples per host, so this is a result for this
machine and network—not a general ranking of Kalshi endpoints.

## Trying explicit source ports

I compared three requests using local ports `41001`–`41003` with three using
the OS-selected port, against the recommended production host:

| Port selection | TCP connect range | TLS complete range | First-byte range |
|---|---:|---:|---:|
| Explicit | 22–24 ms | 55–63 ms | 75–83 ms |
| OS-selected | 24–37 ms | 52–74 ms | 72–107 ms |

**Forcing a source port did not make this measurably faster.** The ranges
overlap, and the remote IP changed between requests, so the port wasn't the
only changing factor.

Sources: [Kalshi's documented endpoint list](https://docs.kalshi.com/getting_started/api_environments);
[curl timing fields](https://curl.se/docs/manpage.html#-w).
