# Transports

This version of `mercure`, in addition to the default `bolt` transport, implements the following
transports: `gdbm`, `redis`, and `postgres`.  Each of these transports provides a built-in cleanup
routine which, when enabled, wakes up on predefined intervals and removes from the database events
older than a certain timestamp.  These two values - wake up interval and event time-to-live - are
configured by the following parameters in the transport URL:

<a name="cleanup_parameters"></a>
* `cleanup_interval=` _DURATION_

Sets interval between two successive database cleanups.  If not set, periodic database cleanup is disabled.

* `event_ttl=` _DURATION_

Sets event time-to-live.  Default is 24 hours.

The _DURATION_ argument must be a valid input to [time.ParseDuration](https://pkg.go.dev/time#ParseDuration).

For example, the following transport URL requires the use of `redis` database and performs database cleanup
each 2 hours.  During cleanup, events older than 12 hours are evicted:

```
   redis://hostname?cleanup_interval=2h&event_ttl=12h
```

The sections below discuss each transport type in detail

## GDBM transport

This transport uses [GNU dbm](http://www.gnu.org.ua/software/gdbm) file storage.  The URL has the form:

```
  gdbm://FILE[?PARAM]
```

where _FILE_ is the database file name (either absolute, starting with a slash, or relative to the
current working directory), and _PARAM_ are database [cleanup parameters](#user-content-cleanup_parameters).

## Postgres transport

This transport uses a [PostgreSQL](https://www.postgresql.org/) database.  The URL is a
[connection URI](https://www.postgresql.org/docs/current/libpq-connect.html#id-1.7.3.8.3.6) with
optional [cleanup parameters](#user-content-cleanup_parameters) in its query part.

To use this transport follow the steps below:

1. Create the database `mercure` and the user to access it.

2. Connect to the database and create the table `events`:

```SQL
CREATE TABLE events (
  id varchar(64) NOT NULL,
  ts timestamp NOT NULL DEFAULT NOW(),
  message text
);

CREATE UNIQUE INDEX event_id ON events (id);
CREATE INDEX event_ts ON events (ts);
```

3. Configure the `transport_url` parameter (or `MERCURE_TRANSPORT_URL` environment
variable, if using the mercure container), e.g. (assuming database name `mercure`, user name `user` and
password `guessme`):

  ```conf
    transport_url "postgres://user:guessme@localhost/mercure&cleanup_interval=6h"
  ```

## Redis transport

This transport uses a remote [redis](https://redis.io/) database.  It requires Redis version 6.2.7 or newer.

To use redis database, you need to do the following.

1. Start up a redis server

2. In the `mercure` configuration, set the `transport_url` parameter to the _URL_ of your server.
If you are using `mercure` docker container, specify the URL in the `MERCURE_TRANSPORT_URL` environment
variable.

### URL

Redis database can be accessed via TCP/IP or via a UNIX socket.  This is defined by the _scheme_ part
of the database URL.  There are three possible URLs:

* Plaintext TCP

  ```
   redis://[USER[:PASS]@]HOST:[PORT][/DBNUM][?PARAM]
  ```

* TLS transport

  ```
   rediss://[USER[:PASS]@]HOST:[PORT][/DBNUM][?PARAM]
  ```

* UNIX socket

  ```
   redis+unix://[USER[:PASS]@]PATH[?PARAM]
  ```

Common parts for all URLs are:

* _USER_

User name.

* _PASS_

User password.

* _HOST_

Host name or IP address of the server running the `redis` server.

* _PORT_

Port number to use.  This defaults to 6379.

* _DBNUM_

Number of the redis database to use.

* _PATH_

Absolute pathname of the UNIX socket file.

* _PARAM_

Additional parameters.  These fall into two categories: _Mercure-specific_ and _generic Redis_ parameters.

### Mercure-specific parameters

URL parameters specific for the `mercure` redis transport are cleanup parameters [discussed above](#user-content-cleanup_parameters) plus the following:

* `stream=` _NAME_

Sets the name of the [redis stream](https://redis.io/docs/manual/data-types/streams/).  Default value
is `mercure`.

### Generic Redis parameters

* `max_retries=` _INT_

Maximum number of retries before giving up connection.  This defaults to 3.  The value of -1
disables retrying.

* `min_retry_backoff=` _DURATION_

Minimum backoff interval between each retry.  The value of this parameter must be a valid
[duration](https://pkg.go.dev/time#ParseDuration).  The default is 8 milliseconds.  -1 disables
backoff.

* `max_retry_backoff=` _DURATION_

Maximum backoff interval between each retry.

* `dial_timeout=` _DURATION_

Dial timeout for establishing new connections.  Default is 5 seconds.

* `read_timeout=` _DURATION_

Timeout for socket reads. If reached, commands will fail with a timeout instead of blocking.
The value -1 means no timeout.  Default is 3 seconds.

* `write_timeout=` _DURATION_

Timeout for socket writes. If reached, commands will fail with a timeout instead of blocking.  Default
is the value of `read_timeout` parameter.

* `pool_fifo=` _BOOL_

Type of connection pool: `true` for FIFO, `false` (default) for LIFO.

* `pool_size=` _INT_

Maximum number of socket connections.  Default is 10 connections per every available CPU.

* `min_idle_conns=` _INT_

Minimum number of idle connections which is useful when establishing new connection is slow.

* `max_conn_age=` _DURATION_

Connection age at which client retires (closes) the connection.  Default is to never close
aged connections.

* `pool_timeout=` _DURATION_

Amount of time client waits for connection if all connections are busy before returning an error.
Default is `read_timeout` + 1 second.

* `idle_timeout=` _DURATION_

Amount of time after which client closes idle connections.  Should be less than server's timeout.
Default is 5 minutes. -1 disables idle timeout check.

* `idle_check_frequency=` _DURATION_

Frequency of idle checks made by idle connections reaper.  Default is 1 minute.  -1 disables idle
connections reaper, but idle connections are still discarded by the client if `idle_timeout` is set.

* `read_only=` _BOOL_

Enables read-only mode when querying slave nodes.
