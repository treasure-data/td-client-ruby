# Treasure Data API library for Ruby
[<img src="https://travis-ci.org/treasure-data/td-client-ruby.svg?branch=master" alt="Build Status" />](https://travis-ci.org/treasure-data/td-client-ruby)
[<img src="https://ci.appveyor.com/api/projects/status/github/treasure-data/td-client-ruby?branch=master&svg=true" alt="Build Status" />](https://ci.appveyor.com/project/treasure-data/td-client-ruby/branch/master)
[<img src="https://coveralls.io/repos/treasure-data/td-client-ruby/badge.svg?branch=master&service=github" alt="Coverage Status" />](https://coveralls.io/github/treasure-data/td-client-ruby?branch=master)

## Getting Started

    > gem install td-client

### Running Tests

    > gem install jeweler
    > gem install webmock
    > rake spec

## Configuration

The Client API library constructor supports a number of options that can
be provided as part of the optional 'opts' hash map to these methods:

* `initialize`(apikey, opts={}) (constructor)
* `Client.authenticate`(user, password, opts={}) (class method)
* `Client.server_status`(opts={}) (class method)

### Endpoint

Add the `:endpoint` key to the opts to provide an alternative endpoint to make
the API calls to. Examples are an alternate API endpoint or a static IP address
provided by Treasure Data on a case-by-case basis.

The default endpoint is:

    https://api.treasuredata.com

and configure communication using SSL encryption over HTTPS; the same happens
every time the provided endpoint is prefixed by 'https'.

E.g.

    opts.merge({:endpoint => "https://api-alternate.treasuredata.com"})

The endpoint can alternatively be provided by setting the `TD_API_SERVER`
environment variable. The `:endpoint` option takes precedence over the
`TD_API_SERVER` environment variable setting.

For communication through a Proxy, please see the Proxy option below.

### Connection, Read, and Send Timeouts

The connection, read, and send timeouts can be provided via the
`:connect_timeout`, `:read_timeout`, `:send_timeout` keys respectively.
The values for these keys is the number of seconds.

E.g.

    opts.merge({:connect_timeout => 60,
                :read_timeout    => 60,
                :send_timeout    => 60})

### SSL

The `:ssl` key specifies whether SSL communication ought to be used when
communicating with the default or custom endpoint.

This option is ignored if the endpoint (default or custom) URL specifies the
scheme (e.g. the protocol, https or http) in which case SSL enabled/disabled is
inferred directly from the URL scheme.

E.g.

    # SSL is enabled as specified by the :ssl option
    opts.merge({:endpoint => "api.treasuredata.com", :ssl => true})

    # the ssl option is ignored in this case
    opts.merge({:endpoint => "https://api.treasuredata.com", :ssl => false})

### Proxy

If your network requires accessing our endpoint through a proxy (anonymous or
private), the proxy settings can be specified through the `:http_proxy` option.

E.g.

    # anonymous proxies
    opts.merge({:http_proxy => "http://myproxy.com:1234"})
    opts.merge({:http_proxy => "myproxy.com:1234"})

    # private proxies
    opts.merge({:http_proxy => "https://username:password@myproxy.com:1234"})
    opts.merge({:http_proxy => "username:password@myproxy.com:1234"})

The proxy settings can alternatively be provided by setting the `HTTP_PROXY`
environment variable. The `:http_proxy` option takes precedence over the
`HTTP_PROXY` environment variable setting.

### Additional Header(s)

The Ruby client configures the communication with the Treasure Data REST API
endpoints using the required HTTP Headers (including authorization, Date,
User-Agent and Accept-Encoding, Content-Length, Content-Encoding where
applicable) basing on what method call is made.

The user can specify any additional HTTP Header using the `:headers` option.

E.g.

    opts.merge({:headers => "MyHeader: myheadervalue;"})

To specify a custom User-Agent please see the option below.

### Additional User-Agent(s)

Add the `:user_agent` key to the opts hash to provide an additional user agent
for all the interactions with the APIs.
The provided user agent string will be added to this default client library user
agent `TD-Client-Ruby: X.Y.Z` where X.Y.Z is the version number of this Ruby
Client library.

E.g.

    opts.merge({:user_agent => "MyApp: A.B.C"})

which sets the user agent to:

    "MyApp: A.B.C; TD-Client-Ruby: X.Y.Z"

### Retry POST Requests

Add the `:retry_post_requests` key to the opts hash to require that every
failed POST request is retried for up to 10 minutes with an exponentially
doubling backoff window just as it happens for GET requests by default.

Note that this can lead to unwanted results since POST request as not always
idempotent, that is retrying a POST request by lead to the creation of
duplicated resources, such as the submission of 2 identical jobs on failures.

As for GET requests, the retrying mechanism is triggered on 500+ HTTP error
codes or the raising of exceptions during the communication with the remote
server.

E.g.

    opts.merge({:retry_post_requests => true})

to enable retrying for POST requests.

## Testing Hooks

The client library implements several hooks to enable/disable/trigger special
behaviors. These hooks are triggered using environment variables:

* Enable debugging mode:

    `$ TD_CLIENT_DEBUG=1`

  Currently debugging mode consists of:

  * show request and response of `HTTP`/`HTTPS` `GET` REST API calls;
  * show request of `HTTP`/`HTTPS` `POST`/`PUT` REST API calls.

## More Information

  * Copyright: (c) 2011 Treasure Data Inc.
  * License: Apache License, Version 2.0
