# retryableredis

This is a wrapper against radix.Conn which will retry and reconnect on errors.

This is for use cases when you have no other proper way than just retrying, it obviously wont work with transactions.

Note that you also have to use the wrapped Cmd types when doing this as the underlying actions can't be reused after unmarshal has been called

**Pipelies are also unsupported using this**