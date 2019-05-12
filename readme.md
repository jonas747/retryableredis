# retryableredis

This is a wrapper against radix.Conn which will retry and reconnect on errors.

This is for use cases when you have no other proper way than just retrying, it obviously wont work with transactions.