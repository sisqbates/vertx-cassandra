# Cassandra client

An asynchronous client for interacting with a Cassandra database

This is a work in progress, so expect fireworks when using it...

## Testing

The tests require a running Cassandra cluster at localhost. 

You can easily setup a cluster using [ccm](https://github.com/pcmanus/ccm)

For example: 
`ccm create -n 1 vertx_cassandra -v 2.1.7` will setup a cluster with a single instance and then `ccm start vertx_cassandra` will start it.

As you can guess, use `ccm stop vertx_cassandra` to stop the cluster.

## License

This project is released under the Apache License 2.0 