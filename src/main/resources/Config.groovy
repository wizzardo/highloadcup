server {
    host = '0.0.0.0'
    port = 8080
    ioWorkersCount = 4
    ttl = 60 * 1000
    context = '/'
}
environments {
    prod {
        server.port = 80
    }
    dev {
        server.debugOutput = false
    }
}