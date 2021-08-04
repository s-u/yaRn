osrv.start <- function (host=NULL, port=9013L, threads=4L) .Call(C_start, host, port, threads)
osrv.ask <- function(host="127.0.0.1", port=9013L, cmd, key, value=NULL) .Call(C_ask, host, port, cmd, key, value)
