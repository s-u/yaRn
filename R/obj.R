os.pop <- function(key, peek=FALSE) .Call(C_get, key, !peek)
os.push <- function(key, what) .Call(C_put, key, what)
