.e <- new.env()

.run <- function() {
  c = rediscc::redis.connect("hdp", 6379, reconnect=TRUE)
  h = system("hostname", intern=TRUE)
  id = paste(h, Sys.getpid(), sep='-')
  rediscc::redis.push(c, "yarn", id)
  cat("Listenting on queue", id, "\n")
  repeat {
    a <- rediscc::redis.pop(c, id, 5)
    if (isTRUE(a == "quit")) break
  }
  cat("Done with", id, "\n")
}

yarn <- function(n=10, name="yaRn", opts) {
  jar <- .e$ys.jar
  if (is.null(jar)) {
    cp <- system("yarn classpath", intern=TRUE)
    q <- unlist(strsplit(cp, ":", TRUE))
    q <- gsub(".*\\*$", "", q)
    jars <- unique(unlist(lapply(q, function(o) Sys.glob(paste0(q, "/hadoop-yarn-applications-distributedshell-*.jar")))))
    if (!length(jars)) stop("cannot determine JAR location")
    jar <- jars[1L]
  }
  sh <- tempfile(fileext=".sh")
  sink(sh)
  cat('#!/bin/bash\ncat > yarn.tmp.R <<- "EOF"\n.run <- ')
  dput(.run)
  cat('\n\n.run()\nEOF\n\nRscript yarn.tmp.R && rm yarn.tmp.R\n\n')
  sink()

  opts <- if(missing(opts)) '' else {
    if (!is.null(names(opts))) paste(shQuote(names),"=",shQuote(opts),collapse=' ', sep='') else paste(shQuote(opts),collapse=' ')
  }

  system(paste(
    "yarn", "jar", shQuote(jar), "-jar", shQuote(jar),
    "-shell_script", shQuote(sh), "-num_containers", n, "-appname", shQuote(name), opts))
}
