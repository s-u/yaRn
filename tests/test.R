library(yaRn)
osrv.start()
os.pop("foo")
os.push("foo", charToRaw("bar"))
os.pop("foo", TRUE)
osrv.ask(,, 'g', 'foo')
osrv.ask(,, 's', 'foo', 'gee')
os.pop("foo", TRUE)
os.pop("foo")
os.pop("foo")
os.pop("foo")
osrv.ask(,, 's', 'foo', 'bar')
os.pop("foo", TRUE)
osrv.ask(,, 'g', 'foo')
os.pop("foo", TRUE)
osrv.ask(,, 'r', 'foo')
os.pop("foo")
