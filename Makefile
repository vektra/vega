deps:
	wget https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz
	tar zvxf snappy-1.1.1.tar.gz
	cd snappy-1.1.1 && ./configure --prefix=/usr && sudo make install
	wget https://leveldb.googlecode.com/files/leveldb-1.15.0.tar.gz
	tar xzvf leveldb-1.15.0.tar.gz
	cd leveldb-1.15.0 && make && sudo cp -a include/* /usr/include/ && sudo cp -a lib* /usr/lib/

travis:
	wget https://dl.bintray.com/mitchellh/consul/0.5.0_linux_amd64.zip
	unzip 0.5.0_linux_amd64.zip
	./consul agent -server -bootstrap -data-dir=tmp &
	go get -t ./...
	go test ./...
