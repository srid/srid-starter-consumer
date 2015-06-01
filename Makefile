PWD := $(shell pwd)

all:	build run
	@true

build:
	godep go build

run:
	PATH=${PWD}:${PATH} forego start

deploy:
	git push heroku
