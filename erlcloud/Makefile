.PHONY: deps compile exec clean xref eunit

all: deps compile exec

deps:
	@./rebar get-deps

compile:
	@./rebar compile

exec:
	./bootstrap.erl

clean:
	@./rebar clean

xref:
	@./rebar xref

eunit:
	@./rebar eunit


