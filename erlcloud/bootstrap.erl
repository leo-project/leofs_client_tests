#!/usr/bin/env escript
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-include_lib("deps/erlcloud/include/erlcloud_aws.hrl").

-define(AWS_ACCESS_KEY, "05236").
-define(AWS_SECRET_KEY, "802562235").
-define(AWS_HOST,       "localhost").
-define(AWS_PORT,       8080).

main(_Args) ->
    ok = code:add_paths(["ebin",
                         "deps/erlcloud/ebin",
                         "deps/jsx/ebin",
                         "deps/meck/ebin",
                         "deps/lhttpc/ebin"]),
    erlcloud:start(),
    Conf = erlcloud_s3:new(?AWS_ACCESS_KEY, 
                           ?AWS_SECRET_KEY,
                           ?AWS_HOST,
                           ?AWS_PORT),
    Conf2 = Conf#aws_config{s3_scheme = "http://"},
    try
        % Create a bucket
        erlcloud_s3:create_bucket("erlang", Conf2),
        % Retrieve list of buckets
        List = erlcloud_s3:list_buckets(Conf2),
        io:format("[debug]buckets:~p~n", [List]),
        % PUT an object into the LeoFS
        erlcloud_s3:put_object("erlang", "test-key", "value", [], Conf2),
        % Retrieve list of objects from the LeoFS
        Objs = erlcloud_s3:list_objects("erlang", Conf2),
        io:format("[debug]objects:~p~n", [Objs]),
        % GET an object from the LeoFS
        Obj = erlcloud_s3:get_object("erlang", "test-key", Conf2),
        io:format("[debug]inserted object:~p~n", [Obj]),
        % GET an non-existing object from the LeoFS
        try
            NotFoundObj = erlcloud_s3:get_object("erlang", "test-key-nonexisting", Conf2),
            io:format("[debug]not found object:~p~n", [NotFoundObj])
        catch
            error:{aws_error,{http_error,404,_,_}} ->
                io:format("[debug]404 not found object~n")
        end,
        

        % Range Get object
        io:format("[debug]Range Get~n",[]),
        ObjRange = erlcloud_s3:get_object("erlang", "test-key", [{range, "bytes=1-4"}], Conf2),
        io:format("[debug]~p~n", [ObjRange]),
        <<"alue">> = proplists:get_value(content, ObjRange),

        % GET an object metadata from the LeoFS
        Meta = erlcloud_s3:get_object_metadata("erlang", "test-key", Conf2),
        io:format("[debug]metadata:~p~n", [Meta]),
        % DELETE an object from the LeoFS
        DeletedObj = erlcloud_s3:delete_object("erlang", "test-key", Conf2),
        io:format("[debug]deleted object:~p~n", [DeletedObj]),
        try
            NotFoundObj2 = erlcloud_s3:get_object("erlang", "test-key", Conf2),
            io:format("[debug]not found object:~p~n", [NotFoundObj2])
        catch
            error:{aws_error,{http_error,404,_,_}} ->
                io:format("[debug]404 not found object~n")
        end
    after
        % DELETE a bucket from the LeoFS
        ok = erlcloud_s3:delete_bucket("erlang", Conf2)
    end,
    ok.
