#!/usr/bin/env escript
%% -*- mode: erlang;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-include_lib("deps/erlcloud/include/erlcloud_aws.hrl").

-define(AWS_ACCESS_KEY, "05236").
-define(AWS_SECRET_KEY, "802562235").
-define(AWS_HOST,       "localhost").
-define(AWS_PORT,       8080).
-define(LARGE_OBJ_SIZE, 52428800).
-define(LARGE_FILE_PATH, "../temp_data/testFile.large").
-define(CHUNK_SIZE,     5242880).

main(_Args) ->
    ok = code:add_paths(["ebin",
                         "deps/erlcloud/ebin",
                         "deps/jsx/ebin",
                         "deps/meck/ebin",
                         "deps/lhttpc/ebin"]),
    ssl:start(),
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

        LargeObj = case filelib:is_regular(?LARGE_FILE_PATH) of
                       true ->
                           io:format("[debug] Read File~n"),
                           {ok, Bin} = file:read_file(?LARGE_FILE_PATH),
                           Bin;
                       false ->
                           io:format("[debug] Write File~n"),
                           Bin = crypto:rand_bytes(?LARGE_OBJ_SIZE),
                           ok = file:write_file(?LARGE_FILE_PATH, Bin),
                           Bin
                   end,

        % PUT Single-Part Large Object
        erlcloud_s3:put_object("erlang", "test-key.large.one", LargeObj, [], Conf2),

        % PUT Multi-Part Large Object
        {ok, MPResult} = erlcloud_s3:start_multipart("erlang", "test-key.large.part", [], [], Conf2),
        UploadId = proplists:get_value(uploadId, MPResult),
        io:format("[debug]UploadId:~p~n", [UploadId]),
        {ok, Etags} = upload_parts("erlang", "test-key.large.part", UploadId, LargeObj, [], Conf2),
        erlcloud_s3:complete_multipart("erlang", "test-key.large.part", UploadId, Etags, [], Conf2),

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

        BaseArr = binary:part(LargeObj, 1048576, 10485760 - 1048576 + 1),
        % Range Get Single-Part Large Object
        ObjRange1 = erlcloud_s3:get_object("erlang", "test-key.large.one", [{range, "bytes=1048576-10485760"}], Conf2),
        BaseArr = proplists:get_value(content, ObjRange1),

        % Range Get Multi-Part Large Object
        ObjRange1 = erlcloud_s3:get_object("erlang", "test-key.large.part", [{range, "bytes=1048576-10485760"}], Conf2),
        BaseArr = proplists:get_value(content, ObjRange1),

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

upload_parts(Bucket, Key, UploadId, PartNum, Bin, Headers, Config, Acc) when byte_size(Bin) >= ?CHUNK_SIZE ->
    <<Part:?CHUNK_SIZE/binary, Rest/binary>> = Bin,
    io:format("Upload Part: ~p~n", [PartNum]),
    {ok, Ret} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, Part, Headers, Config),
    Etag = proplists:get_value(etag, Ret),
    upload_parts(Bucket, Key, UploadId, PartNum + 1, Rest, Headers, Config, [{PartNum, Etag}|Acc]);
upload_parts(Bucket, Key, UploadId, PartNum, Bin, Headers, Config, Acc) ->
    {ok, Ret} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, Bin, Headers, Config),
    Etag = proplists:get_value(etag, Ret),
    {ok, [{PartNum, Etag}|Acc]}.

upload_parts(Bucket, Key, UploadId, LargeObj, Headers, Config) ->
    upload_parts(Bucket, Key, UploadId, 1, LargeObj, Headers, Config, []).

