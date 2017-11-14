#!/usr/bin/env escript
%% -*- mode: erlang,erlang-indent-level: 4,indent-tabs-mode: nil -*-
%% ex: ft=erlang ts=4 sw=4 et

-include_lib("deps/erlcloud/include/erlcloud_aws.hrl").

-define(HOST,       "localhost").
-define(PORT,       "8080").

-define(ACCESS_KEY_ID       , "05236").
-define(SECRET_ACCESS_KEY   , "802562235").
-define(SIGN_VER            , v4).

-define(BUCKET      , "teste").
-define(TEMPDATA    , "../temp_data/").

-define(SMALL_TEST_F    , ?TEMPDATA++"testFile").
-define(MEDIUM_TEST_F   , ?TEMPDATA++"testFile.medium").
-define(LARGE_TEST_F    , ?TEMPDATA++"testFile.large").

-define(METADATA_KEY    , "cmeta_key").
-define(METADATA_VAL    , "cmeta_val").

-define(CHUNK_SIZE,     10485760).

main(Args)->
    ok = code:add_paths(["ebin",
                         "deps/idna/ebin",
                         "deps/mimerl/ebin",
                         "deps/certifi/ebin",
                         "deps/metrics/ebin",
                         "deps/ssl_verify_fun/ebin",
                         "deps/hackney/ebin",
                         "deps/erlcloud/ebin",
                         "deps/jsx/ebin",
                         "deps/meck/ebin",
                         "deps/lhttpc/ebin",
                         "deps/base16/ebin",
                         "deps/leo_commons/ebin/"]),
    [SignVer, Host, Port_S, Bucket] = case length(Args) of
                                          0 ->
                                              [?SIGN_VER, ?HOST, ?PORT, ?BUCKET];
                                          _ ->
                                              Args
                                      end,
    hackney:start(),
    erlcloud:start(),

    MetadataMap = [{?METADATA_KEY, ?METADATA_VAL}],
    Port = list_to_integer(Port_S),
    init(SignVer, Host, Port),
    createBucket(Bucket),

    %% Put Object Test
    putObject(Bucket, "test.simple",    ?SMALL_TEST_F),
    putObject(Bucket, "test.medium",    ?MEDIUM_TEST_F),
    putObject(Bucket, "test.large",     ?LARGE_TEST_F),

    %% Put Object with Metadata Test
    putObjectWithMetadata(Bucket, "test.simple.meta", ?SMALL_TEST_F, MetadataMap),
    putObjectWithMetadata(Bucket, "test.large.meta", ?LARGE_TEST_F, MetadataMap),

    %% Multipart Upload Test
    mpObject(Bucket, "test.simple.mp",  ?SMALL_TEST_F),
    mpObject(Bucket, "test.large.mp",   ?LARGE_TEST_F),

    %% Object Metadata Test
    headObject(Bucket, "test.simple",   ?SMALL_TEST_F),
    headObject(Bucket, "test.large",    ?LARGE_TEST_F),
%% MP File ETag != MD5
%%    headObject(Bucket, "test.simple.mp", ?SMALL_TEST_F),
%%    headObject(Bucket, "test.large.mp", ?LARGE_TEST_F),

    %% Get Object Test
    getObject(Bucket, "test.simple",    ?SMALL_TEST_F),
    getObject(Bucket, "test.simple.mp", ?SMALL_TEST_F),
    getObject(Bucket, "test.medium",    ?MEDIUM_TEST_F),
    getObject(Bucket, "test.large",     ?LARGE_TEST_F),
    getObject(Bucket, "test.large.mp",  ?LARGE_TEST_F),

    %% Get Object Again (Cache) Test
    getObject(Bucket, "test.simple",    ?SMALL_TEST_F),
    getObject(Bucket, "test.simple.mp", ?SMALL_TEST_F),
    getObject(Bucket, "test.medium",    ?MEDIUM_TEST_F),
    getObject(Bucket, "test.large",     ?LARGE_TEST_F),

    %% Get Object with Metadata Test
    getObjectWithMetadata(Bucket, "test.simple.meta", ?SMALL_TEST_F, MetadataMap),
    getObjectWithMetadata(Bucket, "test.large.meta", ?LARGE_TEST_F, MetadataMap),

    %% Get Not Exist Object Test
    getNotExist(Bucket, "test.noexist"),

    %% Range Get Object Test
    rangeObject(Bucket, "test.simple",      ?SMALL_TEST_F, 1, 4),
    rangeObject(Bucket, "test.simple.mp",   ?SMALL_TEST_F, 1, 4),
    rangeObject(Bucket, "test.large",       ?LARGE_TEST_F, 1048576, 10485760),
    rangeObject(Bucket, "test.large.mp",    ?LARGE_TEST_F, 1048576, 10485760),
    rangeObject(Bucket, "test.large.mp",    ?LARGE_TEST_F, 31457280, 41943040),
    rangeObject(Bucket, "test.large.mp",    ?LARGE_TEST_F, 41943040, 52420000),

    %% Copy Object Test
    copyObject(Bucket, "test.simple", "test.simple.copy"),
    getObject(Bucket, "test.simple.copy", ?SMALL_TEST_F),

    %% List Object Test
    listObject(Bucket, [], -1),

    %% Delete All Object Test
    deleteAllObjects(Bucket),
    listObject(Bucket, [], 0),

    %% Multiple Page List Object Test
    putDummyObjects(Bucket, "list/", 35, ?SMALL_TEST_F),
    pageListBucket(Bucket, "list/", 35, 10),

%% erlcloud does not have config for delete_objects_batch
%%    %% Multiple Delete
%%    multiDelete(Bucket, "list/", 10),

%% erlcloud does not support Canned ACL
%%    %% GET-PUT ACL
%%    setBucketAcl(Bucket, "private"),
%%    setBucketAcl(Bucket, "public-read"),
%%    setBucketAcl(Bucket, "public-read-write"),
    deleteBucket(Bucket),
    ok.

init(_SignVer, Host, Port) ->
    Conf = erlcloud_s3:new(
             ?ACCESS_KEY_ID,
             ?SECRET_ACCESS_KEY,
             Host,
             Port),
    Conf2 = Conf#aws_config{s3_scheme = "http://"},
    put(s3, Conf2).

createBucket(BucketName) ->
    Conf = get(s3),
    io:format("===== Create Bucket [~s] Start =====\n", [BucketName]),
    erlcloud_s3:create_bucket(BucketName, Conf),
    io:format("===== Create Bucket End =====\n"),
    io:format("\n"),
    ok.

deleteBucket(BucketName) ->
    Conf = get(s3),
    io:format("===== Delete Bucket [~s] Start =====\n", [BucketName]),
    erlcloud_s3:delete_bucket(BucketName, Conf),
    io:format("===== Delete Bucket End =====\n"),
    io:format("\n"),
    ok.

putObject(BucketName, Key, Path) ->
    Conf = get(s3),
    io:format("===== Put Object [~s/~s] Start =====\n", [BucketName, Key]),
    {ok, Bin} = file:read_file(Path),
    erlcloud_s3:put_object(BucketName, Key, Bin, [], Conf),

    case doesObjectExist(BucketName, Key) of
        false ->
           io:format("Put Object [~s/~s] Failed!\n", [BucketName, Key]),
           throw(error);
        true ->
            ok
    end,

    io:format("===== Put Object End =====\n"),
    io:format("\n"),
    ok.

putObjectWithMetadata(BucketName, Key, Path, MetaMap) ->
    Conf = get(s3),
    io:format("===== Put Object [~s/~s] with Metadata Start =====\n", [BucketName, Key]),
    {ok, Bin} = file:read_file(Path),
    erlcloud_s3:put_object(BucketName, Key, Bin, [{meta, MetaMap}], Conf),

    case doesObjectExist(BucketName, Key) of
        false ->
           io:format("Put Object [~s/~s] with Metadata Failed!\n", [BucketName, Key]),
           throw(error);
        true ->
            ok
    end,

    io:format("===== Put Object with Metadata End =====\n"),
    io:format("\n"),
    ok.

mpObject(BucketName, Key, Path) ->
    Conf = get(s3),
    io:format("===== Multipart Upload Object [~s/~s] Start =====\n", [BucketName, Key]),
    {ok, Bin} = file:read_file(Path),
    {ok, MP} = erlcloud_s3:start_multipart(BucketName, Key, [], [], Conf),
    UploadId = proplists:get_value(uploadId, MP),
    {ok, Etags} = uploadParts(BucketName, Key, UploadId, Bin, [], Conf),
    erlcloud_s3:complete_multipart(BucketName, Key, UploadId, Etags, [], Conf),

    case doesObjectExist(BucketName, Key) of
        false ->
           io:format("Multipart Upload Object [~s/~s] Failed!\n", [BucketName, Key]),
           throw(error);
        true ->
            ok
    end,

    io:format("===== Multipart Upload Object End =====\n"),
    io:format("\n"),
    ok.

headObject(BucketName, Key, Path) ->
    Conf = get(s3),
    io:format("===== Head Object [~s/~s] Start =====\n", [BucketName, Key]),
    Meta = erlcloud_s3:get_object_metadata(BucketName, Key, Conf),
    ETag = string:substr(proplists:get_value(etag, Meta), 2, 32),
    CL = list_to_integer(proplists:get_value(content_length, Meta)),
    {ok, Bin} = file:read_file(Path),
    MD5 = leo_hex:binary_to_hex(crypto:hash(md5, Bin)),
    FileSize = byte_size(Bin),
    io:format("ETag: ~s, Size: ~p\n", [ETag, CL]),
    if ETag =:= MD5, CL =:= FileSize ->
           ok;
       true ->
           io:format("Metadata [~s/~s] NOT Match, Size: ~p, MD5: ~s\n", [BucketName, Key, FileSize, MD5]),
           throw(error)
    end,
    io:format("===== Head Object End =====\n"),
    io:format("\n"),
    ok.

getObject(BucketName, Key, Path) ->
    Conf = get(s3),
    io:format("===== Get Object [~s/~s] Start =====\n", [BucketName, Key]),
    Obj = erlcloud_s3:get_object(BucketName, Key, Conf),
    Content = proplists:get_value(content, Obj),
    {ok, Bin} = file:read_file(Path),
    if Content =:= Bin ->
           ok;
       true ->
           io:format("Content NOT Match!\n"),
           throw(error)
    end,
    io:format("===== Get Object End =====\n"),
    io:format("\n"),
    ok.

getObjectWithMetadata(BucketName, Key, Path, MetaMap) ->
    Conf = get(s3),
    io:format("===== Get Object [~s/~s] with Metadata Start =====\n", [BucketName, Key]),
    Obj = erlcloud_s3:get_object(BucketName, Key, Conf),
    lists:foreach(fun({MKey, Val}) ->
                          Val = proplists:get_value("x-amz-meta-" ++ MKey, Obj)
                  end, MetaMap),
    Content = proplists:get_value(content, Obj),
    {ok, Bin} = file:read_file(Path),
    if Content =:= Bin ->
           ok;
       true ->
           io:format("Content NOT Match!\n"),
           throw(error)
    end,
    io:format("===== Get Object with Metadata End =====\n"),
    io:format("\n"),
    ok.

getNotExist(BucketName, Key) ->
    Conf = get(s3),
    io:format("===== Get Not Exist Object [~s/~s] Start =====\n", [BucketName, Key]),
    try
        erlcloud_s3:get_object(BucketName, Key, Conf),
        io:format("Should NOT Exist!\n"),
        throw(error)
    catch
        error:{aws_error,{http_error, 404, _, _}} ->
            ok;
        error:{aws_error,{http_error, 403, _, _}} ->
            ok
    end,
    io:format("===== Get Not Exist Object End =====\n"),
    io:format("\n"),
    ok.

rangeObject(BucketName, Key, Path, Start, End) ->
    Conf = get(s3),
    io:format("===== Range Get Object [~s/~s] (~p-~p) Start =====\n", [BucketName, Key, Start ,End]),
    RangeStr = io_lib:format("bytes=~p-~p", [Start, End]),
    Obj = erlcloud_s3:get_object(BucketName, Key, [{range, RangeStr}], Conf),
    Content = proplists:get_value(content, Obj),
    {ok, Bin} = file:read_file(Path),
    Len = End - Start + 1,
    <<_:Start/binary, Part:Len/binary, _/binary>> = Bin,
    if Content =:= Part ->
           ok;
       true ->
           io:format("Content NOT Match!\n"),
           throw(error)
    end,
    io:format("===== Get Object End =====\n"),
    io:format("\n"),
    ok.

copyObject(BucketName, Src, Dst) ->
    Conf = get(s3),
    io:format("===== Copy Object [~s/~s] -> [~s/~s] Start =====\n", [BucketName, Src, BucketName, Dst]),
    erlcloud_s3:copy_object(BucketName, Dst, BucketName, Src, Conf),
    io:format("===== Copy Object End =====\n"),
    io:format("\n"),
    ok.

listObject(BucketName, Prefix, Expected) ->
    Conf = get(s3),
    io:format("===== List Objects [~s/~s*] Start =====\n", [BucketName, Prefix]),
    Options = case Prefix of
                  [] ->
                      [];
                  _ ->
                      [{prefix, Prefix}]
              end,
    Res = erlcloud_s3:list_objects(BucketName, Options, Conf),
    ObjList = proplists:get_value(contents, Res),
    Count = lists:foldl(
              fun(Obj, Acc) ->
                      ETag = string:substr(proplists:get_value(etag, Obj), 2, 32),
                      Size = proplists:get_value(size, Obj),
                      Key = proplists:get_value(key, Obj),
                      case doesObjectExist(BucketName, Key) of
                          true ->
                              io:format("~s \t Size: ~p\n", [ETag, Size]),
                              Acc + 1;
                          false ->
                              Acc
                      end
              end, 0, ObjList),
    if Count =:= Expected ->
           ok;
       Expected >= 0 ->
           io:format("Number of Objects NOT Match!\n"),
           throw(error);
       true ->
           ok
    end,
    io:format("===== List Objects End =====\n"),
    io:format("\n"),
    ok.

deleteAllObjects(BucketName) ->
    Conf = get(s3),
    io:format("===== Delete All Objects [~s] Start =====\n", [BucketName]),
    Res = erlcloud_s3:list_objects(BucketName, Conf),
    ObjList = proplists:get_value(contents, Res),
    lists:foreach(fun(Obj) ->
                      Key = proplists:get_value(key, Obj),
                      erlcloud_s3:delete_object(BucketName, Key, Conf)
                  end, ObjList),
    io:format("===== Delete All Objects End =====\n"),
    io:format("\n"),
    ok.

putDummyObjects(BucketName, Prefix, Total, Holder) ->
    Conf = get(s3),
    {ok, Bin} = file:read_file(Holder),
    lists:foreach(fun(I) ->
                          Key = Prefix ++ integer_to_list(I),
                          erlcloud_s3:put_object(BucketName, Key, Bin, [], Conf)
                  end, lists:seq(1,Total)).

pageListBucket(BucketName, Prefix, Total, PageSize) ->
    io:format("===== Multiple Page List Objects [~s/~s*] Start =====\n", [BucketName, Prefix]),
    Count = getPage(BucketName, Prefix, 0, [], PageSize),
    io:format("===== End =====\n"),
    if Count =:= Total ->
           ok;
       true ->
           io:format("Number of Objects NOT Match!\n"),
           throw(error)
    end,
    io:format("===== Multiple Page List Objects End =====\n"),
    io:format("\n"),
    ok.

%%multiDelete(BucketName, Prefix, Total) ->
%%    Conf = get(s3),
%%    io:format("===== Multiple Delete Objects [~s/~s] Start =====\n", [BucketName, Prefix]),
%%    DelKeyList = lists:foldl(fun(I, Acc) ->
%%                                     Key = Prefix ++ integer_to_list(I),
%%                                     [Acc | Key]
%%                             end, [], lists:seq(1, Total)),
%%    Res = erlcloud_s3:delete_objects_batch(BucketName, DelKeyList, Conf),
%%
%%    io:format("===== Multiple Delete Objects End =====\n"),
%%    io:format("\n"),
%%    ok.
%%
getPage(BucketName, Prefix, Count, Marker, PageSize) ->
    Conf = get(s3),
    Options = [{max_keys, PageSize}],
    Options2 = case Prefix of
                   [] ->
                       Options;
                   _ ->
                       Options ++ [{prefix, Prefix}]
               end,
    Options3 = case Marker of
                   [] ->
                       Options2;
                   _ ->
                       Options2 ++ [{marker, Marker}]
               end,
    Res = erlcloud_s3:list_objects(BucketName, Options3, Conf),
    io:format("===== Page =====\n"),
    ObjList = proplists:get_value(contents, Res),
    Inc = lists:foldl(fun(Obj, Acc) ->
                              ETag = string:substr(proplists:get_value(etag, Obj), 2, 32),
                              Size = proplists:get_value(size, Obj),
                              Key = proplists:get_value(key, Obj),
                              case doesObjectExist(BucketName, Key) of
                                  true ->
                                      io:format("~s \t Size: ~p \t Count: ~p\n", [ETag, Size, Acc + Count + 1]),
                                      Acc + 1;
                                  false ->
                                      Acc
                              end
                      end, 0, ObjList),
    case proplists:get_value(is_truncated, Res) of
        false ->
            Count + Inc;
        true ->
            LastRec = lists:last(ObjList),
            NextMarker = proplists:get_value(key, LastRec),
            getPage(BucketName, Prefix, Count + Inc, NextMarker, PageSize)
    end.

doesObjectExist(BucketName, Key) ->
    Conf = get(s3),
    try
        erlcloud_s3:get_object_metadata(BucketName, Key, Conf),
        true
    catch
        error:{aws_error,{http_error, 404, _, _}} ->
            false;
        error:{aws_error,{http_error, 403, _, _}} ->
            false
    end.


uploadParts(Bucket, Key, UploadId, LargeObj, Headers, Config) ->
    uploadParts(Bucket, Key, UploadId, 1, LargeObj, Headers, Config, []).

uploadParts(Bucket, Key, UploadId, PartNum, Bin, Headers, Config, Acc) when byte_size(Bin) >= ?CHUNK_SIZE ->
    <<Part:?CHUNK_SIZE/binary, Rest/binary>> = Bin,
    {ok, Ret} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, Part, Headers, Config),
    Etag = proplists:get_value(etag, Ret),
    uploadParts(Bucket, Key, UploadId, PartNum + 1, Rest, Headers, Config, [{PartNum, Etag}|Acc]);
uploadParts(Bucket, Key, UploadId, PartNum, Bin, Headers, Config, Acc) ->
    {ok, Ret} = erlcloud_s3:upload_part(Bucket, Key, UploadId, PartNum, Bin, Headers, Config),
    Etag = proplists:get_value(etag, Ret),
    {ok, [{PartNum, Etag}|Acc]}.
