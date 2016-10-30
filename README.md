# kafka zookeeper client

library for lookup kafka brokers in round robin way from zookeeper server(s).


### install

with rebar:

```sh
p@Jahanbakhsh ~/projects/kafka_zookeeper_client $ rebar g-d
# gets its dependencies
# ...
p@Jahanbakhsh ~/projects/kafka_zookeeper_client $ rebar compile
# compiles all
# ...
```


### example

open erlang shell in project root directory:

```sh
p@Jahanbakhsh ~/projects/kafka_zookeeper_client $ erl -pa ./ebin -pa ./deps/*/ebin
Erlang R16B03 (erts-5.10.4) [source] [64-bit] [smp:8:8] [async-threads:10] [kernel-poll:false]

Eshell V5.10.4  (abort with ^G)
1>
```

get broker address with:

```erlang
1> ZookeeperAddresses = [{"192.168.120.81", 2181}, {"192.168.120.82", 2181}],
   ZookeeperConnectTimeOut = 2000, %% mili seconds
   ErlzkOpts = [], %% Options for erlzk library, see erlzk module for more info
   ErlzkArgs = {ZookeeperAddresses, ZookeeperConnectTimeOut, ErlzkOpts}.
...
2> BrokersDir = "/kafka/brokers/ids", %% brokers directory in zookeeper server(s)
   BrokersIdDir = "/brokers/esid", %% directory for creating ephemeral sequential id
   Debug = {debug_options, [trace]}. %% will prints results in screen all sys module debug options can be set
...
3> kafka_zookeeper_client:get_broker_address(ErlzkArgs, BrokersDir, BrokersIdDir, Debug).
**DEBUG** 2016/10/30 12:42:33
info    
connected to zookeeper server
    host:{192,168,120,81}
    port:2181
    
    
**DEBUG** 2016/10/30 12:42:33
info    
brokers found
    total:3
    
    
**DEBUG** 2016/10/30 12:42:33
warning 
broker id directory not found
    broker_id_directory:"/brokers/esid"
    
    
**DEBUG** 2016/10/30 12:42:33
info    
new dricetory created
    directory:"/brokers/esid"
    
    
**DEBUG** 2016/10/30 12:42:33
info    
'ephemeral_sequetional' id was created
    ephemeral_sequential:"/brokers/esid0000000001"
    
    
**DEBUG** 2016/10/30 12:42:33
info    
broker selected
    index:1
    
    
{address,{"192.168.120.85",9092}}
4>
```

there were 3 brokers and we got first broker, in next call we will get second broker and so on.
    
