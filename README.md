# spring-cloud-consul-cluster
该模块为解决spring-cloud-consul(Config-服务配置、Registry-服务注册、Discovery-服务发现)中ConsulClient单点故障问题而开发的高可用集群版ConsulClient.

# 前言
使用consul作为服务配置、服务注册、服务发现中间件的应用程序都不可避免的遇到consul客户端单点故障问题，
此模块即是为解决该问题而生！
该模块相比于引入Nginx/HAProxy等负载均衡中间件的方式更为可靠和灵活，
因为引入它们，它们自身也会出现单点故障问题，
同时它们的属于负载均衡机制，灵活性不可控！
但是我还是想说：明白了consul的架构及设计初衷，
就应该抛弃类似通过Nginx/HAProxy等负载均衡的方式(包括此模块)来解决consul客户端单点故障问题！
为每个(或者一簇)应用程序部署一个consul client节点，该consul client加入到server集群中去，它们必须部署在一起(一台主机或容器)，
即应用程序与consul client共生死！这就是consul的使用守则(sidecar模式)!

# 应用场景
**该模块仅仅在springcloud微服务跟consul节点不是部署在一起的情况下使用**，
consul的使用守则是应用程序与consul client共生死(部署在一起,应用程序连接与自己部署在一起的那个consul节点)，
这种情况下就没必要使用该模块的必要了！

# 设计思路
### 在springcloud微服务与consul节点不是部署在一起的情况下，
### springcloud微服务连接的那个consul节点单点故障问题不可避免，
### 一旦出现单点故障将出现以下问题：

* 微服务本身仍然可用，
 只是ConsulCatalogWatch、ConfigWatch、ServerListUpdater等内部的定时调用ConsulClient的相关方法(getCatalogServices(),getKVValues(),getHealthServices())将会狂报错，但应用仍然可用不会因此而崩溃。

* consul节点挂了，如果该节点是client节点那么在consul集群中将看不见该serviceId的注册信息，
 这里的看不见是指consul ui中看不见了，ConsulClient.getHealthServices()也看不见！
 如果该节点是server节点那么在consul集群中还是能看见该serviceId的注册信息，
 只不过该服务的健康状态是不健康的！
 不管注册的是哪种consul类型的节点，
 对服务的healthcheck因为consul节点挂了而终止了对该serviceId服务的健康检测，
 因此在consul集群中该serviceId服务将是不健康的或者根本就查不到！
 解决该问题，就需要多点注册！即springcloud微服务启动时注册到spring.cloud.consul.cluster.nodes 中指定的多个consul节点中，
 多点注册的好处就是某个consul节点挂了，该serviceId的注册信息仍然可见，并且仍然有其他可用consul节点对该serviceId服务进行healthcheck，
 在consul集群中，该serviceId服务仍然是健康的！
 有一点需要说明的是：多点注册并不是越多越好，spring.cloud.consul.cluster.nodes 并不需要考虑扩容问题，
 因为单个consul节点上注册，在其他节点上就能看到，这是consul的特性！
 所以多点注册并不是越多越好，选用三五个节点足已！

### 综上所述集群版本consul客户端ClusterConsulClient应该实现如下基本功能：

* spring.cloud.consul.cluster.nodes 如果配置为单个节点，那么与原来一样.

* ClusterConsulClient内部实际是代理了spring.cloud.consul.cluster.nodes 配置的多个节点的 ConsulClient的 行为(方法)，
只不过在发生单点故障时做动态切换并且进行fallback重试。

* ClusterConsulClient 创建实例时对spring.cloud.consul.cluster.nodes 配置的多个节点进行遍历注册，
并启用定时任务对节点进行健康检测。

* ClusterConsulClient内部应该通过哈希一致性算法，
根据一个合理的哈希key(例如spring.cloud.client.ip-address)选举出一个固定的 ClusterConsulClient 作为当前正在使用的客户端(称之为currentClient)，
在微服务的运行过程中如果 currentClient 不可用了，
那么立即通过一致性哈希算法在剩余可用 ClusterConsulClient 中选举一个出来赋给 currentClient 并进行Retry重试。
选用哈希一致性算法主要是考虑 spring.cloud.consul.cluster.nodes 配置的多个节点的单点压力分布均衡问题。

* ClusterConsulClient 多点注册带来的问题就是 ConsulClient.getHealthServices() 结果集重点重复问题，
也就引起了 ConsulDiscoveryClient.getInstances() 和 ConsulServerList.getXxxServers() 结果集出现完全重复问题，
解决这个问题到底是把解决逻辑放在 ClusterConsulClient 里面呢(在 getHealthServices() 方法里面进行去重)，
还是把去重逻辑放到 ConsulDiscoveryClient.getInstances() 和 ConsulServerList.getXxxServers() 方法中呢？
这个我选择了后者，因为前者的结果集是重点重复，而后者是完全重复，放在后者也是一个较为合理的方式。

* 监控检查间隔 spring.cloud.consul.retry.initial-interval，默认为10秒。

# 功能实现

* 集群fallback策略：组成客户端集群的节点中会通过哈希一致性算法得出一个 ClusterConsulClient 作为当前正在使用的 ClusterConsulClient(currentClient)。
如果 currentClient 不可用，则立即在剩余可用节点中再次通过哈希一致性算法得到一个可用的 ClusterConsulClient 并赋给currentClient，
并通过 RetryTemplate 进行 fallback 重试。
同时定时健康检测全部集群 ClusterConsulClient 的可用性，

* 服务配置模块：服务配置使用的是一系列 ClusterConsulClient KV操作的方法。这些方法仅在当前节点上执行一次，如果当前节点不可用则使用 RetryTemplate 进行fallback重试!

* 服务注册模块：服务注册 ConsulServiceRegistry 中所用到的几个方法是 ConsulClient.agentServiceRegister，ConsulClient.agentServiceDeregister，ConsulClient.agentServiceSetMaintenance。
注册服务必须在每个集群节点上都注册(register)一遍(多点广播注册)，
同样取消注册(deregister)、设置服务状态(setStatus)也是同理。
至于为什么要多点广播注册?
原因有二：
(1)、在单个client节点上注册的服务信息仅在该client节点可用的情况下才会出现在集群中(ui/ConsulClient.getHealthServices())中可见，
如果该client节点挂了，那么集群中(ui/ConsulClient.getHealthServices())看不到它上面注册的服务了，因此必须多节点注册；
(2)、在单个client节点上注册的服务信息及其healthcheck，仅在该节点可用的情况下healthcheck才被执行，
如果该节点挂了，那么该节点上注册的服务的healthcheck将无法执行，因此会出现服务实际是健康的，
但是consul集群认为其是不健康的(因为负责健康检测的那个节点挂了)

* 服务发现模块：服务发现ConsulDiscoveryClient中所用到的几个方法是ConsulClient.getCatalogServices，ConsulClient.getHealthServices。
负载均衡Ribbon中ServerList中所用到的方法是 ConsulClient.getHealthServices。
这些方法仅在当前节点上执行一次，如果当前节点不可用则使用RetryTemplate进行fallback重试!

* 其他SpringCloud中未使用到的方法，使用默认策略，
即仅在当前节点上执行一次，如果当前节点不可用则使用 RetryTemplate 进行fallback重试!

* 由于服务注册为多点广播，因此服务发现底层所使用到的方法 ConsulClient.getHealthServices() 会出现相同服务实例ID的多条重复结果集。
在此将去重逻辑放在了自定义的 ConsulDiscoveryClient 和 ConsulServerList 中了

# 版本
* 分支 dev-2.1.2.RELEASE 基于spring-cloud-consul 2.1.2.RELEASE版本
* 分支 dev-2.2.1.RELEASE 基于spring-cloud-consul 2.2.1.RELEASE版本

# 使用方法

* 1.项目中引入starter：[spring-cloud-starter-consul-cluster](https://github.com/lansheng228/spring-cloud-consul)

**以下依赖已发布到maven中央库中了**

````xml

	<dependency>
		<groupId>com.github.lansheng228</groupId>
		<artifactId>spring-cloud-starter-consul-cluster</artifactId>
		<version>2.2.1.3.RELEASE</version>
	</dependency>

````

* 2.在bootstrap.yml|properties中指定 spring.cloud.consul.cluster.nodes 为多节点，如下所示：

```
# consul agent
spring.cloud.consul.cluster.nodes=192.168.1.101:8500,192.168.1.102:8500,192.168.1.103:8500
```

* 3. 在bootstrap.yml|properties中指定监控检查间隔 spring.cloud.consul.retry.initial-interval ，如下所示：

```
# 服务监测时间间隔
spring.cloud.consul.retry.initial-interval: 10000
```

如不配置，默认为10秒。

* 4.开启相关日志的打印：

````xml
	
	<logger name="org.springframework.cloud.consul" level="DEBUG"/>
	
````
