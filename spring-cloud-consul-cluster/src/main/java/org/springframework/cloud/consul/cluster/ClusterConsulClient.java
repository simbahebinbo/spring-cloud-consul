package org.springframework.cloud.consul.cluster;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.ecwid.consul.transport.TransportException;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.OperationException;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.acl.AclClient;
import com.ecwid.consul.v1.acl.model.Acl;
import com.ecwid.consul.v1.acl.model.NewAcl;
import com.ecwid.consul.v1.acl.model.UpdateAcl;
import com.ecwid.consul.v1.agent.AgentClient;
import com.ecwid.consul.v1.agent.model.Member;
import com.ecwid.consul.v1.agent.model.NewCheck;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.agent.model.Self;
import com.ecwid.consul.v1.agent.model.Service;
import com.ecwid.consul.v1.catalog.CatalogClient;
import com.ecwid.consul.v1.catalog.model.CatalogDeregistration;
import com.ecwid.consul.v1.catalog.model.CatalogNode;
import com.ecwid.consul.v1.catalog.model.CatalogRegistration;
import com.ecwid.consul.v1.catalog.model.CatalogService;
import com.ecwid.consul.v1.coordinate.CoordinateClient;
import com.ecwid.consul.v1.coordinate.model.Datacenter;
import com.ecwid.consul.v1.coordinate.model.Node;
import com.ecwid.consul.v1.event.EventClient;
import com.ecwid.consul.v1.event.model.Event;
import com.ecwid.consul.v1.event.model.EventParams;
import com.ecwid.consul.v1.health.HealthClient;
import com.ecwid.consul.v1.health.model.Check;
import com.ecwid.consul.v1.health.model.Check.CheckStatus;
import com.ecwid.consul.v1.health.model.HealthService;
import com.ecwid.consul.v1.kv.KeyValueClient;
import com.ecwid.consul.v1.kv.model.GetBinaryValue;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.query.QueryClient;
import com.ecwid.consul.v1.query.model.QueryExecution;
import com.ecwid.consul.v1.session.SessionClient;
import com.ecwid.consul.v1.session.model.NewSession;
import com.ecwid.consul.v1.session.model.Session;
import com.ecwid.consul.v1.status.StatusClient;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.springframework.cloud.consul.ConsulProperties;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * ?????????ConsulClient
 *
 * ???????????????????????????????????????Nginx/HAProxy??????????????????????????????????????????(Nginx/HAProxy????????????????????????????????????????????????)
 *
 * ????????????ConsulClient???????????????spring-cloud-consul????????????????????????????????????????????????????????????????????????
 *
 *
 * 1??????????????????????????????ConsulClient KV??????????????????????????????????????????
 *
 * ????????????????????????????????????RetryTemplate??????fallback??????!
 *
 * 2????????????????????????
 *
 * 2.1???ConsulServiceRegistry ???????????????????????????(agentServiceRegister, agentServiceDeregister, agentServiceSetMaintenance)???
 *
 * ?????????????????????????????????
 *
 * ??????????????????client??????????????????????
 *
 * ???????????????
 *
 * (1)????????????client???????????????????????????????????????client????????????????????????????????????????????????(ui/ConsulClient.getHealthServices())????????????
 *
 * ?????????client??????????????????????????????(ui/ConsulClient.getHealthServices())?????????????????????????????????????????????????????????????????????
 *
 * (2)????????????client????????????????????????????????????healthcheck????????????????????????????????????healthcheck???????????????
 *
 * ????????????????????????????????????????????????????????????healthcheck??????????????????????????????????????????????????????????????????consul??????????????????????????????(?????????????????????????????????????????????)
 *
 * 2.2 TtlScheduler ?????????????????????(agentCheckPass)???????????????????????????????????????????????????!
 *
 * 3????????????????????????
 *
 * 3.1?????????????????????????????????????????????(getCatalogServices???getHealthServices)???
 *
 * ????????????????????????????????????????????????????????????????????????RetryTemplate??????fallback??????!
 *
 * 3.2??????2.1?????????????????????????????????????????????????????????????????????(getHealthServices)???
 *
 * ????????????????????????????????????????????????(ConsulServiceRegistry.getInstances()???ConsulServerList.getXxxServers())????????????????????????!
 *
 * 4?????????SpringCloud???????????????????????????????????????????????????????????????????????????????????????
 *
 * ????????????????????????????????????RetryTemplate??????fallback??????!
 */

@Slf4j
public class ClusterConsulClient extends ConsulClient implements AclClient, AgentClient,
    CatalogClient, CoordinateClient, EventClient, HealthClient, KeyValueClient,
    QueryClient, SessionClient, StatusClient, RetryListener {

  private static final String CURRENT_CLIENT_KEY = "currentClient";

  @Getter
  private final ScheduledExecutorService consulClientsExecutor = Executors
      .newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 4);

  /**
   * ConsulClient??????
   */
  @Getter
  private final ClusterConsulProperties clusterConsulProperties;

  /**
   * ??????ConsulClient
   */
  @Getter
  private List<ConsulClientHolder> consulClients;

  /**
   * ??????RetryTemplate
   */
  @Getter
  private final RetryTemplate retryTemplate;

  /**
   * ?????????????????????ConsulClient
   */
  @Getter
  @Setter
  private volatile ConsulClientHolder currentClient;


  private Map<String, Boolean> consulClientHealthMap;

  private Set<String> clientIdSet;

  private NewService currentNewService;
  private String currentToken;

  /**
   * ????????????????????????????????????
   */
  private final Lock chooseLock = new ReentrantLock();

  public ClusterConsulClient(ClusterConsulProperties clusterConsulProperties) {
    super();
    this.clusterConsulProperties = clusterConsulProperties;
    this.consulClientHealthMap = Maps.newConcurrentMap();
    this.clientIdSet = Sets.newHashSet();
    // ????????????????????????
    this.consulClients = createConsulClients();
    // ??????????????????
    this.retryTemplate = createRetryTemplate();
    // ??????????????????
    this.currentClient = initCurrentConsulClient();
    this.scheduleConsulClientsHealthCheck();
    this.scheduleConsulClientsCreate();
  }

  //????????????
  private void agentServiceReregister() {
    Response<Void> result = null;
    for (ConsulClientHolder consulClient : this.consulClients) {
      if (consulClient.isHealthy()) {
        if (ObjectUtils.isNotEmpty(this.currentNewService)) {
          if (ObjectUtils.isNotEmpty(this.currentToken)) {
            result = consulClient.getClient().agentServiceRegister(this.currentNewService, this.currentToken);
          } else {
            result = consulClient.getClient().agentServiceRegister(this.currentNewService);
          }
        }
      }
    }

    if (ObjectUtils.isNotEmpty(this.currentNewService)) {
      if (ObjectUtils.isNotEmpty(this.currentToken)) {
        log.debug(
            CommonConstant.LOG_PREFIX + ">>> function agentServiceReregister => currentNewService: {}  ===  currentToken: {} ===  result: {} <<<",
            this.currentNewService, this.currentToken, result);
      } else {
        log.debug(
            CommonConstant.LOG_PREFIX + ">>> function agentServiceReregister => currentNewService: {}  ===  result: {} <<<",
            this.currentNewService, result);
      }
    }
  }

  /**
   * ????????????ConsulClient
   *
   * @return ??????????????????
   */
  protected List<ConsulClientHolder> createConsulClients() {
    List<String> connectList = prepareConnectList();
    List<ConsulClientHolder> tmpConsulClients = connectList.stream().map(connect -> {
      String[] connects = connect.split(CommonConstant.SEPARATOR_COLON);
      ConsulProperties properties = new ConsulProperties();
      properties.setEnabled(clusterConsulProperties.isEnabled());
      properties.setScheme(clusterConsulProperties.getScheme());
      properties.setTls(clusterConsulProperties.getTls());
      properties.setHost(connects[0]);
      properties.setPort(Integer.parseInt(connects[1]));

      ConsulClientHolder consulClientHolder = new ConsulClientHolder(properties);
      clientIdSet.add(consulClientHolder.getClientId());

      return consulClientHolder;
    }).filter(ConsulClientHolder::isHealthy).sorted().collect(Collectors.toList()); // ??????

    //consul agent??????????????????consul agent???????????????consul???????????????????????????
    if (tmpConsulClients.size() < this.clusterConsulProperties.getClusterNodes().size()) {
      log.warn(CommonConstant.LOG_PREFIX + ">>> Some consul clients are not available. Please check.");
    }

    //consul agent???????????????3???????????????????????????????????????
    if (tmpConsulClients.size() <= 3) {
      log.warn(CommonConstant.LOG_PREFIX + ">>> The num of consul clients is too few. Please check and add more consul client.");
    }

    //consul agent?????????2?????? consul????????????????????????
    if (tmpConsulClients.size() < 2) {
      log.error(CommonConstant.LOG_PREFIX + ">>> The consul cluster is not available. Please check and repair.");
    }

    List<String> clientIdList = tmpConsulClients.stream().map(ConsulClientHolder::getClientId)
        .collect(Collectors.toList());
    log.info(CommonConstant.LOG_PREFIX + ">>> Creating cluster consul clients: {} <<<", clientIdList);

    return tmpConsulClients;
  }

  /**
   * ??????ConsulClient???????????????
   */
  protected List<String> prepareConnectList() {
    List<String> connectList = this.clusterConsulProperties.getClusterNodes();
    log.info(CommonConstant.LOG_PREFIX + ">>> Connect list: " + connectList + " <<<");

    return connectList;
  }

  /**
   * ???????????? RetryTemplate??? ????????????SimpleRetryPolicy(maxAttempts??????consulClients.size() + 1)
   */
  protected RetryTemplate createRetryTemplate() {
    Map<Class<? extends Throwable>, Boolean> retryableExceptions = null;

    if (CollectionUtils.isNotEmpty(clusterConsulProperties.getRetryableExceptions())) {
      retryableExceptions = clusterConsulProperties.getRetryableExceptions().stream()
          .collect(Collectors.toMap(Function.identity(), e -> Boolean.TRUE,
              (oldValue, newValue) -> newValue));
    }

    if (MapUtils.isEmpty(retryableExceptions)) {
      retryableExceptions = createDefaultRetryableExceptions();
    }

    RetryTemplate tmpRetryTemplate = new RetryTemplate();
    SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(this.clusterConsulProperties.getClusterNodes().size(),
        retryableExceptions, true);
    tmpRetryTemplate.setRetryPolicy(retryPolicy);
    tmpRetryTemplate.setListeners(new RetryListener[]{this});

    return tmpRetryTemplate;
  }

  /**
   * ???????????????retryableExceptions
   */
  protected Map<Class<? extends Throwable>, Boolean> createDefaultRetryableExceptions() {
    Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
    retryableExceptions.put(TransportException.class, true);
    retryableExceptions.put(OperationException.class, true);
    retryableExceptions.put(IOException.class, true);
    retryableExceptions.put(ConnectException.class, true);
    retryableExceptions.put(TimeoutException.class, true);
    retryableExceptions.put(SocketTimeoutException.class, true);

    return retryableExceptions;
  }

  /**
   * ?????????ConsulClient
   */
  private ConsulClientHolder initCurrentConsulClient() {
    ConsulClientHolder chooseClient = chooseClient(this.clusterConsulProperties.getClusterClientKey(), this.consulClients);
    log.info(CommonConstant.LOG_PREFIX + ">>>  init current consul client: {}  <<<", chooseClient);

    return chooseClient;
  }

  private ConsulClientHolder chooseClient(String key, List<ConsulClientHolder> clients) {
    ConsulClientHolder chooseClient = ConsulClientUtil.chooseClient(key, clients);
    log.info(CommonConstant.LOG_PREFIX + ">>>  Hash Key: {}  ==== Hash List: {}  ====  Hash Result: {} <<<", key, clients, chooseClient);

    return chooseClient;
  }

  /**
   * ????????????????????????????????????????????????ConsulClient
   */
  protected void chooseConsulClient() {
    try {
      this.chooseLock.lock();
      if (!this.currentClient.isHealthy()) {
        // ?????????????????????
        List<ConsulClientHolder> availableClients = this.consulClients.stream()
            .filter(ConsulClientHolder::isHealthy).sorted()
            .collect(Collectors.toList());
        log.info(CommonConstant.LOG_PREFIX + ">>> Available ConsulClients: " + availableClients + " <<<");

        if (ObjectUtils.isNotEmpty(availableClients)) {
          // ???????????????????????????????????????????????????????????????
          ConsulClientHolder choosedClient = chooseClient(
              this.clusterConsulProperties.getClusterClientKey(), availableClients);
          if (ObjectUtils.isNotEmpty(choosedClient)) {
            log.info(CommonConstant.LOG_PREFIX + ">>> Successfully choosed a new ConsulClient : {} <<<",
                choosedClient);
            this.currentClient = choosedClient;
          } else {
            log.warn(CommonConstant.LOG_PREFIX + ">>> Choosed New ConsulClient Fail!!!");
          }
        } else {
          log.error(CommonConstant.LOG_PREFIX + ">>> No consul client is available!!!");
        }
      }
    } finally {
      this.chooseLock.unlock();
    }
  }

  /**
   * ???????????????ConsulClient
   *
   * @param context - ???????????????
   */
  protected ConsulClient getRetryConsulClient(RetryContext context) {
    context.setAttribute(CURRENT_CLIENT_KEY, this.currentClient);
    int retryCount = context.getRetryCount();
    if ((!this.currentClient.isHealthy())
        && (CollectionUtils.isNotEmpty(this.consulClients))) {
      log.info(CommonConstant.LOG_PREFIX + ">>> Current ConsulClient[{}] Is Unhealthy. Choose Again! <<<",
          this.currentClient);
      chooseConsulClient();
    }
    if (retryCount > 0) {
      log.info(CommonConstant.LOG_PREFIX + ">>> Using current ConsulClient[{}] for retry {} <<<",
          this.currentClient, retryCount);
    }

    return this.currentClient.getClient();
  }

  @Override
  public final <T, E extends Throwable> boolean open(RetryContext context,
      RetryCallback<T, E> callback) {
    return true;
  }

  @Override
  public final <T, E extends Throwable> void close(RetryContext context,
      RetryCallback<T, E> callback, Throwable throwable) {
    context.removeAttribute(CURRENT_CLIENT_KEY);
  }

  /**
   * ??????ConsulClient?????????????????????????????????????????????????????????
   */
  @Override
  public <T, E extends Throwable> void onError(RetryContext context,
      RetryCallback<T, E> callback, Throwable throwable) {
    ConsulClientHolder tmpCurrentClient = (ConsulClientHolder) context
        .getAttribute(CURRENT_CLIENT_KEY);
    if (ObjectUtils.isNotEmpty(tmpCurrentClient)) {
      tmpCurrentClient.setHealthy(false);
    }
  }

  /**
   * ConsulClient?????????????????????
   */
  protected void scheduleConsulClientsHealthCheck() {
    consulClientsExecutor.scheduleAtFixedRate(
        this::checkConsulClientsHealth, clusterConsulProperties.getHealthCheckInterval(),
        clusterConsulProperties.getHealthCheckInterval(), TimeUnit.MILLISECONDS);
  }

  protected void scheduleConsulClientsCreate() {
    consulClientsExecutor.scheduleAtFixedRate(
        this::createAllConsulClients, clusterConsulProperties.getHealthCheckInterval(),
        clusterConsulProperties.getHealthCheckInterval(), TimeUnit.MILLISECONDS);
  }

  /**
   * ????????????ConsulClient????????????????????????
   */
  protected void checkConsulClientsHealth() {
    this.consulClientHealthMap = checkAllConsulClientsHealth();

    boolean allHealthy = isAllConsulClientsHealthy();
    if (allHealthy) {
      log.info(CommonConstant.LOG_PREFIX + ">>> All consul clients are healthy. <<<");
    }
  }

  protected void createAllConsulClients() {
    long currentHealthClientNum = this.consulClientHealthMap.values().stream().filter(isHealthy -> isHealthy).count();
    int clientNum = clientIdSet.size();
    log.info(CommonConstant.LOG_PREFIX + ">>> current health client num: {}           all client num: {}     Is Same ? {} <<<",
        currentHealthClientNum, clientNum, clientNum == currentHealthClientNum);
    //??????consul????????????????????????????????????client
    if (clientNum <= currentHealthClientNum) {
      return;
    }

    log.warn(CommonConstant.LOG_PREFIX + ">>> some consul clients are unhealthy. Please check!  <<<");

    //??????????????????consul?????????????????????client
    List<ConsulClientHolder> tmpConsulClients = createConsulClients();

    boolean flag = ListUtil.isSame(this.consulClients, tmpConsulClients);

    log.info(CommonConstant.LOG_PREFIX + ">>> createAllConsulClients. {}           The Size: {}.     Is Same ? {} <<<",
        tmpConsulClients, tmpConsulClients.size(), flag);

    //consul???????????????
    if (!flag) {
      this.consulClients = tmpConsulClients;
      //????????????
      agentServiceReregister();
    }
  }

  private Map<String, Boolean> checkAllConsulClientsHealth() {
    Map<String, Boolean> tmpConsulClientHealthMap = new HashMap<>();
    for (ConsulClientHolder consulClient : this.consulClients) {
      consulClient.checkHealth();
      tmpConsulClientHealthMap.put(consulClient.getClientId(), consulClient.isHealthy());
    }
    log.info(CommonConstant.LOG_PREFIX + ">>> check all consul clients healthy: {} <<<", tmpConsulClientHealthMap);

    return tmpConsulClientHealthMap;
  }

  /**
   * ???????????????ConsulClient??????????????????????
   */
  protected boolean isAllConsulClientsHealthy() {
    AtomicBoolean allHealthy = new AtomicBoolean(true);
    this.consulClientHealthMap.values().forEach(isHealthy -> allHealthy.set(allHealthy.get() && isHealthy));
    log.info(CommonConstant.LOG_PREFIX + ">>>  All Consul Clients are health? {} <<<", allHealthy.get());

    return allHealthy.get();
  }

  @Override
  public Response<String> getStatusLeader() {
    return this.retryTemplate.execute(context -> {
      Response<String> leader = getRetryConsulClient(context).getStatusLeader();
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getStatusLeader => leader: {} <<<", leader);

      return leader;
    });
  }

  @Override
  public Response<List<String>> getStatusPeers() {
    return this.retryTemplate.execute(context -> {
      Response<List<String>> peers = getRetryConsulClient(context).getStatusPeers();
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getStatusPeers => peers: {} <<<", peers);

      return peers;
    });
  }

  @Override
  public Response<String> sessionCreate(NewSession newSession, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<String> sessionCreate = getRetryConsulClient(context).sessionCreate(newSession, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function sessionCreate => newSession: {} === queryParams: {} === sessionCreate: {} <<<", newSession,
          queryParams,
          sessionCreate);

      return sessionCreate;
    });
  }

  @Override
  public Response<String> sessionCreate(NewSession newSession, QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<String> sessionCreate = getRetryConsulClient(context).sessionCreate(newSession,
          queryParams, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function sessionCreate => newSession: {} === queryParams: {} === token: {} === sessionCreate: {} <<<",
          newSession,
          queryParams, token, sessionCreate);

      return sessionCreate;
    });
  }

  @Override
  public Response<Void> sessionDestroy(String session, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      log.debug(CommonConstant.LOG_PREFIX + ">>> function sessionDestroy => session: {} === queryParams: {}  <<<", session, queryParams);

      return getRetryConsulClient(context).sessionDestroy(session, queryParams);
    });
  }

  @Override
  public Response<Void> sessionDestroy(String session, QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      log.debug(CommonConstant.LOG_PREFIX + ">>> function sessionDestroy => session: {} === queryParams: {}  === token: {} <<<", session,
          queryParams, token);

      return getRetryConsulClient(context).sessionDestroy(session, queryParams, token);
    });
  }

  @Override
  public Response<Session> getSessionInfo(String session, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Session> sessionInfo = getRetryConsulClient(context).getSessionInfo(session, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getSessionInfo => session: {} === queryParams: {}  === sessionInfo: {} <<<", session,
          queryParams,
          sessionInfo);

      return sessionInfo;
    });
  }

  @Override
  public Response<Session> getSessionInfo(String session, QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Session> sessionInfo = getRetryConsulClient(context).getSessionInfo(session, queryParams, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getSessionInfo => session: {} === queryParams: {}  === sessionInfo: {} <<<", session,
          queryParams,
          sessionInfo);

      return sessionInfo;
    });
  }

  @Override
  public Response<List<Session>> getSessionNode(String node, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Session>> sessionNode = getRetryConsulClient(context).getSessionNode(node, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getSessionNode => node: {} === queryParams: {}  === sessionNode: {} <<<", node,
          queryParams, sessionNode);

      return sessionNode;
    });
  }

  @Override
  public Response<List<Session>> getSessionNode(String node, QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<Session>> sessionNode = getRetryConsulClient(context).getSessionNode(node,
          queryParams, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getSessionNode => node: {} === queryParams: {}  === token: {}  === sessionNode: {} <<<",
          node, queryParams,
          token, sessionNode);

      return sessionNode;
    });
  }

  @Override
  public Response<List<Session>> getSessionList(QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Session>> sessionList = getRetryConsulClient(context).getSessionList(queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getSessionList => queryParams: {}   === sessionList: {} <<<", queryParams, sessionList);

      return sessionList;
    });
  }

  @Override
  public Response<List<Session>> getSessionList(QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<Session>> sessionList = getRetryConsulClient(context).getSessionList(queryParams, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getSessionList => queryParams: {}   === token: {} === sessionList: {} <<<", queryParams,
          token,
          sessionList);

      return sessionList;
    });
  }

  @Override
  public Response<Session> renewSession(String session, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Session> renewSession = getRetryConsulClient(context).renewSession(session,
          queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function renewSession => session: {}   ===  queryParams: {}   === renewSession: {} <<<", session,
          queryParams,
          renewSession);

      return renewSession;
    });
  }

  @Override
  public Response<Session> renewSession(String session, QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Session> renewSession = getRetryConsulClient(context).renewSession(session,
          queryParams, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function renewSession => session: {}   ===  queryParams: {}   === token: {} === renewSession: {} <<<",
          session,
          queryParams, token, renewSession);

      return renewSession;
    });
  }

  @Override
  public Response<QueryExecution> executePreparedQuery(String uuid, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<QueryExecution> queryExecution = getRetryConsulClient(context).executePreparedQuery(uuid,
          queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function executePreparedQuery => uuid: {}   ===  queryParams: {}   === queryExecution: {}  <<<",
          uuid,
          queryParams, queryExecution);

      return queryExecution;
    });
  }

  @Override
  public Response<GetValue> getKVValue(String key) {
    return this.retryTemplate.execute(context -> {
      Response<GetValue> value = getRetryConsulClient(context).getKVValue(key);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVValue => key: {}   ===  value: {} <<<", key, value);

      return value;
    });
  }

  @Override
  public Response<GetValue> getKVValue(String key, String token) {
    return this.retryTemplate.execute(context -> {
      Response<GetValue> value = getRetryConsulClient(context).getKVValue(key, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVValue => key: {}   ===  token: {}  ===  value: {} <<<", key, token, value);

      return value;
    });
  }

  @Override
  public Response<GetValue> getKVValue(String key, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<GetValue> value = getRetryConsulClient(context).getKVValue(key, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVValue => key: {}   ===  queryParams: {}  ===  value: {} <<<", key, queryParams,
          value);

      return value;
    });
  }

  @Override
  public Response<GetValue> getKVValue(String key, String token, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<GetValue> value = getRetryConsulClient(context).getKVValue(key, token,
          queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVValue => key: {}   ===  token: {}  ===  queryParams: {}  ===  value: {} <<<", key,
          token, queryParams,
          value);

      return value;
    });
  }

  @Override
  public Response<GetBinaryValue> getKVBinaryValue(String key) {
    return this.retryTemplate.execute(context -> {
      Response<GetBinaryValue> binaryValue = getRetryConsulClient(context).getKVBinaryValue(key);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVBinaryValue => key: {}  ===  binaryValue: {} <<<", key, binaryValue);

      return binaryValue;
    });
  }

  @Override
  public Response<GetBinaryValue> getKVBinaryValue(String key, String token) {
    return this.retryTemplate.execute(context -> {
      Response<GetBinaryValue> binaryValue = getRetryConsulClient(context).getKVBinaryValue(key, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVBinaryValue => key: {}  ===  token: {}  ===  binaryValue: {} <<<", key, token,
          binaryValue);

      return binaryValue;
    });
  }

  @Override
  public Response<GetBinaryValue> getKVBinaryValue(String key, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<GetBinaryValue> binaryValue = getRetryConsulClient(context).getKVBinaryValue(key,
          queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVBinaryValue => key: {}  ===  queryParams: {}  ===  binaryValue: {} <<<", key,
          queryParams,
          binaryValue);

      return binaryValue;
    });
  }

  @Override
  public Response<GetBinaryValue> getKVBinaryValue(String key, String token, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<GetBinaryValue> binaryValue = getRetryConsulClient(context).getKVBinaryValue(key, token,
          queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getKVBinaryValue => key: {}  ===  token: {}  ===  queryParams: {}  ===  binaryValue: {} <<<",
          key, token,
          queryParams, binaryValue);

      return binaryValue;
    });
  }

  @Override
  public Response<List<GetValue>> getKVValues(String keyPrefix) {
    return this.retryTemplate.execute(context -> {
      Response<List<GetValue>> valueList = getRetryConsulClient(context).getKVValues(keyPrefix);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVValues => keyPrefix: {}  ===  valueList: {} <<<", keyPrefix, valueList);

      return valueList;
    });
  }

  @Override
  public Response<List<GetValue>> getKVValues(String keyPrefix, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<GetValue>> valueList = getRetryConsulClient(context).getKVValues(keyPrefix,
          token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVValues => keyPrefix: {}  ===  token: {}  ===  valueList: {} <<<", keyPrefix, token,
          valueList);

      return valueList;
    });
  }

  @Override
  public Response<List<GetValue>> getKVValues(String keyPrefix, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<GetValue>> valueList = getRetryConsulClient(context).getKVValues(keyPrefix,
          queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVValues => keyPrefix: {}  ===  queryParams: {}  ===  valueList: {} <<<", keyPrefix,
          queryParams,
          valueList);

      return valueList;
    });
  }

  @Override
  public Response<List<GetValue>> getKVValues(String keyPrefix, String token, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<GetValue>> valueList = getRetryConsulClient(context).getKVValues(keyPrefix, token,
          queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getKVValues => keyPrefix: {}  ===  token: {}  ===  queryParams: {}  ===  valueList: {} <<<",
          keyPrefix,
          token, queryParams, valueList);

      return valueList;
    });
  }

  @Override
  public Response<List<GetBinaryValue>> getKVBinaryValues(String keyPrefix) {
    return this.retryTemplate.execute(context -> {
      Response<List<GetBinaryValue>> binaryValueList = getRetryConsulClient(context).getKVBinaryValues(keyPrefix);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVBinaryValues => keyPrefix: {}  ===  binaryValueList: {} <<<", keyPrefix,
          binaryValueList);

      return binaryValueList;
    });
  }

  @Override
  public Response<List<GetBinaryValue>> getKVBinaryValues(String keyPrefix, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<GetBinaryValue>> binaryValueList = getRetryConsulClient(context).getKVBinaryValues(keyPrefix, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVBinaryValues => keyPrefix: {}  ===  token: {}  ===  binaryValueList: {} <<<",
          keyPrefix, token,
          binaryValueList);

      return binaryValueList;
    });
  }

  @Override
  public Response<List<GetBinaryValue>> getKVBinaryValues(String keyPrefix, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<GetBinaryValue>> binaryValueList = getRetryConsulClient(context).getKVBinaryValues(keyPrefix,
          queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVBinaryValues => keyPrefix: {}  ===  queryParams: {}  ===  binaryValueList: {} <<<",
          keyPrefix,
          queryParams,
          binaryValueList);

      return binaryValueList;
    });
  }

  @Override
  public Response<List<GetBinaryValue>> getKVBinaryValues(String keyPrefix, String token, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<GetBinaryValue>> binaryValueList = getRetryConsulClient(context).getKVBinaryValues(keyPrefix, token, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getKVBinaryValues => keyPrefix: {}  ===  token: {}  ===  queryParams: {}  ===  binaryValueList: {} <<<",
          keyPrefix, token, queryParams, binaryValueList);

      return binaryValueList;
    });
  }

  @Override
  public Response<List<String>> getKVKeysOnly(String keyPrefix) {
    return this.retryTemplate.execute(context -> {
      Response<List<String>> keyList = getRetryConsulClient(context).getKVKeysOnly(keyPrefix);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVKeysOnly => keyPrefix: {}  ===  keyList: {} <<<",
          keyPrefix, keyList);

      return keyList;
    });
  }

  @Override
  public Response<List<String>> getKVKeysOnly(String keyPrefix, String separator, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<String>> keyList = getRetryConsulClient(context).getKVKeysOnly(keyPrefix,
          separator, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVKeysOnly => keyPrefix: {}  ===  separator: {}  ===  token: {}  ===  keyList: {} <<<",
          keyPrefix, separator, token, keyList);

      return keyList;
    });
  }

  @Override
  public Response<List<String>> getKVKeysOnly(String keyPrefix, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<String>> keyList = getRetryConsulClient(context).getKVKeysOnly(keyPrefix, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getKVKeysOnly => keyPrefix: {}  ===  queryParams: {} ===  keyList: {} <<<",
          keyPrefix, queryParams, keyList);

      return keyList;
    });
  }

  @Override
  public Response<List<String>> getKVKeysOnly(String keyPrefix, String separator, String token, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<String>> keyList = getRetryConsulClient(context).getKVKeysOnly(keyPrefix,
          separator, token, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getKVKeysOnly => keyPrefix: {}  ===  separator: {}  ===  token: {}  ===  queryParams: {} ===  keyList: {} <<<",
          keyPrefix, separator, token, queryParams, keyList);

      return keyList;
    });
  }

  @Override
  public Response<Boolean> setKVValue(String key, String value) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVValue(key, value);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function setKVValue => key: {}  ===  value: {}  ===  result: {} <<<",
          key, value, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVValue(String key, String value, PutParams putParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVValue(key, value, putParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function setKVValue => key: {}  ===  value: {}  ===  putParams: {} ===  result: {} <<<",
          key, value, putParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVValue(String key, String value, String token,
      PutParams putParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVValue(key, value, token,
          putParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function setKVValue => key: {}  ===  value: {}  ===  token: {}  ===  putParams: {} ===  result: {} <<<",
          key, value, token, putParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVValue(String key, String value, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVValue(key, value, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function setKVValue => key: {}  ===  value: {}  ===  queryParams: {} ===  result: {} <<<",
          key, value, queryParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVValue(String key, String value, PutParams putParams, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVValue(key, value, putParams, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function setKVValue => key: {}  ===  value: {}  ===  putParams: {}  ===  queryParams: {} ===  result: {} <<<",
          key, value, putParams, queryParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVValue(String key, String value, String token,
      PutParams putParams, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVValue(key, value, token,
          putParams, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function setKVValue => key: {}  ===  value: {}  ===  token: {}   ===  putParams: {}  ===  queryParams: {} ===  result: {} <<<",
          key, value, token, putParams, queryParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVBinaryValue(String key, byte[] value) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVBinaryValue(key, value);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function setKVBinaryValue => key: {}  ===  value: {}  ===  result: {} <<<",
          key, value, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVBinaryValue(String key, byte[] value, PutParams putParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVBinaryValue(key, value, putParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function setKVBinaryValue => key: {}  ===  value: {}  ===  putParams: {}  ===  result: {} <<<",
          key, value, putParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVBinaryValue(String key, byte[] value, String token, PutParams putParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVBinaryValue(key, value,
          token, putParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function setKVBinaryValue => key: {}  ===  value: {}  ===  token: {}  ===  putParams: {}  ===  result: {} <<<",
          key, value, token, putParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVBinaryValue(String key, byte[] value, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVBinaryValue(key, value, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function setKVBinaryValue => key: {}  ===  value: {}  ===  queryParams: {}  ===  result: {} <<<",
          key, value, queryParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVBinaryValue(String key, byte[] value, PutParams putParams, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVBinaryValue(key, value, putParams, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function setKVBinaryValue => key: {}  ===  value: {}  ===  putParams: {}   ===  queryParams: {}  ===  result: {} <<<",
          key, value, putParams, queryParams, result);

      return result;
    });
  }

  @Override
  public Response<Boolean> setKVBinaryValue(String key, byte[] value, String token,
      PutParams putParams, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Boolean> result = getRetryConsulClient(context).setKVBinaryValue(key, value,
          token, putParams, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function setKVBinaryValue => key: {}  ===  value: {}  ===  putParams: {}   ===  queryParams: {}  ===  result: {} <<<",
          key, value, putParams, queryParams, result);

      return result;
    });
  }

  @Override
  public Response<Void> deleteKVValue(String key) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).deleteKVValue(key);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function deleteKVValue => key: {} ===  result: {} <<<", key, result);

      return result;
    });
  }

  @Override
  public Response<Void> deleteKVValue(String key, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).deleteKVValue(key, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function deleteKVValue => key: {}  ===  token: {}  ===  result: {} <<<", key, token, result);

      return result;
    });
  }

  @Override
  public Response<Void> deleteKVValue(String key, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).deleteKVValue(key, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function deleteKVValue => key: {}  ===  queryParams: {}  ===  result: {} <<<", key, queryParams,
          result);

      return result;
    });
  }

  @Override
  public Response<Void> deleteKVValue(String key, String token, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).deleteKVValue(key, token, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function deleteKVValue => key: {}  ===  token: {}  ===  queryParams: {}  ===  result: {} <<<", key,
          token,
          queryParams, result);

      return result;
    });
  }

  @Override
  public Response<Void> deleteKVValues(String key) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).deleteKVValues(key);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function deleteKVValues => key: {}  ===  result: {} <<<", key, result);

      return result;
    });
  }

  @Override
  public Response<Void> deleteKVValues(String key, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).deleteKVValues(key, token);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function deleteKVValues => key: {}  ===  token: {}  ===  result: {} <<<", key, token, result);

      return result;
    });
  }

  @Override
  public Response<Void> deleteKVValues(String key, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).deleteKVValues(key, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function deleteKVValues => key: {}  ===  queryParams: {}  ===  result: {} <<<", key, queryParams,
          result);

      return result;
    });
  }

  @Override
  public Response<Void> deleteKVValues(String key, String token, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).deleteKVValues(key, token, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function deleteKVValues => key: {}  ===  token: {}  ===  queryParams: {}  ===  result: {} <<<", key,
          token,
          queryParams, result);

      return result;
    });
  }

  @Override
  public Response<List<Check>> getHealthChecksForNode(String nodeName, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Check>> checkList = getRetryConsulClient(context).getHealthChecksForNode(nodeName, queryParams);
      log.debug(CommonConstant.LOG_PREFIX + ">>> function getHealthChecksForNode => nodeName: {}  ===  queryParams: {}  ===  checkList: {} <<<",
          nodeName,
          queryParams, checkList);

      return checkList;
    });
  }

  @Override
  public Response<List<Check>> getHealthChecksForService(String serviceName, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Check>> checkList = getRetryConsulClient(context).getHealthChecksForService(serviceName, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getHealthChecksForService => serviceName: {}  ===  queryParams: {}  ===  checkList: {} <<<",
          serviceName,
          queryParams, checkList);

      return checkList;
    });
  }

  @Override
  public Response<List<HealthService>> getHealthServices(String serviceName,
      boolean onlyPassing, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<HealthService>> healthServiceList = getRetryConsulClient(context)
          .getHealthServices(serviceName, onlyPassing, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getHealthServices => serviceName: {}  ===  onlyPassing: {}  ===  queryParams: {}  ===  healthServiceList: {} <<<",
          serviceName, onlyPassing, queryParams, healthServiceList);

      return healthServiceList;
    });
  }

  @Override
  public Response<List<HealthService>> getHealthServices(String serviceName, String tag,
      boolean onlyPassing, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<HealthService>> healthServiceList = getRetryConsulClient(context).getHealthServices(
          serviceName, tag, onlyPassing, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getHealthServices => serviceName: {}  ===  tag: {}  ===  onlyPassing: {}  ===  queryParams: {}  ===  healthServiceList: {} <<<",
          serviceName, tag, onlyPassing, queryParams, healthServiceList);

      return healthServiceList;
    });
  }

  @Override
  public Response<List<HealthService>> getHealthServices(String serviceName,
      boolean onlyPassing, QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<HealthService>> healthServiceList = getRetryConsulClient(context).getHealthServices(
          serviceName, onlyPassing, queryParams, token);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getHealthServices => serviceName: {}  ===  onlyPassing: {}  ===  queryParams: {}  ===  token: {}  ===  healthServiceList: {} <<<",
          serviceName, onlyPassing, queryParams, token, healthServiceList);

      return healthServiceList;
    });
  }

  @Override
  public Response<List<HealthService>> getHealthServices(String serviceName, String tag,
      boolean onlyPassing, QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<HealthService>> healthServiceList = getRetryConsulClient(context).getHealthServices(
          serviceName, tag, onlyPassing, queryParams, token);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getHealthServices => serviceName: {}  ===  tag: {}  ===  onlyPassing: {}  ===  queryParams: {}  ===  token: {}  ===  healthServiceList: {} <<<",
          serviceName, tag, onlyPassing, queryParams, token, healthServiceList);

      return healthServiceList;
    });
  }

  @Override
  public Response<List<Check>> getHealthChecksState(QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Check>> checkList = getRetryConsulClient(context).getHealthChecksState(queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getHealthChecksState =>  queryParams: {}  ===  checkList: {} <<<",
          queryParams, checkList);

      return checkList;
    });
  }

  @Override
  public Response<List<Check>> getHealthChecksState(CheckStatus checkStatus, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Check>> checkList = getRetryConsulClient(context).getHealthChecksState(checkStatus, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getHealthChecksState =>  checkStatus: {}  ===  queryParams: {}  ===  checkList: {} <<<",
          checkStatus, queryParams, checkList);

      return checkList;
    });
  }

  @Override
  public Response<Event> eventFire(String event, String payload, EventParams eventParams, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Event> eventFire = getRetryConsulClient(context).eventFire(event, payload, eventParams, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function eventFire =>  event: {}  ===  payload: {}  ===  eventParams: {} ===  queryParams: {}  ===  eventFire: {} <<<",
          event, payload, eventParams, queryParams, eventFire);

      return eventFire;
    });
  }

  @Override
  public Response<List<Event>> eventList(QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Event>> eventList = getRetryConsulClient(context).eventList(queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function eventList =>  queryParams: {}  ===  eventList: {} <<<",
          queryParams, eventList);

      return eventList;
    });
  }

  @Override
  public Response<List<Event>> eventList(String event, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Event>> eventList = getRetryConsulClient(context).eventList(event, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function eventList =>  event: {}  ===  queryParams: {}  ===  eventList: {} <<<",
          event, queryParams, eventList);

      return eventList;
    });
  }

  @Override
  public Response<List<Datacenter>> getDatacenters() {
    return this.retryTemplate.execute(context -> {
      Response<List<Datacenter>> datacenterList = getRetryConsulClient(context).getDatacenters();
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getDatacenters =>  datacenterList: {} <<<",
          datacenterList);

      return datacenterList;
    });
  }

  @Override
  public Response<List<Node>> getNodes(QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<Node>> nodeList = getRetryConsulClient(context).getNodes(queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getNodes =>  queryParams: {}  === nodeList: {} <<<",
          queryParams, nodeList);

      return nodeList;
    });
  }

  @Override
  public Response<Void> catalogRegister(CatalogRegistration catalogRegistration) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).catalogRegister(catalogRegistration);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function catalogRegister =>  catalogRegistration: {}  === result: {} <<<",
          catalogRegistration, result);

      return result;
    });
  }

  @Override
  public Response<Void> catalogRegister(CatalogRegistration catalogRegistration, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).catalogRegister(catalogRegistration, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function catalogRegister =>  catalogRegistration: {}  === token: {}  === result: {} <<<",
          catalogRegistration, token, result);

      return result;
    });
  }

  @Override
  public Response<Void> catalogDeregister(CatalogDeregistration catalogDeregistration) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).catalogDeregister(catalogDeregistration);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function catalogDeregister =>  catalogDeregistration: {}  === result: {} <<<",
          catalogDeregistration, result);

      return result;
    });
  }

  @Override
  public Response<List<String>> getCatalogDatacenters() {
    return this.retryTemplate.execute(context -> {
      Response<List<String>> catalogDatacenterList = getRetryConsulClient(context).getCatalogDatacenters();
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getCatalogDatacenters =>  catalogDatacenterList: {} <<<",
          catalogDatacenterList);

      return catalogDatacenterList;
    });
  }

  @Override
  public Response<List<com.ecwid.consul.v1.catalog.model.Node>> getCatalogNodes(QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<com.ecwid.consul.v1.catalog.model.Node>> catalogNodeList = getRetryConsulClient(context)
          .getCatalogNodes(queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getCatalogNodes =>  queryParams: {}  ===  catalogNodeList: {} <<<",
          queryParams, catalogNodeList);

      return catalogNodeList;
    });
  }

  @Override
  public Response<Map<String, List<String>>> getCatalogServices(QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<Map<String, List<String>>> catalogServiceMap = getRetryConsulClient(context)
          .getCatalogServices(queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getCatalogServices =>  queryParams: {}  ===  catalogServiceMap: {} <<<",
          queryParams, catalogServiceMap);

      return catalogServiceMap;
    });
  }

  @Override
  public Response<Map<String, List<String>>> getCatalogServices(QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Map<String, List<String>>> catalogServiceMap = getRetryConsulClient(context)
          .getCatalogServices(queryParams, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getCatalogServices =>  queryParams: {}  ===  token: {}  ===  catalogServiceMap: {} <<<",
          queryParams, token, catalogServiceMap);

      return catalogServiceMap;
    });
  }

  @Override
  public Response<List<CatalogService>> getCatalogService(String serviceName, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<CatalogService>> catalogServiceList = getRetryConsulClient(context)
          .getCatalogService(serviceName, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getCatalogService =>  serviceName: {}  ===  queryParams: {}  ===  catalogServiceList: {} <<<",
          serviceName, queryParams, catalogServiceList);

      return catalogServiceList;
    });
  }

  @Override
  public Response<List<CatalogService>> getCatalogService(String serviceName,
      String tag, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<List<CatalogService>> catalogServiceList = getRetryConsulClient(context)
          .getCatalogService(serviceName, tag, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getCatalogService =>  serviceName: {}  ===  tag: {}  ===  queryParams: {}  ===  catalogServiceList: {} <<<",
          serviceName, tag, queryParams, catalogServiceList);

      return catalogServiceList;
    });
  }

  @Override
  public Response<List<CatalogService>> getCatalogService(String serviceName,
      QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<CatalogService>> catalogServiceList = getRetryConsulClient(context)
          .getCatalogService(serviceName, queryParams, token);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getCatalogService =>  serviceName: {}  ===  queryParams: {}  ===  token: {}  ===  catalogServiceList: {} <<<",
          serviceName, queryParams, token, catalogServiceList);

      return catalogServiceList;
    });
  }

  @Override
  public Response<List<CatalogService>> getCatalogService(String serviceName,
      String tag, QueryParams queryParams, String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<CatalogService>> catalogServiceList = getRetryConsulClient(context)
          .getCatalogService(serviceName, tag, queryParams, token);
      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function getCatalogService =>  serviceName: {}  ===  tag: {} ===  queryParams: {}  ===  token: {}  ===  catalogServiceList: {} <<<",
          serviceName, tag, queryParams, token, catalogServiceList);

      return catalogServiceList;
    });
  }

  @Override
  public Response<CatalogNode> getCatalogNode(String nodeName, QueryParams queryParams) {
    return this.retryTemplate.execute(context -> {
      Response<CatalogNode> catalogNode = getRetryConsulClient(context).getCatalogNode(nodeName, queryParams);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getCatalogNode =>  nodeName: {}  ===  queryParams: {}  ===  catalogNode: {} <<<",
          nodeName, queryParams, catalogNode);

      return catalogNode;
    });
  }

  @Override
  public Response<Map<String, com.ecwid.consul.v1.agent.model.Check>> getAgentChecks() {
    return this.retryTemplate.execute(context -> {
      Response<Map<String, com.ecwid.consul.v1.agent.model.Check>> checkList = getRetryConsulClient(context)
          .getAgentChecks();
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getAgentChecks =>  checkList: {} <<<", checkList);

      return checkList;
    });
  }

  @Override
  public Response<Map<String, Service>> getAgentServices() {
    return this.retryTemplate.execute(context -> {
      Response<Map<String, Service>> agentServiceMap = getRetryConsulClient(context).getAgentServices();
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getAgentServices =>  agentServiceMap: {} <<<", agentServiceMap);

      return agentServiceMap;
    });
  }

  @Override
  public Response<List<Member>> getAgentMembers() {
    return this.retryTemplate.execute(context -> {
      Response<List<Member>> agentMemberList = getRetryConsulClient(context).getAgentMembers();
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getAgentMembers =>  agentMemberList: {} <<<", agentMemberList);

      return agentMemberList;
    });
  }

  @Override
  public Response<Self> getAgentSelf() {
    return this.retryTemplate.execute(context -> {
      Response<Self> agentSelf = getRetryConsulClient(context).getAgentSelf();
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getAgentSelf =>  agentSelf: {} <<<", agentSelf);

      return agentSelf;
    });
  }

  @Override
  public Response<Self> getAgentSelf(String token) {
    return this.retryTemplate.execute(context -> {
      Response<Self> agentSelf = getRetryConsulClient(context).getAgentSelf(token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getAgentSelf =>  token: {}  ===  agentSelf: {} <<<", token, agentSelf);

      return agentSelf;
    });
  }

  @Override
  public Response<Void> agentSetMaintenance(boolean maintenanceEnabled) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentSetMaintenance(maintenanceEnabled);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentSetMaintenance =>  maintenanceEnabled: {}  ===  result: {} <<<", maintenanceEnabled,
          result);

      return result;
    });
  }

  @Override
  public Response<Void> agentSetMaintenance(boolean maintenanceEnabled, String reason) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentSetMaintenance(maintenanceEnabled, reason);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentSetMaintenance =>  maintenanceEnabled: {}  ===  reason: {}  ===  result: {} <<<",
          maintenanceEnabled, reason, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentJoin(String address, boolean wan) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentJoin(address, wan);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentJoin =>  address: {}  ===  wan: {}  ===  result: {} <<<",
          address, wan, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentForceLeave(String node) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentForceLeave(node);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentForceLeave => node: {}  ===  result: {} <<<",
          node, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckRegister(NewCheck newCheck) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckRegister(newCheck);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckRegister => newCheck: {}  ===  result: {} <<<",
          newCheck, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckRegister(NewCheck newCheck, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckRegister(newCheck, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckRegister => newCheck: {}  ===  token: {}  ===  result: {} <<<",
          newCheck, token, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckDeregister(String checkId) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckDeregister(checkId);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckDeregister => checkId: {}  ===  result: {} <<<",
          checkId, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckDeregister(String checkId, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckDeregister(checkId, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckDeregister => checkId: {}  ===  token: {}  ===  result: {} <<<",
          checkId, token, result);

      return result;
    });
  }

  /**
   * ?????????????????????????????????????????????????????????
   *
   * see TtlScheduler
   */
  @Override
  public Response<Void> agentCheckPass(String checkId) {
    Response<Void> response = null;
    for (ConsulClientHolder consulClient : this.consulClients) {
      try {
        response = consulClient.getClient().agentCheckPass(checkId);
      } catch (Exception e) {
        log.warn(CommonConstant.LOG_PREFIX + ">>> {} <<<", e.getMessage());
      }
    }

    log.debug(
        CommonConstant.LOG_PREFIX + ">>> function agentCheckPass => checkId: {}  ===  response: {} <<<",
        checkId, response);

    return response;
  }

  /**
   * ?????????????????????????????????????????????????????????
   *
   * see TtlScheduler
   */
  @Override
  public Response<Void> agentCheckPass(String checkId, String note) {
    Response<Void> response = null;
    for (ConsulClientHolder consulClient : this.consulClients) {
      try {
        response = consulClient.getClient().agentCheckPass(checkId, note);
      } catch (Exception e) {
        log.warn(CommonConstant.LOG_PREFIX + ">>> {} <<<", e.getMessage());
      }
    }
    log.debug(
        CommonConstant.LOG_PREFIX + ">>> function agentCheckPass => checkId: {}  ===  note: {}  ===  response: {} <<<",
        checkId, note, response);

    return response;
  }

  /**
   * ?????????????????????????????????????????????????????????
   *
   * see TtlScheduler
   */
  @Override
  public Response<Void> agentCheckPass(String checkId, String note, String token) {
    Response<Void> response = null;
    for (ConsulClientHolder consulClient : this.consulClients) {
      try {
        response = consulClient.getClient().agentCheckPass(checkId, note, token);
      } catch (Exception e) {
        log.warn(CommonConstant.LOG_PREFIX + ">>> {} <<<", e.getMessage());
      }
    }
    log.debug(
        CommonConstant.LOG_PREFIX + ">>> function agentCheckPass => checkId: {}  ===  note: {}  ===  token: {}  ===  response: {} <<<",
        checkId, note, token, response);

    return response;
  }

  @Override
  public Response<Void> agentCheckWarn(String checkId) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckWarn(checkId);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckWarn => checkId: {}  ===  result: {} <<<",
          checkId, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckWarn(String checkId, String note) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckWarn(checkId, note);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckWarn => checkId: {}  ===  note: {}  ===  result: {} <<<",
          checkId, note, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckWarn(String checkId, String note, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckWarn(checkId, note, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckWarn => checkId: {}  ===  note: {}  ===  token: {}  ===  result: {} <<<",
          checkId, note, token, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckFail(String checkId) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckFail(checkId);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckFail => checkId: {}  ===  result: {} <<<",
          checkId, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckFail(String checkId, String note) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckFail(checkId, note);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckFail => checkId: {}  ===  note: {}  ===  result: {} <<<",
          checkId, note, result);

      return result;
    });
  }

  @Override
  public Response<Void> agentCheckFail(String checkId, String note, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).agentCheckFail(checkId, note, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentCheckFail => checkId: {}  ===  note: {}  ===  token: {}  ===  result: {} <<<",
          checkId, note, token, result);

      return result;
    });
  }

  /**
   * ???????????????????????????
   *
   * see ConsulServiceRegistry.register(...)
   */
  @Override
  public Response<Void> agentServiceRegister(NewService newService) {
    this.currentNewService = newService;

    return this.retryTemplate.execute(context -> {
      Response<Void> result = null;
      for (ConsulClientHolder consulClient : this.consulClients) {
        if (consulClient.isHealthy()) {
          result = consulClient.getClient().agentServiceRegister(newService);
        }
      }
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentServiceRegister => newService: {}  ===  result: {}  <<<",
          newService, result);

      return result;
    });
  }

  /**
   * ???????????????????????????
   *
   * see ConsulServiceRegistry.register(...)
   */
  @Override
  public Response<Void> agentServiceRegister(NewService newService, String token) {
    this.currentNewService = newService;
    this.currentToken = token;

    return this.retryTemplate.execute(context -> {
      Response<Void> result = null;
      for (ConsulClientHolder consulClient : this.consulClients) {
        if (consulClient.isHealthy()) {
          result = consulClient.getClient().agentServiceRegister(newService, token);
        }
      }
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentServiceRegister => newService: {}  ===  token: {} ===  response: {} <<<",
          newService, token, result);

      return result;
    });
  }

  /**
   * ???????????????????????????
   *
   * see ConsulServiceRegistry.deregister(...)
   */
  @Override
  public Response<Void> agentServiceDeregister(String serviceId) {
    return this.retryTemplate.execute(context -> {
      Response<Void> response = null;
      for (ConsulClientHolder consulClient : this.consulClients) {
        if (consulClient.isHealthy()) {
          response = consulClient.getClient().agentServiceDeregister(serviceId);
        }
      }

      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentServiceDeregister => serviceId: {}   ===  response: {}  <<<",
          serviceId, response);

      return response;
    });
  }

  /**
   * ???????????????????????????
   *
   * see ConsulServiceRegistry.deregister(...)
   */
  @Override
  public Response<Void> agentServiceDeregister(String serviceId, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> response = null;
      for (ConsulClientHolder consulClient : this.consulClients) {
        if (consulClient.isHealthy()) {
          response = consulClient.getClient().agentServiceDeregister(serviceId, token);
        }
      }

      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentServiceDeregister => serviceId: {}  ===  token: {}  ===  response: {}  <<<",
          serviceId, token, response);

      return response;
    });
  }

  /**
   * ?????????????????????setMaintenance
   *
   * see ConsulServiceRegistry.setStatus(...)
   */
  @Override
  public Response<Void> agentServiceSetMaintenance(String serviceId, boolean maintenanceEnabled) {
    return this.retryTemplate.execute(context -> {
      Response<Void> response = null;
      for (ConsulClientHolder consulClient : this.consulClients) {
        if (consulClient.isHealthy()) {
          response = consulClient.getClient().agentServiceSetMaintenance(serviceId,
              maintenanceEnabled);
        }
      }

      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function agentServiceSetMaintenance => serviceId: {}  ===  maintenanceEnabled: {}  ===  response: {}  <<<",
          serviceId, maintenanceEnabled, response);

      return response;
    });
  }

  /**
   * ?????????????????????setMaintenance
   *
   * see ConsulServiceRegistry.setStatus(...)
   */
  @Override
  public Response<Void> agentServiceSetMaintenance(String serviceId,
      boolean maintenanceEnabled, String reason) {
    return this.retryTemplate.execute(context -> {
      Response<Void> response = null;
      for (ConsulClientHolder consulClient : this.consulClients) {
        if (consulClient.isHealthy()) {
          response = consulClient.getClient().agentServiceSetMaintenance(serviceId,
              maintenanceEnabled, reason);
        }
      }

      log.debug(
          CommonConstant.LOG_PREFIX
              + ">>> function agentServiceSetMaintenance => serviceId: {}  ===  maintenanceEnabled: {}  ===  reason: {} ===  response: {}  <<<",
          serviceId, maintenanceEnabled, reason, response);

      return response;
    });
  }

  /**
   * ???????????????????????????????????????agentReload()??????
   */
  @Override
  public Response<Void> agentReload() {
    Response<Void> response = null;
    for (ConsulClientHolder consulClient : this.consulClients) {
      try {
        response = consulClient.getClient().agentReload();
      } catch (Exception e) {
        log.warn(CommonConstant.LOG_PREFIX + ">>> {} <<<", e.getMessage());
      }
    }

    log.debug(
        CommonConstant.LOG_PREFIX + ">>> function agentReload =>  response: {}  <<<", response);

    return response;
  }

  @Override
  public Response<String> aclCreate(NewAcl newAcl, String token) {
    return this.retryTemplate.execute(context -> {
      Response<String> acl = getRetryConsulClient(context).aclCreate(newAcl, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function aclCreate => newAcl: {}  ===  token: {}  ===  acl: {} <<<",
          newAcl, token, acl);

      return acl;
    });
  }

  @Override
  public Response<Void> aclUpdate(UpdateAcl updateAcl, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).aclUpdate(updateAcl, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function aclUpdate => updateAcl: {}  ===  token: {}  ===  result: {} <<<",
          updateAcl, token, result);

      return result;
    });
  }

  @Override
  public Response<Void> aclDestroy(String aclId, String token) {
    return this.retryTemplate.execute(context -> {
      Response<Void> result = getRetryConsulClient(context).aclDestroy(aclId, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function aclDestroy => aclId: {}  ===  token: {}  ===  result: {} <<<",
          aclId, token, result);

      return result;
    });
  }

  @Override
  public Response<Acl> getAcl(String id) {
    return this.retryTemplate.execute(context -> {
      Response<Acl> acl = getRetryConsulClient(context).getAcl(id);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getAcl => id: {}  ===  acl: {} <<<",
          id, acl);

      return acl;
    });
  }

  @Override
  public Response<String> aclClone(String aclId, String token) {
    return this.retryTemplate.execute(context -> {
      Response<String> aclClone = getRetryConsulClient(context).aclClone(aclId, token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function aclClone => aclId: {}  ===  token: {}  ===  aclClone: {} <<<",
          aclId, token, aclClone);

      return aclClone;
    });
  }

  @Override
  public Response<List<Acl>> getAclList(String token) {
    return this.retryTemplate.execute(context -> {
      Response<List<Acl>> aclList = getRetryConsulClient(context).getAclList(token);
      log.debug(
          CommonConstant.LOG_PREFIX + ">>> function getAclList => token: {}  ===  aclList: {} <<<",
          token, aclList);

      return aclList;
    });
  }
}
