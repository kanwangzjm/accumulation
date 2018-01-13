```
@Service
@Slf4j
public class DxFeedCandleHistoryReceiver implements SimpleJob {

    private static Map<String, Map<String, TimeTrend>> hhmmCandleMap = Maps.newConcurrentMap();

    private static long stopTime = System.currentTimeMillis();
    private static long subFromTime = System.currentTimeMillis();
    private static String monthDay = "";
    private static boolean saveDB = true;
    private static Set<String> symbolSet = Sets.newHashSet();
    private static boolean supportUpdate = false;
    private static boolean onlyUpdate = false;

    // 核心线程池20个, Pool最大20, alive时间2min
    private static final int CORE_POOL_SIZE = 20;
    private static final int MAXIMUM_POOL_SIZE = 20;
    private static final long KEEP_ALIVE_TIME = 120L;

    // 设置插入速度限流200
    private static final RateLimiter rateLimiter = RateLimiter.create(200);

    private static final ThreadPoolExecutor saveToDBExecutor = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>());

    private final static ScheduledExecutorService subCheckExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("subscribe-check-executor"));

    @Autowired
    private TimeTrendMapper timeTrendMapper;
    @Autowired
    private ScheduleService scheduleService;
    @Autowired
    private DailyInfoMapper dailyInfoMapper;
    @Autowired
    private SymbolMapper symbolMapper;
    @Autowired
    private SymbolService symbolService;

    @PostConstruct
    public void init() {
        ThreadPoolMonitorRegistry.getInstance().register(MonitorConstant.DXFEED.DXFEED_CANDLE_SAVE_DB_THREADPOOL, saveToDBExecutor);

        subCheckExecutor.scheduleAtFixedRate(() -> {
            JMonitor.recordSize(MonitorConstant.DXFEED.DXFEED_CANDLE_HISTORY_SUB_LEFT, symbolSet.size());
            if (symbolSet.size() > 0 && symbolSet.size() < 1000) {
                log.info("left subscribe history candle symbols, {}", JsonUtils.toJson(symbolSet));
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    @Override
    public void execute(ShardingContext shardingContext) {
        JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_HISTORY_RECEIVER_CONSOLE);
        subscribe(shardingContext);
    }

    private void subscribe(ShardingContext shardingContext) {
        boolean isForceExecute = false;
        saveDB = true;
        supportUpdate = false;
        onlyUpdate = false;
        subFromTime = getLatestTradingFrom();
        List<String> symbolList = null;

        String parameterSubFrom = "";
        String parameterSupportUpdate = "";
        String parameterOnlyUpdate = "";
        String parameterSymbols = "";
        String parameterForce = "";
        String parameterSaveDB = "";

        if (StringUtils.isNotBlank(shardingContext.getJobParameter())) {
            for (String parameter : Splitter.on(";").splitToList(shardingContext.getJobParameter())) {
                List<String> kv = Splitter.on("=").splitToList(parameter);
                if (StringUtils.equalsIgnoreCase(kv.get(0), "symbols")) {
                    parameterSymbols = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "subFrom")) {
                    parameterSubFrom = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "force")) {
                    parameterForce = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "supportUpdate")) {
                    parameterSupportUpdate = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "onlyUpdate")) {
                    parameterOnlyUpdate = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "saveDB")) {
                    parameterSaveDB = kv.get(1);
                }
            }
        }
        if (StringUtils.isNotBlank(parameterSymbols)) { // 默认订阅所有有效的symbol, 如果包含symbols参数,则只订阅指定的symbols
            isForceExecute = true;
            saveDB = false;
            symbolList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(parameterSymbols);
        } else {
            symbolList = Lists.newArrayList(RedisHelper.getMarketRedisClient().hashs().hkeys(RedisKeyGenerator.getUSValidSymbolsData()));
            if (CollectionUtils.isEmpty(symbolList)) {
                symbolList = symbolMapper.selectValidSymbols().stream().map(symbol -> symbol.getSymbol()).collect(Collectors.toList());
            }
        }

        if (StringUtils.isNotBlank(parameterSubFrom)) { // 默认订阅从最近的开市时间开始订阅, 如果包含subFrom参数且时间大于最近的开始时间,则使用subFrom代替
            saveDB = false;
            isForceExecute = true;
            try {
                long subFromParameter = com.jimubox.stock.common.util.DateUtils.parseDateFromString(parameterSubFrom, "yyyyMMdd-HHmmss").getTime();
                if (subFromParameter > subFromTime && subFromParameter < System.currentTimeMillis()) {
                    subFromTime = subFromParameter;
                }
            } catch (Throwable t) {
                log.error("parse subFrom time exception, use default, subTimeFrom:" + parameterSubFrom, t);
            }
        }
        if (StringUtils.isNotBlank(parameterForce)) { // 默认非交易日不执行此任务, 如果包含force参数,则强制定时任务执行
            isForceExecute = true;
            saveDB = false;
        }

        if (!saveDB) {
            if (StringUtils.isNotBlank(parameterSaveDB)) { // 默认手动执行时不做saveDB操作. 如果包含saveDB参数,则强制执行saveDB操作
                isForceExecute = true;
                saveDB = true;
            }
        }

        if (StringUtils.isNotBlank(parameterSupportUpdate)) { // 默认不做update操作,如果包含supportUpdate参数,则在执行insert时遇到DuplicateKeyException会做更新操作
            supportUpdate = true;
        }

        if (StringUtils.isNotBlank(parameterOnlyUpdate)) { // 如果包含onlyUpdate参数,则不执行insert直接执行update
            onlyUpdate = true;
        }
        log.info("subFrom:{}, {}, saveDB:{}, isForceExecute:{}, supportUpdate:{}, onlyUpdate:{}", subFromTime, new Date(subFromTime), saveDB, isForceExecute,
                supportUpdate, onlyUpdate);

        if (!isForceExecute) {
            // 没有指定的参数(说明不是手动执行的),而且当前不属于交易日,则不需要执行下面的操作
            if (!scheduleService.isTradingDay(new Date())) {
                return;
            }
        }
        stopTime = DateUtils.addMinutes(new Date(), 2).getTime();

        SimpleDateFormat monthDayFormat = new SimpleDateFormat("MMdd");
        monthDayFormat.setTimeZone(DxFeedCandleHelper.timeZoneXNYS);
        monthDay = monthDayFormat.format(new Date(subFromTime));

        symbolSet = Sets.newHashSet();
        for (String symbol : symbolList) {
            if (IndexHelper.getIndexSet().contains(symbol)) {
                continue;
            }
            hhmmCandleMap.put(symbol, Maps.newConcurrentMap());
            symbolSet.add(symbol);
        }

        String[] dxfeedSymbols = symbolSet.stream().map(s -> s + "{=1m}").toArray(String[]::new);
        DXFeedTimeSeriesSubscription<TimeSeriesEvent<?>> candleTimeSeriesSub = DXFeed.getInstance().createTimeSeriesSubscription(Candle.class);
        candleTimeSeriesSub.addEventListener(new CandleHistoryEventListener<>());
        candleTimeSeriesSub.setFromTime(subFromTime);
        candleTimeSeriesSub.addSymbols(dxfeedSymbols);
        candleTimeSeriesSub.addChangeListener(new CandleHistoryChangeListener());

        Timer timer = new Timer();
        Date targetDate = DateUtils.addMinutes(new Date(), 60);
        timer.schedule(new TimerTask() {
            public void run() {
                // 60分钟后, 如果有没订阅完,则自动处理(处理某些股票订阅时出现异常无法关闭订阅)
                if (CollectionUtils.isNotEmpty(symbolSet)) {
                    Set<String> toRemovedSymbolSet = Sets.newHashSet(symbolSet);
                    for (String toRemovedSymbol : toRemovedSymbolSet) {
                        log.info("force remove history candle subscribe, symbol:{}", toRemovedSymbol);
                        removeSubAndToRedis(toRemovedSymbol);
                    }
                }
                log.info("force close candle subscribe, symbol size:{}", symbolSet.size());
                candleTimeSeriesSub.clear();
                candleTimeSeriesSub.close();
            }
        }, targetDate);
    }

    private void removeSubAndToRedis(String symbol) {
        if (!symbolSet.contains(symbol)) {
            return;
        }
        symbolSet.remove(symbol);

        Map<String, TimeTrend> map = hhmmCandleMap.get(symbol);
        map = mergeRedisValue(map, symbol);
        if (MapUtils.isEmpty(map)) {
            log.warn("no timetrend to insert, symbol:{}", symbol);
            return;
        }
        String redisKey = RedisKeyGenerator.getTimeTrendKey(symbol, monthDay);

        Map<String, String> barMap = Maps.newHashMap();
        List<TimeTrend> timeTrendList = Lists.newArrayList();
        String instrumentId = symbolService.selectInstrumentIdBySymbol(symbol);
        List<DailyInfo> dailyInfoList = dailyInfoMapper.getLastNDailyInfo(instrumentId, 1);
        DailyInfo dailyInfo = CollectionUtils.isNotEmpty(dailyInfoList) ? dailyInfoList.get(0) : null;

        for (Map.Entry<String, TimeTrend> entry : map.entrySet()) {
            TimeTrend timeTrend = entry.getValue();
            if (dailyInfo != null) {
                timeTrend.setPreviousclose(dailyInfo.getLast());
            }
            JimuStockBar bar = TimeTrendUtils.fromTimeTrendToJMStockBar(timeTrend);
            barMap.put(entry.getKey(), JsonUtils.toJson(bar));
            timeTrendList.add(entry.getValue());
        }

        long start = System.currentTimeMillis();
        try {
            RedisHelper.getPriceRedis().hashs().hmset(redisKey, barMap);
            JMonitor.recordOne(MonitorConstant.buildSuccessKey(MonitorConstant.DXFEED.DXFEED_CANDLE_HISTORY_DATA_SAVE_REDIS),
                    System.currentTimeMillis() - start);
        } catch (Throwable t) {
            log.error("insert history timeTrend to redis exception, symbol:" + symbol, t);
            JMonitor.recordOne(MonitorConstant.buildFailKey(MonitorConstant.DXFEED.DXFEED_CANDLE_HISTORY_DATA_SAVE_REDIS), System.currentTimeMillis() - start);
        }

        if (saveDB && CollectionUtils.isNotEmpty(timeTrendList)) {
            Collections.sort(timeTrendList, new Comparator<TimeTrend>() {
                @Override
                public int compare(TimeTrend o1, TimeTrend o2) {
                    return o1.getEndtime().compareTo(o2.getEndtime());
                }
            });

            saveToDBExecutor.execute(() -> {
                int insertSuccess = 0;
                int filterCount = 0;
                int failedCount = 0;
                for (TimeTrend timeTrend : timeTrendList) {
                    if (!DxFeedCandleHelper.isTradingHourMinute(timeTrend.getEndtime())) {
                        filterCount++;
                        continue;
                    }
                    try {
                        boolean isAcquire = rateLimiter.tryAcquire(); // 限速执行
                        if (!isAcquire) {
                            rateLimiter.acquire();
                        }
                        long dbStart = System.currentTimeMillis();
                        timeTrend.setInstrumentId(instrumentId);
                        if (onlyUpdate) {
                            timeTrendMapper.updateByPrimaryKeySelective(instrumentId, timeTrend);
                        } else {
                            timeTrendMapper.insertSelective(instrumentId, timeTrend); // shard许多不支持...
                        }
                        JMonitor.recordOne(MonitorConstant.buildSuccessKey(MonitorConstant.DXFEED.DXFEED_CANDLE_DATA_SAVE_DB),
                                System.currentTimeMillis() - dbStart);
                        insertSuccess++;
                    } catch (Throwable t) {
                        if (!(t instanceof DuplicateKeyException || t instanceof TooManyResultsException)) {
                            log.error("insert timeTrend to db exception, symbol:" + symbol, t);
                            JMonitor.recordOne(MonitorConstant.buildFailKey(MonitorConstant.DXFEED.DXFEED_CANDLE_DATA_SAVE_DB));
                            failedCount++;
                        } else if (supportUpdate) {
                            try {
                                timeTrendMapper.updateByPrimaryKeySelective(instrumentId, timeTrend);
                            } catch (Throwable t1) {
                                log.error("update timeTrend to db exception, symbol:" + symbol, t1);
                            }
                        }
                    }
                }
                log.info("insert timeTrendList, symbol:{}, listSize:{}, insertSuccess:{}, filterCount:{}, failedCount:{}", symbol, timeTrendList.size(),
                        insertSuccess, filterCount, failedCount);
            });
        }

        // 释放内存
        hhmmCandleMap.get(symbol).clear();
    }

    private long getLatestTradingFrom() {
        TradingSession tradingSession = scheduleService.getLastTradingSession(new Date());
        return tradingSession.getStart();
    }

    /**
     * 处理dxfeed的推送信息
     */
    private class CandleHistoryEventListener<T> implements DXFeedEventListener<T> {
        @Override
        public void eventsReceived(List<T> events) {
            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_HISTORY_CANDLE_PUSH_TIMES);
            JMonitor.incrRecord(MonitorConstant.DXFEED.DXFEED_HISTORY_CANDLE_PUSH, events.size());
            SimpleDateFormat hhmmFormat = new SimpleDateFormat("HHmm");
            hhmmFormat.setTimeZone(DxFeedCandleHelper.timeZoneXNYS);
            for (T event : events) {
                try {
                    Candle currentCandle = (Candle) event;
                    String symbol = currentCandle.getEventSymbol().getBaseSymbol();
                    String candleTimeFormat = hhmmFormat.format(currentCandle.getTime());
                    pushToMap(currentCandle, symbol, candleTimeFormat);

                    if (currentCandle.getTime() >= stopTime || currentCandle.getTime() <= subFromTime) {
                        removeSubAndToRedis(symbol);
                    }
                } catch (Throwable t) {
                    log.error("handle candle exception, event:" + JsonUtils.toJson(event), t);
                    JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_HANDLE_EXCEPTION);
                }
            }
        }
    }

    private void pushToMap(Candle currentCandle, String symbol, String candleTimeFormat) {
        TimeTrend timeTrend = DxFeedCandleHelper.adaptTimeTrend(currentCandle, symbol, null, true);
        if (timeTrend != null) {
            hhmmCandleMap.get(symbol).put(candleTimeFormat, timeTrend);
        }
    }

    private Map<String, TimeTrend> mergeRedisValue(Map<String, TimeTrend> map, String symbol) {
        if (MapUtils.isEmpty(map) || IndexHelper.getIndexSet().contains(symbol)) {
            map = Maps.newHashMap();
        }
        try {
            int count = 0;
            Map<String, String> barMap = RedisHelper.getPriceRedis().hashs().hgetAll(RedisKeyGenerator.getTimeTrendKey(symbol, monthDay));
            if (MapUtils.isEmpty(barMap)) {
                return map;
            }
            for (Map.Entry<String, String> entry : barMap.entrySet()) {
                if (!map.containsKey(entry.getKey())) { // 订阅的历史数据中不包含,尝试使用redis中的数据进行补充
                    String barValue = entry.getValue();
                    JimuStockBar jimuStockBar = JsonUtils.fromJson(barValue, JimuStockBar.class);

                    TimeTrend timeTrend = adapt(jimuStockBar, symbol);
                    if (timeTrend != null && DxFeedCandleHelper.isTradingHourMinute(timeTrend.getEndtime())) {
                        map.put(entry.getKey(), timeTrend);
                        count++;
                        log.info("merge redis data, symbol:{}, {}", symbol, barValue);
                    }
                }
            }
            if (count > 0) {
                JMonitor.incrRecord(MonitorConstant.DXFEED.DXFEED_CANDLE_HISTORY_DATA_MERGE_REDIS, count);
            }
        } catch (Throwable t) {
            log.warn("handle redis timetrend exception, symbol:" + symbol, t);
        }
        return map;
    }

    private TimeTrend adapt(JimuStockBar bar, String symbol) {
        if (bar == null) {
            return null;
        }
        TimeTrend timeTrend = new TimeTrend();
        timeTrend.setSymbol(symbol);
        timeTrend.setOpenprice(bar.getOpen());
        timeTrend.setHigh(bar.getHigh());
        timeTrend.setLow(bar.getLow());
        timeTrend.setClose(bar.getClose());
        timeTrend.setVwap(bar.getVWAP());
        timeTrend.setEndtime(JodaTimeUtil.parse("MM/dd/yyyy hh:mm:ss aa", bar.getEndDate() + " " + bar.getEndTime()).toDate());
        timeTrend.setVolume(bar.getVolume());
        timeTrend.setPreviousclose(bar.getPreClose());
        return timeTrend;
    }

    /**
     * 处理dxfeed变更的消息
     */
    private class CandleHistoryChangeListener implements ObservableSubscriptionChangeListener {
        @Override
        public void symbolsAdded(Set<?> set) {
            JMonitor.incrRecord(MonitorConstant.DXFEED.DXFEED_CANDLE_HISTORY_SUB_SYMBOL_ADD, set.size());
        }

        @Override
        public void symbolsRemoved(Set<?> set) {
            JMonitor.incrRecord(MonitorConstant.DXFEED.DXFEED_CANDLE_HISTORY_SUB_SYMBOL_REMOVE, set.size());
        }

        @Override
        public void subscriptionClosed() {
            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_SUB_CLOSE);
        }
    }
}
```