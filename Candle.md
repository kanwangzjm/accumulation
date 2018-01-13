```
@Service
@Slf4j
public class DxFeedCandleReceiver implements SimpleJob {

    // 每个订阅的最大数目
    private static final int MAX_SUB_SIZE = 500;

    private static final int CANDLE_HANDLE_MINUTES = 5;

    private final static ScheduledExecutorService candleExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("candle-minute-executor"));

    private final static ScheduledExecutorService symbolExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("symbol-sync-executor"));

    private static List<DXFeedTimeSeriesSubscription<TimeSeriesEvent<?>>> candleTimeSeriesSubList = Lists.newArrayList();

    @Autowired
    private SymbolService symbolService;
    @Autowired
    private ScheduleService scheduleService;
    @Autowired
    private DailyInfoMapper dailyInfoMapper;

    // 上次同步股票列表时间
    private static Date lastSyncSymbolTime;

    /**
     * 缓存candle值, 每分钟有定时任务来取值写入分时的redis里
     * key: symbol + HHmm (中间没有连接字符)
     */
    private final static Cache<String, Candle> candleCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

    private final static Cache<String, Candle> preAfterCandleCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

    private final static Cache<String, DailyInfo> dailyInfoCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build();

    private final static Cache<String, ConcurrentHashSet<String>> minutePushedSymbolMapCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    @PostConstruct
    public void init() {

        log.info("init DxFeedCandleReceiver, add scheduled executor and job");

        /**
         * 延迟订阅dxfeed candle
         */
        Timer timer = new Timer();
        Date targetDate = DateUtils.addMinutes(new Date(), 2);
        timer.schedule(new TimerTask() {
            public void run() {
                subscribeCandle();
            }
        }, targetDate);

        /**
         * 定时处理上一分钟的candle
         * 注意: 这里需要让定时任务在每分钟开始几秒钟运行
         */
        long seconds = DateUtils.getFragmentInSeconds(new Date(), Calendar.MINUTE);
        candleExecutorService.scheduleAtFixedRate(() -> {
            handleLatestCandles();
            handleLatestPreAfterCandles();
        }, 60 + 5 - seconds, 60, TimeUnit.SECONDS);

        /**
         * 定时更新订阅的symbol
         */
        symbolExecutorService.scheduleAtFixedRate(() -> handleUpdatedSymbols(), 60, 60, TimeUnit.SECONDS);
    }


    @Override
    public void execute(ShardingContext shardingContext) throws Exception {
        resubscribe();
    }

    private void resubscribe() {
        // 关闭之前的订阅
        for(DXFeedTimeSeriesSubscription<TimeSeriesEvent<?>> candleTimeSeriesSub : candleTimeSeriesSubList) {
            candleTimeSeriesSub.clear();
            candleTimeSeriesSub.close();
        }
        candleTimeSeriesSubList = Lists.newArrayList();

        // 重新订阅
        subscribeCandle();
    }

    /**
     * 根据最新的股票列表, 添加/取消相关订阅
     */
    public void handleUpdatedSymbols() {
        JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_SYMBOL_CHECK);
        List<Symbol> updatedSymbolList = symbolService.selectSymbolsByDate(lastSyncSymbolTime);
        lastSyncSymbolTime = new Date();
        if (CollectionUtils.isEmpty(updatedSymbolList)) {
            return;
        }
        // 发现有变更就重新订阅好了
        resubscribe();
    }

    /**
     * 订阅dxfeed candle
     */
    private void subscribeCandle() {
        try {
            long from = System.currentTimeMillis() - 10 * 60 * 1000;
            Set<String> stockIndexSet = RedisHelper.getMarketRedisClient().hashs().hkeys(RedisKeyGenerator.getUSValidSymbolsData());
            if (CollectionUtils.isEmpty(stockIndexSet)) {
                stockIndexSet = symbolService.selectValidSymbols().stream().map(symbol -> symbol.getSymbol()).collect(Collectors.toSet());
            }
            stockIndexSet.removeAll(IndexHelper.getIndexSet());
            List<String> symbolList = Lists.newArrayList(stockIndexSet);

            lastSyncSymbolTime = new Date();

            int size = (symbolList.size() + MAX_SUB_SIZE - 1) / MAX_SUB_SIZE;
            for (int i = 0; i < size; i++) {
                // symbol{=1m}才可以从接口中拿到vwap数据, 1m代表时间频次为1min
                int fromIndex = MAX_SUB_SIZE * i;
                int toIndex = MAX_SUB_SIZE * (i + 1) > symbolList.size() ? symbolList.size() : MAX_SUB_SIZE * (i + 1);
                String[] symbols = symbolList.subList(fromIndex, toIndex).stream().map(s -> s + "{=1m}").toArray(String[]::new);

                DXFeedTimeSeriesSubscription<TimeSeriesEvent<?>> candleTimeSeriesSub = DXFeed.getInstance().createTimeSeriesSubscription(Candle.class);
                candleTimeSeriesSub.addEventListener(new CandleEventListener<>());
                candleTimeSeriesSub.setFromTime(from);
                candleTimeSeriesSub.addSymbols(symbols);
                candleTimeSeriesSub.addChangeListener(new CandleObservableSubscriptionChangeListener());
                candleTimeSeriesSubList.add(candleTimeSeriesSub);
            }
        } catch (Throwable t) {
            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_SUB_FAILED);
            log.error("candle subscribe exception", t);
            safeThreadSleep(10000);
            subscribeCandle();
        }
    }

    /**
     * 处理dxfeed的推送信息
     */
    private class CandleEventListener<T> implements DXFeedEventListener<T> {
        @Override
        public void eventsReceived(List<T> events) {
            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_PUSH_TIMES);
            JMonitor.incrRecord(MonitorConstant.DXFEED.DXFEED_CANDLE_PUSH, events.size());
            SimpleDateFormat hhmmFormat = new SimpleDateFormat("HHmm");
            hhmmFormat.setTimeZone(DxFeedCandleHelper.timeZoneXNYS);
            for (T event : events) {
                try {
                    Candle currentCandle = (Candle) event;
                    if (!DxFeedCandleHelper.isCandleLegal(currentCandle)) { // candle 不合法
                        JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_DROP_NO_LEGAL);
                        continue;
                    }

                    String symbol = currentCandle.getEventSymbol().getBaseSymbol();
                    String candleTimeFormat = hhmmFormat.format(currentCandle.getTime());
                    String candleCacheKey = getCandleGuavaCacheKey(symbol, candleTimeFormat);
                    MarketLogHelper.logCandle(symbol, currentCandle, candleTimeFormat);

                    // 记录一下candle延迟的时间
                    JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_TIME_DELAY, System.currentTimeMillis() - currentCandle.getTime());

                    if (DxFeedCandleHelper.isInTrading(candleTimeFormat)) {  // 处理开市期间分时数据
                        Candle cacheCandle = candleCache.getIfPresent(candleCacheKey);
                        if (cacheCandle == null || cacheCandle.getTime() <= currentCandle.getTime()) {
                            candleCache.put(candleCacheKey, currentCandle);
                            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_EXECUTE);
                            minutePushedSymbolMapCache.get(candleTimeFormat, () -> new ConcurrentHashSet<>()).add(symbol.intern());
                        } else {
                            // 数据不是最新
                            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_DROP);
                        }
                    } else if (DxFeedCandleHelper.isPreMarketOrAfterHour(candleTimeFormat)) { // 处理盘前盘后分时数据
                        Candle preAfterCandle = preAfterCandleCache.getIfPresent(candleCacheKey);
                        if (preAfterCandle == null || preAfterCandle.getTime() <= currentCandle.getTime()) {
                            preAfterCandleCache.put(candleCacheKey, currentCandle);
                            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_EXECUTE_PRE_AFTER);
                            minutePushedSymbolMapCache.get(candleTimeFormat, () -> new ConcurrentHashSet<>()).add(symbol.intern());
                        } else {
                            // 数据不是最新
                            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_DROP_PRE_AFTER);
                        }
                    } else { // 不是开市的数据,也不是盘前盘后的数据
                        JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_DROP_NO_USE);
                    }
                } catch (Throwable t) {
                    log.error("handle candle exception, event:" + JsonUtils.toJson(event), t);
                    JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_HANDLE_EXCEPTION);
                }
            }
        }
    }

    /**
     * 处理dxfeed变更的消息
     */
    private class CandleObservableSubscriptionChangeListener implements ObservableSubscriptionChangeListener {
        @Override
        public void symbolsAdded(Set<?> set) {
            JMonitor.incrRecord(MonitorConstant.DXFEED.DXFEED_CANDLE_SUB_SYMBOL_ADD, set.size());
            log.info("candle add symbols subscribe succeed, size:{}", set.size());
        }

        @Override
        public void symbolsRemoved(Set<?> set) {
            JMonitor.incrRecord(MonitorConstant.DXFEED.DXFEED_CANDLE_SUB_SYMBOL_REMOVE, set.size());
            log.info("candle remove symbols subscribe succeed, size:{}", set.size());
        }

        @Override
        public void subscriptionClosed() {
            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_SUB_CLOSE);
        }
    }

    /**
     * 处理开市期间的candle
     */
    private void handleLatestCandles() {
        boolean currentIsTradingTime = scheduleService.isTradingSession(new Date());
        if (currentIsTradingTime) {
            Date date = DateUtils.addMinutes(new Date(), -1);
            boolean lastMinuteIsTradingTime = scheduleService.isTradingSession(date);
            if (!lastMinuteIsTradingTime) {
                // 当前这一分钟是交易时间, 而且上一分钟不是交易时间, 说明这一分钟开始:开市, 还没有数据能处理
                return;
            }
        }
        if (!currentIsTradingTime) {
            // 当前这一分钟不是交易时间, 说明:闭市, 闭市前几分钟的数据需要处理
            Date latestTime = DateUtils.addMinutes(new Date(), -1 * CANDLE_HANDLE_MINUTES);
            boolean tradingDateTime = scheduleService.isTradingSession(latestTime);
            if (!tradingDateTime) {
                return;
            }
        }

        if (candleCache.size() == 0) {
            log.info("no candle data to handle");
            return;
        }
        try {
            JMonitor.recordOne(MonitorConstant.buildSuccessKey(MonitorConstant.DXFEED.DXFEED_CANDLE_HANDLE_LATEST));
            // 正常处理开市时的candle接收处理逻辑, currentIsTradingTime与lastMinuteIsTradingTime都为true
            SimpleDateFormat hhmmFormat = new SimpleDateFormat("HHmm");
            hhmmFormat.setTimeZone(DxFeedCandleHelper.timeZoneXNYS);
            SimpleDateFormat monthDayFormat = new SimpleDateFormat("MMdd");
            monthDayFormat.setTimeZone(DxFeedCandleHelper.timeZoneXNYS);
            String monthDay = monthDayFormat.format(new Date());

            List<String> timeList = Lists.newArrayList();
            for (int i = 1; i <= CANDLE_HANDLE_MINUTES; i++) {
                String tempHHmm = hhmmFormat.format(DateUtils.addMinutes(new Date(), -1 * i));
                timeList.add(tempHHmm);
                // 记录一下前i分钟推送不同[股票]的数量
                // 因为推送有延迟, 只记录上1分钟可能不准, 这里保持和更新redis的时间一致
                JMonitor.recordSize(MonitorConstant.DXFEED.DXFEED_CANDLE_PUSH_SYMBOLS + "_" + i,
                        minutePushedSymbolMapCache.get(tempHHmm, () -> new ConcurrentHashSet<>()).size());

            }
            Set<String> symbolList = RedisHelper.getMarketRedisClient().hashs().hkeys(RedisKeyGenerator.getUSValidSymbolsData());
            if (CollectionUtils.isEmpty(symbolList)) {
                symbolList = symbolService.selectValidSymbols().stream().map(symbol -> symbol.getSymbol()).collect(Collectors.toSet());
            }
            for (String symbol : symbolList) { // 遍历symbol挨个处理
                if (IndexHelper.getIndexSet().contains(symbol)) {
                    continue;
                }
                Map<String, String> hhmmMap = Maps.newHashMap();
                for (String hhmm : timeList) {
                    // 得到最新的candle
                    String key = getCandleGuavaCacheKey(symbol, hhmm);
                    Candle candle = candleCache.getIfPresent(key);
                    if (candle == null) {
                        continue;
                    }
                    // 生成 timeTrend 及 bar
                    TimeTrend timeTrend = DxFeedCandleHelper.adaptTimeTrend(candle, symbol, getDailyInfo(symbol), false);
                    if (timeTrend != null) {
                        JimuStockBar bar = TimeTrendUtils.fromTimeTrendToJMStockBar(timeTrend);
                        hhmmMap.put(hhmm, JsonUtils.toJson(bar));


                        // 在本地记录下来。 其实candle都可以在本地记录下来
                        candleValueTableHolder.putPriceVolume(symbol, hhmm,
                                new PriceVolume(timeTrend.getHigh(), timeTrend.getLow(), timeTrend.getVolume()));
                    }
                }
                // 更新redis
                if (MapUtils.isNotEmpty(hhmmMap)) {
                    String redisKey = RedisKeyGenerator.getTimeTrendKey(symbol, monthDay);
                    saveToRedis(redisKey, symbol, hhmmMap);
                }
            }
        } catch (Throwable t) {
            log.error("handle latest candles exception", t);
            JMonitor.recordOne(MonitorConstant.buildFailKey(MonitorConstant.DXFEED.DXFEED_CANDLE_HANDLE_LATEST));
        }
    }

    /**
     * 处理盘前盘后的candle
     */
    private void handleLatestPreAfterCandles() {
        Date currentTime = new Date();
        if (!scheduleService.isTradingDay(currentTime)) {
            return;
        }
        String newYorkHHmmStr = JodaTimeUtil.formatHM(currentTime, JodaTimeUtil.AmericaZone);
        int newYorkHHmm = Integer.parseInt(newYorkHHmmStr);
        boolean isPre = (newYorkHHmm >= TradingConstant.PRE_MARKET_START && newYorkHHmm < TradingConstant.TRADING_START + CANDLE_HANDLE_MINUTES);     // 处理盘前段
        boolean isPost = (newYorkHHmm > TradingConstant.TRADING_END && newYorkHHmm <= TradingConstant.AFTER_HOUR_END + CANDLE_HANDLE_MINUTES);  // 处理盘后段

        if (!isPre && !isPost) {
            return;
        }
        if (preAfterCandleCache.size() == 0) {
            log.info("no candle data to handle pre or after");
            return;
        }
        try {
            JMonitor.recordOne(MonitorConstant.buildSuccessKey(MonitorConstant.DXFEED.DXFEED_CANDLE_HANDLE_LATEST_PRE_AFTER));
            String monthDay = JodaTimeUtil.formatMD(currentTime, JodaTimeUtil.AmericaZone);
            List<String> timeList = Lists.newArrayList();
            for (int i = 1; i <= CANDLE_HANDLE_MINUTES; i++) {
                String tempHHmm = JodaTimeUtil.formatHM(DateUtils.addMinutes(new Date(), -1 * i), JodaTimeUtil.AmericaZone);
                timeList.add(tempHHmm);
                // 记录一下前i分钟推送不同[股票]的数量
                // 因为推送有延迟, 只记录上1分钟可能不准, 这里保持和更新redis的时间一致
                JMonitor.recordSize(MonitorConstant.DXFEED.DXFEED_CANDLE_PUSH_SYMBOLS + "_" + i,
                        minutePushedSymbolMapCache.get(tempHHmm, () -> new ConcurrentHashSet<>()).size());

            }
            Set<String> symbolList = RedisHelper.getMarketRedisClient().hashs().hkeys(RedisKeyGenerator.getUSValidSymbolsData());
            if (CollectionUtils.isEmpty(symbolList)) {
                symbolList = symbolService.selectValidSymbols().stream().map(symbol -> symbol.getSymbol()).collect(Collectors.toSet());
            }
            for (String symbol : symbolList) { // 遍历symbol挨个处理
                if (IndexHelper.getIndexSet().contains(symbol)) {
                    continue;
                }
                Map<String, String> hhmmMap = Maps.newHashMap();
                for (String hhmm : timeList) {
                    // 得到最新的candle
                    String key = getCandleGuavaCacheKey(symbol, hhmm);
                    Candle candle = preAfterCandleCache.getIfPresent(key);
                    if (candle == null) {
                        continue;
                    }
                    TimeTrend timeTrend = DxFeedCandleHelper.adaptTimeTrend(candle, symbol, null, false); // 昨收价及相关放在取分时线时补充
                    if (timeTrend != null) {
                        hhmmMap.put(hhmm, JsonUtils.toJson(TimeTrendUtils.fromTimeTrendToJMStockBar(timeTrend)));
                    }
                }
                // 更新redis
                if (MapUtils.isNotEmpty(hhmmMap)) {
                    // 保证开市中,盘前,盘后分时数据使用不同的redis值
                    String redisKey = isPre ? RedisKeyGenerator.getPreMarketTimeTrendKey(symbol, monthDay) : RedisKeyGenerator.getAfterHourTimeTrendKey(symbol, monthDay);
                    saveToRedis(redisKey, symbol, hhmmMap);
                }
            }
        } catch (Throwable t) {
            log.error("handle latest pre or after candles exception", t);
            JMonitor.recordOne(MonitorConstant.buildFailKey(MonitorConstant.DXFEED.DXFEED_CANDLE_HANDLE_LATEST_PRE_AFTER));
        }
    }

    private void saveToRedis(String redisKey, String symbol, Map<String, String> map) {
        if (MapUtils.isEmpty(map)) {
            return;
        }
        long start = System.currentTimeMillis();
        try {
            RedisHelper.getPriceRedis().hashs().hmset(redisKey, map);
            JMonitor.recordOne(MonitorConstant.buildSuccessKey(MonitorConstant.DXFEED.DXFEED_CANDLE_DATA_SAVE_REDIS), System.currentTimeMillis() - start);
        } catch (Throwable t) {
            log.error("insert timeTrend to redis exception, symbol:" + symbol, t);
            JMonitor.recordOne(MonitorConstant.buildFailKey(MonitorConstant.DXFEED.DXFEED_CANDLE_DATA_SAVE_REDIS), System.currentTimeMillis() - start);
        }
    }

    /**
     * 定义guava cache中key生成规则
     */
    private String getCandleGuavaCacheKey(String symbol, String hhmmFormat) {
        return symbol + hhmmFormat;
    }

    private static void safeThreadSleep(long mills) {
        try {
            Thread.currentThread().sleep(mills);
        } catch (Throwable t) {
            log.error("thread sleep exception", t);
        }
    }

    private DailyInfo getDailyInfo(String symbol) {
        DailyInfo lastDailyInfo = dailyInfoCache.getIfPresent(symbol);
        if (lastDailyInfo == null) {
            String instrumentId = symbolService.selectInstrumentIdBySymbol(symbol);
            List<DailyInfo> dailyInfoList = dailyInfoMapper.getLastNDailyInfo(instrumentId, 1);
            if (CollectionUtils.isEmpty(dailyInfoList)) {
                return null;
            }
            lastDailyInfo = dailyInfoList.get(0);
            dailyInfoCache.put(symbol, lastDailyInfo);
        }
        return lastDailyInfo;
    }
}
```