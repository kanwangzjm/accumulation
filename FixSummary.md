```
@Service
@Slf4j
public class FixSummary implements SimpleJob {

    @Autowired
    private SymbolMapper symbolMapper;
    @Autowired
    private ScheduleService scheduleService;

    @Override
    public void execute(ShardingContext shardingContext) throws Exception {

        List<String> symbolList;
        String parameterSymbols = "";
        String parameterTarget = "";
        String parameterFrom = "";
        String parameterColumns = "";
        boolean forceExecute = false; // 必须指定【force】参数才执行，否则走默认逻辑可能会出错！！！

        if (StringUtils.isNotBlank(shardingContext.getJobParameter())) {
            for (String parameter : Splitter.on(";").omitEmptyStrings().trimResults().splitToList(shardingContext.getJobParameter())) {
                List<String> kv = Splitter.on("=").splitToList(parameter);
                if (StringUtils.equalsIgnoreCase(kv.get(0), "symbols")) {        // symbols代表指定修正的股票标识，默认全量
                    parameterSymbols = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "target")) {  // target=delay 代表要修正的是延迟数据，默认修正的是实时数据
                    parameterTarget = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "from")) {    // from=candle 代表通过timetrend修正，默认实时和延迟的summary互相修正
                    parameterFrom = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "columns")) { // columns代表修正的字段，默认为all, 支持：all、dayClose、dayOpen、dayHigh、dayLow、prevDayClose，通过字符串包含的形式，需要注意大小写(因为close有两个字段)
                    parameterColumns = kv.get(1);
                } else if (StringUtils.equalsIgnoreCase(kv.get(0), "force")) {   // force参数存在时才执行！！！！！
                    forceExecute = true;
                }
            }
        }

        if (!forceExecute) {
            log.warn("no need to execute FixSummary Job, parameters must contain 'force'");
            return;
        }

        if (StringUtils.isNotBlank(parameterSymbols)) {
            symbolList = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(parameterSymbols);
        } else {
            symbolList = symbolMapper.selectValidSymbols().stream().map(symbol -> symbol.getSymbol()).collect(Collectors.toList());
        }

        boolean isDelay = false;
        if (StringUtils.isNotBlank(parameterTarget) && parameterTarget.toLowerCase().equals("delay")) {
            isDelay = true;
        }

        String monthDay = getLastUsTradingMMdd();
        for (String symbol : symbolList) {
            try {
                fixSymbolSummary(symbol, monthDay, parameterFrom, parameterColumns, isDelay);
            } catch (Exception e) {
                log.error("fixSymbolSummary exception, symbol:" + symbol, e);
            }
        }
    }

    /**
     * 修正指定股票的summary或delaySummary
     *
     * @param symbol           股票标识，比如: CTRP,AAPL
     * @param monthDay         对应美股的交易日期，使用candle来修正时拼装对应的redisKey。比如：0203，AAPL在redis里的key为：TMT_AAPL0203
     * @param parameterFrom    通过什么值来修复，from=candle代表通过timetrend修正，默认实时和延迟的summary互相修正
     * @param parameterColumns 修正的字段，默认为all, 支持：all、dayClose、dayOpen、dayHigh、dayLow、prevDayClose，通过字符串包含的形式，需要注意大小写(因为close有两个字段)
     * @param isDelay          true:修正delaySummary，false:修正summary
     */
    private void fixSymbolSummary(String symbol, String monthDay, String parameterFrom, String parameterColumns, boolean isDelay) {
        /**
         * 第一步，确定一下修正使用的数据
         */
        RedisSummary fromSummary = null;
        if (StringUtils.isNotBlank(parameterFrom) && parameterFrom.toLowerCase().equals("candle")) { // 通过【candle】数据生成Summary
            Map<String, String> barMap = RedisHelper.getPriceRedis().hashs().hgetAll(RedisKeyGenerator.getTimeTrendKey(symbol, monthDay));

            if (MapUtils.isNotEmpty(barMap)) {
                String firstKey = null;
                String lastKey = null;
                Integer first = null;
                Integer last = null;
                BigDecimal highPrice = null;
                BigDecimal lowPrice = null;

                for (Map.Entry<String, String> entry : barMap.entrySet()) { // 遍历，计算出当日第一次有数的点、最后一次有数的点、最高价、最低价
                    int time = Integer.parseInt(entry.getKey());
                    if (first == null || first.compareTo(time) > 0) {
                        first = time;
                        firstKey = entry.getKey();
                    }
                    if (last == null || last.compareTo(time) < 0) {
                        last = time;
                        lastKey = entry.getKey();
                    }
                    JimuStockBar bar = JsonUtils.fromJson(entry.getValue(), JimuStockBar.class);
                    if (highPrice == null || bar.getHigh().compareTo(highPrice) > 0) {
                        highPrice = bar.getHigh();
                    }
                    if (lowPrice == null || bar.getLow().compareTo(lowPrice) < 0) {
                        lowPrice = bar.getLow();
                    }
                }
                JimuStockBar firstBar = JsonUtils.fromJson(barMap.get(firstKey), JimuStockBar.class); // 得到第一个有数的点
                JimuStockBar lastBar = JsonUtils.fromJson(barMap.get(lastKey), JimuStockBar.class);   // 得到最后一个有数的点

                // 适配出一个summary
                fromSummary = new RedisSummary();
                fromSummary.setSymbol(symbol);
                fromSummary.setDayClose(lastBar.getClose());
                fromSummary.setDayOpen(firstBar.getOpen());
                fromSummary.setDayHigh(highPrice);
                fromSummary.setDayLow(lowPrice);
                fromSummary.setPrevDayClose(firstBar.getPreClose());
            }
        } else {
            String fromSummaryJson;
            if (isDelay) { // 要修正的是延迟数据， 从【实时Summary】取数据
                fromSummaryJson = RedisHelper.getPriceRedis().getValue(RedisKeyGenerator.getDxFeedSummary(symbol));
            } else {      // 要修正的是实时数据， 从【延迟Summary】取数据修正
                fromSummaryJson = RedisHelper.getPriceRedis().getValue(RedisKeyGenerator.getDxFeedDelaySummary(symbol));
            }
            fromSummary = JsonUtils.fromJson(fromSummaryJson, RedisSummary.class);
        }

        /**
         * 第二步，确定要被修正的数据
         */
        RedisSummary toSummary = null;
        String toSummaryJson;
        if (isDelay) { // 要修正的是【延迟数据】
            toSummaryJson = RedisHelper.getPriceRedis().getValue(RedisKeyGenerator.getDxFeedDelaySummary(symbol));
        } else {      // 要修正的是【实时数据】
            toSummaryJson = RedisHelper.getPriceRedis().getValue(RedisKeyGenerator.getDxFeedSummary(symbol));
        }

        toSummary = JsonUtils.fromJson(toSummaryJson, RedisSummary.class);
        if (fromSummary == null || toSummary == null) {
            log.warn("cannot fix summary, symbol:{}, fromSummary:{}, toSummary:{}", symbol, fromSummary, toSummary);
            return;
        }

        /**
         * 第三步，修正相关字段
         */
        if ((StringUtils.isBlank(parameterColumns) || "all".equals(parameterColumns))                       // 如果要修正的是全部字段，而且不是使用candle来修正的，那么就简单点，直接替换掉
                && (StringUtils.isBlank(parameterFrom) || !parameterFrom.toLowerCase().equals("candle"))) { // candle的话不能对所有的summary字段赋值，挨个赋值
            toSummary = fromSummary;
        } else {
            if (parameterColumns.contains("dayClose")) {
                toSummary.setDayClose(fromSummary.getDayClose());
            }
            if (parameterColumns.contains("dayOpen")) {
                toSummary.setDayOpen(fromSummary.getDayOpen());
            }
            if (parameterColumns.contains("dayHigh")) {
                toSummary.setDayHigh(fromSummary.getDayHigh());
            }
            if (parameterColumns.contains("dayLow")) {
                toSummary.setDayLow(fromSummary.getDayLow());
            }
            if (parameterColumns.contains("prevDayClose")) {
                toSummary.setPrevDayClose(fromSummary.getPrevDayClose());
            }
        }

        /**
         * 第四步，更新redis
         */
        if (WebConfig.isJimuboxDebugMode()) {
            log.info("symbol:{}, fromSummary:{}, toSummary:{}", symbol, fromSummary, toSummary);
        }
        RedisHelper.getPriceRedis().setValue(symbol, JsonUtils.toJson(toSummary));
    }

    private String getLastUsTradingMMdd() {
        TradingSession tradingSession = scheduleService.getLastTradingSession(new Date());
        Date lastTradingStart = new Date(tradingSession.getStart());
        return JodaTimeUtil.formatMD(lastTradingStart, JodaTimeUtil.AmericaZone);
    }
}
```