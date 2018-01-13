```
@Override
public List<JimuStockBar> getCurrentTimeTrend(String symbol, String exchangeCode, Integer period, boolean isRealTime) throws Exception {
    List<JimuStockBar> barList = Lists.newArrayList();

    try {
        Symbol currentSymbol = symbolService.selectBySymbol(symbol);
        if (currentSymbol == null || SymbolStatus.INVALID.getValue().equals(currentSymbol.getStatus())) {  // 股票或不存在，或者已经下线，那么直接返回
            return barList;
        }
    } catch (Exception e) {
        log.error("selectBySymbol exception, symbol:" + symbol, e);
    }

    Date currentDate = new Date();
    // 最近的开市时间: 当前开市,则为当日开市时间;当前闭市,上一个交易日的开市时间
    TradingSession tradingSession = scheduleService.getLastTradingSession(currentDate);
    long lastTradingStart = tradingSession.getStart();

    if (isRealTime || period > 1 || !scheduleService.isTradingDay(currentDate)) { //redis缓存的是全量数据,延时分时线的慢慢算, 如果闭市了, 则考虑使用缓存数据
        String redisData = RedisHelper.getPriceRedis().getValue(RedisKeyGenerator.getCurrentTimeTrendKey(symbol, period));
        if (StringUtils.isNotBlank(redisData)) {
            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_TIMETREND_HIT_CACHE);
            return JsonUtils.getList(redisData, JimuStockBar.class);
        }
    }

    JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_TIMETREND_MISS_CACHE);
    String monthDay = getLastUsTradingMMdd(); // 例如 0908， 用于计算redis的key
    long start = System.currentTimeMillis();

    List<String> hhmmList = getUSTradingDayTimeList(currentDate, isRealTime, period,
            symbol); // 例如：0930,0931,0932， redis中hashes里的key，通过key去redis中取值，遍历生成最终数据

    if (CollectionUtils.isEmpty(hhmmList)) {
        return barList;
    }

    Map<String, String> barMap = RedisHelper.getPriceRedis().hashs().hgetAll(RedisKeyGenerator.getTimeTrendKey(symbol, monthDay));
    JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_DATA_READ_REDIS, System.currentTimeMillis() - start);

    JimuStockBar lastBar = null;
    Date lastEndDate = null;
    BigDecimal preClose = calcPreClose(symbol);

    if (MapUtils.isEmpty(barMap)) {
        // redis中无任何数据
        TimeTrend timeTrend = getLastTradingDayTimeTrend(symbol);
        if (timeTrend == null) { // 新增的symbol会没有历史timeTrend
            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_NO_REDIS_AND_NO_DB_DATA);
            log.warn("dxfeed_no_redis_and_no_db_data, symbol:{}", symbol);
        }
        lastEndDate = new Date(lastTradingStart);
        if (timeTrend == null) {
            timeTrend = adaptTimeTrendWithIPO(findIPO(symbol), lastEndDate);
            if (timeTrend == null) {
                return barList;
            } else {
                JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_TIMETREND_USE_IPO_PRICE);
            }
        }
        timeTrend.setVolume(BigDecimal.ZERO);
        if (preClose != null) {
            timeTrend.setPreviousclose(preClose);
        }
        for (int i = 0; i < hhmmList.size(); i++) {
            timeTrend.setEndtime(lastEndDate);
            JimuStockBar curBar = TimeTrendUtils.fromTimeTrendToJMStockBar(timeTrend);
            if (indexSet.contains(symbol)) {
                curBar.setVWAP(null);
            }
            barList.add(curBar);
            lastEndDate = DateUtils.addMinutes(lastEndDate, period);
        }
        saveCurrentTimeTrendToRedis(symbol, isRealTime, period, barList);
        return barList;
    }

    Map<String, JimuStockBar> jimuStockBarMap = Maps.newHashMap();
    nanoStart = System.nanoTime();
    for (String hhmm : hhmmList) {
        if (barMap.containsKey(hhmm)) {
            jimuStockBarMap.put(hhmm, JsonUtils.fromJson(barMap.get(hhmm), JimuStockBar.class));
        }
    }
    
    // 初始化第一条数据, 没有取出最近一条代替
    if (!jimuStockBarMap.containsKey(hhmmList.get(0))) {
        TimeTrend timeTrend = getLastTradingDayTimeTrend(symbol);
        if (timeTrend == null) { // 没有历史数据
            JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_NO_TIMETREND_DATA);
            log.warn("no_timeTrend_in_db, symbol:{}", symbol);
        }
        lastEndDate = new Date(lastTradingStart);
        if (timeTrend == null) {
            timeTrend = adaptTimeTrendWithIPO(findIPO(symbol), lastEndDate);
            if (timeTrend == null) {
                return barList;
            } else {
                JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_TIMETREND_USE_IPO_PRICE);
            }
        }
        timeTrend.setVolume(BigDecimal.ZERO);
        timeTrend.setVwap(timeTrend.getVwap());
        timeTrend.setEndtime(lastEndDate);
        if (preClose != null) {
            timeTrend.setPreviousclose(preClose);
        }
        lastBar = TimeTrendUtils.fromTimeTrendToJMStockBar(timeTrend);
    } else {
        lastBar = jimuStockBarMap.get(hhmmList.get(0));
        lastEndDate = JodaTimeUtil.parseWithZone("MM/dd/yyyy hh:mm:ss aa", lastBar.getEndDate() + " " + lastBar.getEndTime(), JodaTimeUtil.BeijingZone)
                .toDate();
    }

    // 遍历时间, 没有数据的点做补全
    BigDecimal totalAmount = BigDecimal.ZERO;
    BigDecimal totalVolume = BigDecimal.ZERO;

    long nanoExchangeStart = System.nanoTime();
    for (String hhmm : hhmmList) {
        if (jimuStockBarMap.containsKey(hhmm)) {
            lastBar = jimuStockBarMap.get(hhmm);
            if (!indexSet.contains(symbol)) {
                totalAmount = totalAmount.add(lastBar.getVWAP().multiply(lastBar.getVolume()));
                totalVolume = totalVolume.add(lastBar.getVolume());
                if (totalAmount.compareTo(BigDecimal.ZERO) > 0 && totalVolume.compareTo(BigDecimal.ZERO) > 0) {
                    lastBar.setVWAP(totalAmount.divide(totalVolume, 4, BigDecimal.ROUND_HALF_UP));
                }
            }
        } else { // 当前分钟没值时，使用上一分钟的值代替
            Date startDate = DateUtils.addMinutes(lastEndDate, -1);
            lastBar.setStartDate(JodaTimeUtil.formatMDY(startDate, JodaTimeUtil.BeijingZone));
            lastBar.setStartTime(JodaTimeUtil.formatHMSA(startDate, JodaTimeUtil.BeijingZone));
            lastBar.setEndDate(JodaTimeUtil.formatMDY(lastEndDate, JodaTimeUtil.BeijingZone));
            lastBar.setEndTime(JodaTimeUtil.formatHMSA(lastEndDate, JodaTimeUtil.BeijingZone));
            lastBar.setVolume(BigDecimal.ZERO); // 补充的点的volume为0,代表无交易
        }

        lastEndDate = DateUtils.addMinutes(lastEndDate, period);
        if (preClose != null) {
            lastBar.setPreClose(preClose);
            lastBar.setChangeFromPreClose(lastBar.getClose().subtract(lastBar.getPreClose()));
            lastBar.setPercentChangeFromPreClose(lastBar.getClose().subtract(lastBar.getPreClose()).multiply(new BigDecimal(100))
                    .divide(lastBar.getPreClose(), 2, BigDecimal.ROUND_HALF_UP));
        }
        JimuStockBar curBar = copy(lastBar);
        if (indexSet.contains(symbol)) { // 这一步特别关键,指数的vwap需要为null
            curBar.setVWAP(null);
        }
        barList.add(curBar);
    }
    JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_DATA_EXCHANGE_TIMETREND, System.currentTimeMillis() - start);
    saveCurrentTimeTrendToRedis(symbol, isRealTime, period, barList);
    return barList;
}

private void saveCurrentTimeTrendToRedis(String symbol, boolean isRealTime, int period, List<JimuStockBar> barList) {
    if (!isRealTime || CollectionUtils.isEmpty(barList)) {
        return;
    }
    int cacheTime = calCurrentTimeTrendCacheSeconds();
    long start = System.currentTimeMillis();
    RedisHelper.getPriceRedis().setValue(RedisKeyGenerator.getCurrentTimeTrendKey(symbol, period), JsonUtils.toJson(barList), cacheTime);
    JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_CANDLE_DATA_SAVE_REDIS, System.currentTimeMillis() - start);
}

private int calCurrentTimeTrendCacheSeconds() {
    Date date = new Date();
    if (scheduleService.isTradingDay(date)) { // 交易期间,只缓存到下一分钟的第10s
        String hhmm = JodaTimeUtil.formatHM(date, JodaTimeUtil.AmericaZone);
        int current = Integer.parseInt(hhmm);
        // 美股开始时间为9:30~16:00, 订阅历史数据在16:30开始执行,1.5小时可以保证执行完
        if (current >= 930 && current <= 1845) {
            int seconds = (int) org.apache.commons.lang3.time.DateUtils.getFragmentInSeconds(date, Calendar.MINUTE);
            return 70 - seconds;
        }
    }
    try {
        // 如果不在开始期间,那么分时线数据缓存到下个交易日开始
        return (int) (scheduleService.getNextTradingSession(date).getStart() - date.getTime()) / 1000 + 10;
    } catch (Throwable t) {
        log.error("scheduleService.getNextTradingSession exception", t);
        return DEFAULT_EXPIRE_SECONDS;
    }
}
```

```
@Override
public List<JimuStockBar> getHis5TimeTrend(String symbol, String exchangeCode, Integer period, boolean isRealTime) throws Exception {
    List<JimuStockBar> barList = Lists.newArrayList();

    try {
        Symbol currentSymbol = symbolService.selectBySymbol(symbol);
        if (currentSymbol == null || SymbolStatus.INVALID.getValue().equals(currentSymbol.getStatus())) { // 股票或不存在，或者已经下线，那么直接返回
            return barList;
        }
    } catch (Exception e) {
        log.error("selectBySymbol exception, symbol:" + symbol, e);
    }

    Date currentDate = new Date();

    String barJson = RedisHelper.getPriceRedis().getValue(RedisKeyGenerator.getHit5Key(symbol, period));
    JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_HIT5_READ_REDIS, System.currentTimeMillis() - start);

    if (StringUtils.isNotBlank(barJson)) {
        JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_HIT5_HIT_CACHE);
        barList.addAll(JsonUtils.getList(barJson, JimuStockBar.class));
    } else {
        JMonitor.recordOne(MonitorConstant.DXFEED.DXFEED_HIT5_MISS_CACHE);
        // 计算非分时线外的4个交易日,由近及远
        List<TradingSession> sessionList = Lists.newArrayList();
        TradingSession tradingSession = scheduleService.getLastTradingSession(currentDate);

        String ipoDateStr = "";
        IpoInfo ipoInfo = findIPO(symbol);
        if (ipoInfo != null) {
            ipoDateStr = JodaTimeUtil.format(ipoInfo.getIpoDate(), YMD_FORMATTER);
        }
        boolean useIPOSetting = false;
        for (int i = 0; i < 4; i++) {
            TradingSession curSession = scheduleService.getLastTradingSession(new Date(tradingSession.getStart() - ONE_MINUTE_MILLSECONDS));
            sessionList.add(new TradingSession(curSession.getStart(), curSession.getEnd(), true));
            tradingSession = curSession;

            // 判断是否是ipo日期
            if (StringUtils.isNotBlank(ipoDateStr)) {
                Date tradingDate = new Date(curSession.getStart());
                String tradingDateStr = JodaTimeUtil.format(tradingDate, YMD_FORMATTER);
                if (ipoDateStr.equals(tradingDateStr)) { // 如果是ipo日期，则不需要继续查之前的日期了
                    log.info("his5 meets ipo setting, symbol:{}, no need get data for more date, end:{}", symbol, ipoDateStr);
                    useIPOSetting = true;
                    break;
                }
            }
        }

        // 得到历史timeTrend的起止时间
        Date startDate = new Date(sessionList.get(sessionList.size() - 1).getStart());
        Date endDate = new Date(sessionList.get(0).getEnd());
        // 获得历史timeTrend
        String instrumentId = symbolService.selectInstrumentIdBySymbol(symbol);
        if (StringUtils.isBlank(instrumentId)) {
            return barList;
        }

        long searchDatabaseStart = System.currentTimeMillis();
        List<TimeTrend> tmpList = timeTrendMapper.getTimeTrends(instrumentId, startDate, endDate);
        if (useIPOSetting) {
            TimeTrend timeTrend = adaptTimeTrendWithIPO(ipoInfo, startDate);
            if (timeTrend != null) {
                log.info("use ipo setting for his5, symbol:{}", symbol);
                tmpList.add(timeTrend);
            }
        }
        Collections.sort(tmpList, new Comparator<TimeTrend>() {
            @Override
            public int compare(TimeTrend o1, TimeTrend o2) {
                return o1.getEndtime().compareTo(o2.getEndtime());
            }
        });
        // timeTrend补全
        tmpList = repairTimeTrend(symbol, tmpList, sessionList, period);

        // 转化为前端使用的格式
        BigDecimal firstPreClose = (CollectionUtils.isEmpty(tmpList)) ? BigDecimal.ZERO : tmpList.get(0).getPreviousclose();
        List<JimuStockBar> historyList = tmpList.stream().map(timeTrend -> adaptToJMStockBarForHis5(timeTrend, firstPreClose)).collect(Collectors.toList());

        // 将历史数据写入redis
        RedisHelper.getPriceRedis().setValue(RedisKeyGenerator.getHit5Key(symbol, period), JsonUtils.toJson(historyList), calHistoryHit5CacheSeconds());

        barList.addAll(historyList);
    }

    List<JimuStockBar> lastTimeTrends = getCurrentTimeTrend(symbol, exchangeCode, period, isRealTime);

    if (CollectionUtils.isNotEmpty(lastTimeTrends)) {
        if (CollectionUtils.isNotEmpty(barList)) { // 修改今日昨收价为5日线第一个点的昨收价，并同时计算与昨收价有关的变量的值
            BigDecimal firstPreClose = (CollectionUtils.isEmpty(barList)) ? BigDecimal.ZERO : barList.get(0).getPreClose();
            for (JimuStockBar jimuStockBar : lastTimeTrends) {
                jimuStockBar.setPreClose(firstPreClose);
                jimuStockBar.setChangeFromPreClose(jimuStockBar.getClose().subtract(firstPreClose));
                if (firstPreClose.compareTo(BigDecimal.ZERO) == 0) {
                    jimuStockBar.setPercentChangeFromPreClose(BigDecimal.ZERO);
                } else {
                    jimuStockBar.setPercentChangeFromPreClose(jimuStockBar.getClose().subtract(firstPreClose).multiply(new BigDecimal(100)).divide(firstPreClose, 2, BigDecimal.ROUND_HALF_UP));
                }
            }
        }
        barList.addAll(lastTimeTrends);
    }
    return barList;
}

/**
 * 补全timeTrend
 *
 * @param symbol              股票
 * @param originTimeTrendList 必须按递增顺序给出
 * @param sessionList         需要处理的交易日,逆序给出
 */
private List<TimeTrend> repairTimeTrend(String symbol, List<TimeTrend> originTimeTrendList, List<TradingSession> sessionList, int period) {
    List<TimeTrend> timeTrendList = Lists.newArrayList();
    String instrumentId = symbolService.selectInstrumentIdBySymbol(symbol);
    if (StringUtils.isBlank(instrumentId)) {
        return timeTrendList;
    }
    TimeTrend lastTimeTrend = timeTrendMapper.getLastOneBySymbol(instrumentId, new Date(sessionList.get(sessionList.size() - 1).getStart()));
    if (lastTimeTrend == null) {
        if (CollectionUtils.isEmpty(originTimeTrendList)) { // 没有任何记录,也找不到第一个点,不需要计算
            return timeTrendList;
        }
        lastTimeTrend = originTimeTrendList.get(0);
        lastTimeTrend.setVwap(lastTimeTrend.getClose());
    } else {
        lastTimeTrend.setVwap(lastTimeTrend.getClose());
    }
    if (indexSet.contains(symbol) && lastTimeTrend != null) {
        lastTimeTrend.setVwap(null);
    }
    int index = 0;
    for (int i = sessionList.size() - 1; i >= 0; i--) { // 遍历交易日(交易日本身是逆序的)
        long start = sessionList.get(i).getStart();
        long end = sessionList.get(i).getEnd();
        BigDecimal totalAmount = BigDecimal.ZERO;
        BigDecimal totalVolume = BigDecimal.ZERO;
        while (start <= end) { // 处理每个交易日的开始时间
            TimeTrend cur = index >= originTimeTrendList.size() ? null : originTimeTrendList.get(index);
            while (cur != null && cur.getEndtime().getTime() < start) {
                index++;
                cur = index >= originTimeTrendList.size() ? null : originTimeTrendList.get(index);
            }
            if (cur != null && cur.getEndtime().getTime() - start <= 1000) { // 小于1s时认为数据是ok的
                if (indexSet.contains(symbol)) {
                    cur.setVwap(null);
                } else {
                    totalAmount = totalAmount.add(cur.getVwap().multiply(cur.getVolume()));
                    totalVolume = totalVolume.add(cur.getVolume());
                    if (totalAmount.compareTo(BigDecimal.ZERO) > 0 && totalVolume.compareTo(BigDecimal.ZERO) > 0) {
                        cur.setVwap(totalAmount.divide(totalVolume, 4, BigDecimal.ROUND_HALF_UP));
                    } else {
                        cur.setVwap(lastTimeTrend.getVwap());
                    }
                }
                timeTrendList.add(cur);
                lastTimeTrend = cur;
                index++;
            } else {
                TimeTrend timeTrend = copy(lastTimeTrend);
                timeTrend.setEndtime(new Date(start));
                timeTrend.setVolume(BigDecimal.ZERO);
                if (indexSet.contains(symbol)) {
                    timeTrend.setVwap(null);
                } else {
                    timeTrend.setVwap(lastTimeTrend.getVwap());
                }
                timeTrendList.add(timeTrend);
                lastTimeTrend = timeTrend;
            }
            start += ONE_MINUTE_MILLSECONDS * period; // 处理下一个节点
        }
    }
    return timeTrendList;
}
```