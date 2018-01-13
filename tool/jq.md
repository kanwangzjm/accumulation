#依赖包

```
<dependency>
    <groupId>org.jsoup</groupId>
    <artifactId>jsoup</artifactId>
    <version>1.9.2</version>
</dependency>
<dependency>
    <groupId>net.thisptr</groupId>
    <artifactId>jackson-jq</artifactId>
    <version>0.0.8</version>
</dependency>
```

#使用

```
String url = "https://finance.yahoo.com/quote/" + symbol + "/holdings?p=" + symbol;
try {
    String body = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows; U; WindowsNT 5.1; en-US; rv1.8.1.6) Gecko/20070725 Firefox/2.0.0.6")
            .referrer("http://www.google.com").execute().body();
    String pattern = "root.App.main = (.*);\\R";
    Pattern pat = Pattern.compile(pattern);
    Matcher matcher = pat.matcher(body);
    if (matcher.find()) {
        String json = matcher.group(1);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        JsonQuery jq = JsonQuery.compile(".context.dispatcher.stores.QuoteSummaryStore.topHoldings");
        JsonNode in = mapper.readTree(json);
        List<JsonNode> nodes = jq.apply(in);
        for (JsonNode topHoldingNode : nodes) {
            List<JsonNode> holdingsList = topHoldingNode.findValues("holdings");
            if (CollectionUtils.isNotEmpty(holdingsList)) {
                JsonNode holdings = holdingsList.get(0);
                List<Holding> holdingList = JsonUtils.getList(holdings.toString(), Holding.class); // 这里转化一次是为了保证存入redis的结构是符合要求的
                symbolHoldingMap.put(symbol, JsonUtils.toJson(holdingList));
            }
        }
    }
} catch (Exception e) {
    log.error("visit " + url + " exception", e);
}
```