#依赖包

```
<dependency>
    <groupId>rome</groupId>
    <artifactId>rome</artifactId>
    <version>1.0</version>
</dependency>
```

#使用

```
private final static String RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=&dateb=&owner=exclude&count=&output=atom";


URL feedUrl = new URL(String.format(RSS_URL, symbol));
SyndFeedInput input = new SyndFeedInput();
SyndFeed feed = input.build(new XmlReader(feedUrl));
if (feed == null) {
    log.warn("syndFeed is null, symbol:{}", symbol);
    return;
}
List<SyndEntry> list = feed.getEntries();
for (SyndEntry entry : list) {
    System.out.println(entry.getTitle() + "-"+ entry.getUpdatedDate() + "-" + entry.getLink());
}
```