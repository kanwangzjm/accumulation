```
<?xml version="1.0" encoding="UTF-8"?>

<configuration scan="true"><!-- 这个是根配置文件，一定要有的
                            scan：
                                是当配置文件被修改后会被重新加载
                            scanPeriod:
                                设置监测配置文件是否有修改的时间间隔，如果没有给出时间单位，
                                默认单位是毫秒。当scan为true时，此属性生效。默认的时间间隔为1分钟。
                            debug：
                                当此属性设置为true时，将打印出logback内部日志信息，实时查看logback运行状态。
                                默认值为false。
                             -->
    <!-- 日志存放路径 
        下面的标签可以自己定义
        name：相当于Map的key
        value：就是map的value
        ${catalina.base}是tomcat的当前路径
        /logs：就是tomcat下的日志路径，
        /ehrlog：如果没有目录会默认创建
    -->
    <substitutionProperty name="logbase" value="${catalina.base}/logs/ehrLog/" />
    <!-- 时间戳：
            这个时间戳可以作为每日日志的名称 -->
    <timestamp key="bySecond" datePattern="yyyy-MM-dd"/>
    <!-- appender：
        name相当于一个名称
        class：确定要加载哪个类
        encoder：一定要加 encoder ，
        默认配置为PatternLayoutEncoder
        patter：必填
        ConsoleAppender：也明白是什么意思，就是输出在控制台上-->
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>
                %d{yyyy-MM-dd HH:mm:ss} [%thread] [%level] - %m%n
            </pattern>
        </encoder>
    </appender>
    <!-- 把日志存储 
        encoding：日志的编码
        file：指定当前生成的日志文件名称
        rollingPolicy：滚动策略
        FileNamePattern：移动文件最后的名称，跟file标签结合使用，
        比如file里面的内容是  1.txt
        那么，FileNamePattern里面写的是2.txt，那么最后文件名就为2.txt
        如果最后结尾是gz或者zip，那么，就会自动打成压缩包
        -->
    <appender name="logFile" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <!-- 编码 -->
        <Encoding>UTF-8</Encoding>
        <!-- 按照时间来 -->
        <file>${logbase}ehr.${bySecond}.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
        <FileNamePattern>${logbase}ehr.${label}.%d{yyyy-MM-dd}.log.zip</FileNamePattern>
        </rollingPolicy>
        <!-- 布局 -->
        <encoder>
             <!--格式化输出：%d表示日期，%thread表示线程名，%-5level：级别从左显示5个字符宽度%msg：日志消息，%n是换行符--> 
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n</pattern>
        </encoder>
        <append>false</append>

    </appender>
    <!-- logger：日志单位
            name：是你当前扫描的哪个包
            level：日志的级别
            additivity：是否要把当前的日志级别，给root根logger
            appender-ref：appender的引用
         -->
    <logger name="com.xxxx.controller" level="trace" additivity="true">
        <appender-ref ref="STDOUT"/>
        <appender-ref ref="logFile"/>
    </logger>
    <!-- 默认根节点是INFO级别的日志 -->
    <!-- root：logger的根节点，就这一个，默认名称就是root
        level：日志级别
        appender-ref：确定使用哪个appender
         -->
    <root level="info">
        <appender-ref ref="fileAppender"/>
        <appender-ref ref="STDOUT"/>
    </root>
    
    <!--日志异步到数据库 -->  
    <appender name="DB" class="ch.qos.logback.classic.db.DBAppender">
        <!--日志异步到数据库 --> 
        <connectionSource class="ch.qos.logback.core.db.DriverManagerConnectionSource">
           <!--连接池 --> 
           <dataSource class="com.mchange.v2.c3p0.ComboPooledDataSource">
              <driverClass>com.mysql.jdbc.Driver</driverClass>
              <url>jdbc:mysql://127.0.0.1:3306/databaseName</url>
              <user>root</user>
              <password>root</password>
            </dataSource>
        </connectionSource>
    </appender>
</configuration>
```