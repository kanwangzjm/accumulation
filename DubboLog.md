```
/META-INF/dubbo/com.alibaba.dubbo.rpc.Filter

accesslogConsumerFilter=xxx.AccessLogFilter$Consumer
accesslogProviderFilter=xxx.AccessLogFilter$Provider
```
```
AccessLogFilter.class:

public class AccessLogFilter {
    private static final Logger logger = LoggerFactory.getLogger(BBAEAccessLogFilter.class);
    private static final long nowTime = System.currentTimeMillis();
    private static final String LEVEL_ARG_NAME = "qloglevel";
    private static final int SIMPLE_LOG_LEVEL = 5;
    private static final int ARGS_LOG_LEVEL = 8;
    private static final int RETURN_FAILED_VALUE_LEVEL = 9;
    private static final int RETURN_VALUE_LEVEL = 10;
    private static final int DEFAULT_LOG_LEVEL = 8;
    private final AsyncLogger log;
    private final boolean isProvider;

    public BBAEAccessLogFilter(String logFileNamePrefix, boolean isProvider) {
        this.isProvider = isProvider;
        String home = System.getProperty("catalina.base");
        if(home == null) {
            home = "target";
        } else {
            home = home + "/logs";
        }

        try {
            home = (new File(home)).getCanonicalPath();
        } catch (IOException var5) {
            logger.debug("logger homepath failed: ", var5);
        }

        this.log = new AsyncLogger(home + "/" + logFileNamePrefix + ".", ".log", 10000, new TimeBasedRollingPattern("yyyy-MM-dd-HH"));
        logger.info("dubbo access logging in : " + logFileNamePrefix);
    }

    private Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        RuntimeException ex = null;
        Result result = null;
        int level = invoker.getUrl().getMethodParameter(inv.getMethodName(), "qloglevel", 8);
        String accessLog = this.recordAccess(inv, level);
        String args = this.recordArgs(inv, level);
        long start = System.currentTimeMillis();

        try {
            result = invoker.invoke(inv);
        } catch (RuntimeException var14) {
            ex = var14;
        } finally {
            this.outputLog(invoker, inv, ex, start, result, accessLog, args, level);
        }

        if(ex != null) {
            throw ex;
        } else {
            return result;
        }
    }

    private String recordArgs(Invocation inv, int level) {
        if(level < 8) {
            return "";
        } else {
            Object[] args = inv.getArguments();
            if(args != null && args.length > 0) {
                Object[] argsFormatted = new Object[args.length];

                for(int i = 0; i < args.length; ++i) {
                    Object o = args[i];
                    if(o != null && o instanceof Locale) {
                        try {
                            argsFormatted[i] = ((Locale)o).getLanguage();
                        } catch (Exception var8) {
                            argsFormatted[i] = "";
                        }
                    } else {
                        argsFormatted[i] = o;
                    }
                }

                return JsonMapper.obj2String(args);
            } else {
                return "";
            }
        }
    }

    private String recordAccess(Invocation inv, int level) {
        if(level < 5) {
            return "";
        } else {
            RpcContext context = RpcContext.getContext();
            String consumer;
            int consumerPort;
            String provider;
            int providerPort;
            if(!this.isProvider) {
                consumer = context.getLocalHost();
                consumerPort = context.getLocalPort();
                provider = context.getRemoteHost();
                providerPort = context.getRemotePort();
            } else {
                consumer = context.getRemoteHost();
                consumerPort = context.getRemotePort();
                provider = context.getLocalHost();
                providerPort = context.getLocalPort();
            }

            return consumer + ":" + consumerPort + " -> " + provider + ":" + providerPort;
        }
    }

    private void outputLog(Invoker<?> invoker, Invocation inv, RuntimeException ex, long start, Result returnValue, String accessLog, String args, int level) {
        long elapsed = System.currentTimeMillis() - start;

        try {
            StringBuilder log = new StringBuilder(100);
            this.appendSimpleLog(log, invoker, inv, ex, elapsed, Integer.valueOf(level), accessLog);
            this.appendArgs(log, args, Integer.valueOf(level));
            if(ex == null) {
                this.appendReturnValue(log, returnValue, Integer.valueOf(level));
            }

            if(log.length() == 0) {
                return;
            }

            String writeLog = log.toString();
            this.log.log(writeLog);
        } catch (Throwable var14) {
            logger.warn("Exception in AcessLogFilter of service(" + invoker + " -> " + inv + ")", var14);
        }

    }

    private void appendReturnValue(StringBuilder result, Result returnValue, Integer level) {
        if(level.intValue() >= 9) {
            if(returnValue != null) {
                if(returnValue.hasException()) {
                    result.append("FAILED(").append(returnValue.getException().getMessage()).append(") ");
                }

                if(level.intValue() >= 10) {
                    if(!returnValue.hasException()) {
                        result.append("\n Return value(").append(JsonMapper.obj2String(returnValue.getValue())).append(") ");
                    }

                }
            }
        }
    }

    private void appendArgs(StringBuilder result, String args, Integer level) {
        if(level.intValue() >= 8) {
            result.append(args);
        }
    }

    private void appendSimpleLog(StringBuilder log, Invoker<?> invoker, Invocation inv, RuntimeException ex, long elapsed, Integer level, String accessLog) {
        if(level.intValue() >= 5) {
            String serviceName = invoker.getInterface().getName();
            String version = invoker.getUrl().getParameter("version");
            String group = invoker.getUrl().getParameter("group");
            String consumerApp = inv.getAttachment("consumer");
            log.append(elapsed).append(" [").append(consumerApp).append("] ").append(accessLog);
            if(null != group && group.length() > 0) {
                log.append(group).append("/");
            }

            log.append(serviceName);
            if(null != version && version.length() > 0) {
                log.append(":").append(version);
            }

            log.append(" ");
            log.append(inv.getMethodName());
            log.append("(");
            Class<?>[] types = inv.getParameterTypes();
            if(types != null && types.length > 0) {
                boolean first = true;
                Class[] var15 = types;
                int var16 = types.length;

                for(int var17 = 0; var17 < var16; ++var17) {
                    Class<?> type = var15[var17];
                    if(first) {
                        first = false;
                    } else {
                        log.append(",");
                    }

                    log.append(type.getName());
                }
            }

            log.append(") ");
            if(ex != null) {
                log.append("FAILED(").append(ex.getMessage()).append(") ");
                logger.warn("service invoke failed! \n" + log, ex);
            } else {
                log.append("DONE ");
            }

        }
    }

    @Activate(
        group = {"provider"}
    )
    public static class Provider implements Filter {
        private String path;
        private final BBAEAccessLogFilter logFilter;

        public Provider() {
            this.logFilter = new BBAEAccessLogFilter("dubbo-access-provider." + BBAEAccessLogFilter.nowTime, true);
        }

        public Provider(String path) {
            this.logFilter = new BBAEAccessLogFilter(path, true);
        }

        public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
            return this.logFilter.invoke(invoker, invocation);
        }

        public String getPath() {
            return this.path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

    @Activate(
        group = {"consumer"}
    )
    public static class Consumer implements Filter {
        private String path;
        private final BBAEAccessLogFilter logFilter;

        public Consumer() {
            this.logFilter = new BBAEAccessLogFilter("dubbo-access-consumer." + BBAEAccessLogFilter.nowTime, false);
        }

        public Consumer(String path) {
            this.logFilter = new BBAEAccessLogFilter(path, false);
        }

        public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
            return this.logFilter.invoke(invoker, invocation);
        }

        public String getPath() {
            return this.path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }
}
```
```
consumer:

<dubbo:consumer filter="accesslogConsumerFilter">
    <dubbo:parameter key="qaccesslog" value="9"/>
    <dubbo:parameter key="qloglevel" value="8"/>
</dubbo:consumer>

provider:
<dubbo:provider filter="accesslogProviderFilter">
    <dubbo:parameter key="qaccesslog" value="9"/>
    <dubbo:parameter key="qloglevel" value="8"/>
</dubbo:provider>
```