自定义权限

```
@Service
public class FilterChainDefinitionsService implements IFilterChainDefinitionsService {
    @Autowired
    private ShiroPermissionFactory permissFactory;

    @Override
    public void reloadFilterChains() {
        synchronized (permissFactory) {   //强制同步，控制线程安全
            AbstractShiroFilter shiroFilter = null;

            try {
                shiroFilter = (AbstractShiroFilter) permissFactory.getObject();

                PathMatchingFilterChainResolver resolver = (PathMatchingFilterChainResolver) shiroFilter
                        .getFilterChainResolver();
                // 过滤管理器
                DefaultFilterChainManager manager = (DefaultFilterChainManager) resolver.getFilterChainManager();
                // 清除权限配置
                manager.getFilterChains().clear();
                permissFactory.getFilterChainDefinitionMap().clear();
                // 重新设置权限
                permissFactory.setFilterChainDefinitions(ShiroPermissionFactory.definition);//传入配置中的filterchains

                Map<String, String> chains = permissFactory.getFilterChainDefinitionMap();
                //重新生成过滤链
                if (!CollectionUtils.isEmpty(chains)) {
                    chains.forEach((url, definitionChains) -> {
                        manager.createChain(url, definitionChains.trim().replace(" ", ""));
                    });
                }
                // manager.addToChain("/discover/banner", "perms", "sssss");


            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
```