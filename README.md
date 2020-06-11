# opensearch
基于阿里云开放搜索

mvn clean install -e -U 强制刷新
mvn dependency:tree

mvn package -Dmaven.test.skip=true

使用mvn package -DskipTests 跳过单元测试，但是会继续编译

控制台日志变色
-Dlog4j.skipJansi=false 
