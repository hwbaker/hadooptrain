package com.imooc.hadoop.project;

import com.kumkee.userAgent.UserAgentParser;
import com.kumkee.userAgent.UserAgent;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;

/**
 * UserAgentTest测试类
 */
public class UserAgentTest {

    @Test
    public void testFileRead() throws Exception {
        String path = "/Users/hwbaker/SiteGit/testData/10000_access.log";

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(new File(path)))
        );

        String line = "";
        UserAgentParser userAgentParser  = new UserAgentParser();
        int i = 0;

        Map<String, Integer> browserMap = new HashMap<String, Integer>();

        while (line != null) {
            line = reader.readLine();
            i++;

            if (StringUtils.isNotBlank(line)) {

                String source = line.substring(getCharacterPosition(line, "\"", 7)) + 1;
                UserAgent agent = userAgentParser.parse(source);

                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String os = agent.getOs();
                String platform = agent.getPlatform();
                boolean ismobile = agent.isMobile();

                Integer browserVal = browserMap.get(browser);
                if (browserVal != null) {
                    browserMap.put(browser, browserVal + 1);
                } else {
                    browserMap.put(browser, 1);
                }

//                System.out.println(
//                        "browser=[" + browser +"], " +
//                                "engine=["+ engine + "], " +
//                                "engineVersion=["+ engineVersion + "], " +
//                                "os=["+ os + "], " +
//                                "platform=["+ platform + "], " +
//                                "ismobile=[" + ismobile+ "]"
//                );
            }
        }

        System.out.println("userAgentParser records sum: " + i);
        System.out.println("-------------------------------------");

        for (Map.Entry<String, Integer> entity: browserMap.entrySet()) {
            System.out.println(entity.getKey() + ':' + entity.getValue());
        }

    }

    /**
     * 测试自定义方法
     */
    @Test
    public void testGetCharacterPosition()
    {
//        String val = "113.100.63.27 - - [10/Nov/2016:00:01:02 +0800] \"POST /course/ajaxmediauser HTTP/1.1\" 200 54 \"www.imooc.com\" \"http://www.imooc.com/code/5515\" mid=5515&time=60 \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393\" \"-\" 10.100.136.65:80 200 0.022 0.022";
        String val = "111---\"\"\"\"ooo&&&,\"..\"\"mm";

        int index = getCharacterPosition(val, "\"", 7);
        System.out.println(index);
    }

    /**
     * 获取指定字符串中，指定字符标识出现的索引位置
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private int getCharacterPosition(String value, String operator, int index)
    {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int miDx = 0;
        while (slashMatcher.find()) {
            miDx++;

            if (miDx == index) {
                break;
            }
        }

        return slashMatcher.start();
    }

    /**
     * 单元测试：UserAgent工具类使用
     */
    @Test
    public void testUserAgentParser(){
//    public static void main(String[] args) {
        String source = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.1708.400 QQBrowser/9.5.9635.400";
//        String source = "mukewang/5.0.1 (Android 5.0.2; Xiaomi Redmi Note 2 Build/LRX22G),Network WIFI";

        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);

        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String os = agent.getOs();
        String platform = agent.getPlatform();
        boolean ismobile = agent.isMobile();

        System.out.println(
                "browser=[" + browser +"], " +
                "engine=["+ engine + "], " +
                "engineVersion=["+ engineVersion + "], " +
                "os=["+ os + "], " +
                "platform=["+ platform + "], " +
                "ismobile=[" + ismobile+ "]"
        );
//    }
    }

}
