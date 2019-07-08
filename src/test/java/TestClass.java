import com.github.abnair24.kafkaConsumer.Consumer;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class TestClass {

    public static void main(String[] args) {
        List<String> topics = new ArrayList<>();
        List<JsonObject> list = new ArrayList<>();

        topics.add("completed_godeals_voucher_purchases");

        Consumer consumer = new Consumer(""G,
                "group-test-automation-4",
                topics,
                "/Users/aswathyn/Documents/GoJek/codebase/gopoints-api-tests/src/test/resources",
                "");

        list = consumer.init(2);

        for(JsonObject j : list) {
            System.out.println(j.toString());
        }


    }
}
