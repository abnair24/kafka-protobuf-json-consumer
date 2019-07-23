import com.github.abnair24.consumer.KafkaProcessor;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class TestClass {

    public static void main(String[] args) {
        List<String> topics = new ArrayList<>();
        List<JsonObject> list = new ArrayList<>();

        topics.add("completed_godeals_voucher_purchases");
        JsonObject jsonObject = null;
        try{
             jsonObject = new KafkaProcessor("10.14.3.7:6667",
                    "group-test-automation-2",
                    topics,
                    "/Users/aswathyn/Personal/Docs/Java-WS/kafka-protobuf-json-consumer/src/main/resources",
                    "com.gopoints.voucher.order_service.transactions.CompletedVoucherPurchaseEvent",
                    "173100693530020729").init();

             // RUNING THE TEST CASES HERE
            //

        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
        System.out.println(jsonObject.toString());

//        ProtobufConsumer protobufConsumer = new ProtobufConsumer("10.14.3.7:6667",
//                "group-test-automation-5",
//                topics,
//                "/Users/aswathyn/Personal/Docs/Java-WS/kafka-protobuf-json-consumer/src/main/resources",
//                "com.gopoints.voucher.order_service.transactions.CompletedVoucherPurchaseEvent");
//
//        try {
//            list = protobufConsumer.init(2);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        for(JsonObject j : list) {
//            System.out.println(j.toString());
//           if(j.has("173100693530020729")){
//
//           }
//        }


    }
}
