package com.github.abnair24.kafkaConsumer;

import com.github.abnair24.jsonFormatter.JsonFormater;
import com.github.abnair24.util.ProtoBufDecoder;
import com.github.abnair24.util.ProtoDetail;
import com.github.abnair24.util.ProtoUtility;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.nio.file.Path;

public class ProtobufJsonConsumer {

    public static JsonObject protobufToJson(String protoPath,String fullMethod,byte[] inputData) throws Exception {

        ProtoDetail protoDetail = new ProtoDetail(protoPath,fullMethod);

        Path path = ProtoUtility.getDescriptorBinary(protoDetail);

        Descriptors.MethodDescriptor methodDescriptor = ProtoBufDecoder.getMethodDescriptor(protoDetail,path);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(methodDescriptor.getOutputType(),
                inputData);

        return JsonFormater.toJsonObject(dynamicMessage);
    }

    public static String protobufToJsonString(String protoPath,String fullMethod,byte[] inputData) throws Exception {

        ProtoDetail protoDetail = new ProtoDetail(protoPath,fullMethod);

        Path path = ProtoUtility.getDescriptorBinary(protoDetail);

        Descriptors.MethodDescriptor methodDescriptor = ProtoBufDecoder.getMethodDescriptor(protoDetail,path);

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(methodDescriptor.getOutputType(),inputData);

        return JsonFormater.toJson(dynamicMessage);
    }
}
