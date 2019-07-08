package com.github.abnair24.kafkaConsumer;

import com.github.abnair24.jsonFormatter.JsonFormater;
import com.github.abnair24.util.ProtoBufDecoder;
import com.github.abnair24.util.ProtoDetail;
import com.github.abnair24.util.ProtoUtility;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class ProtobufToJson {

    private static final Logger logger = LoggerFactory.getLogger(ProtobufToJson.class);

    public static JsonObject protobufToJson(String protoPath,String fullMethod,byte[] inputData) throws Exception {

        ProtoDetail protoDetail = new ProtoDetail(protoPath,fullMethod);

        Path path = ProtoUtility.getDescriptorBinary(protoDetail);

        Descriptors.Descriptor methodDescriptor = ProtoBufDecoder.getDescriptor(protoDetail,path.toAbsolutePath().toString());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(methodDescriptor,
                inputData);

        return JsonFormater.toJsonObject(dynamicMessage);
    }

    public static String protobufToJsonString(String protoPath,String fullMethod,byte[] inputData) throws Exception {

        ProtoDetail protoDetail = new ProtoDetail(protoPath,fullMethod);

        Path path = ProtoUtility.getDescriptorBinary(protoDetail);

        Descriptors.Descriptor methodDescriptor = ProtoBufDecoder.getDescriptor(protoDetail,path.toAbsolutePath().toString());

        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(methodDescriptor,inputData);

        return JsonFormater.toJson(dynamicMessage);
    }
}
