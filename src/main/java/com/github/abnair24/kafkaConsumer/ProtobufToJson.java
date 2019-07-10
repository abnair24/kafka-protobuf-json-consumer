package com.github.abnair24.kafkaConsumer;

import com.github.abnair24.jsonFormatter.JsonFormater;
import com.google.gson.JsonObject;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ProtobufToJson {

    private final Descriptors.Descriptor descriptor;

    public ProtobufToJson(Descriptors.Descriptor descriptor) {
        this.descriptor = descriptor;
    }

    public JsonObject protobufToJsonObject(byte[] inputData) {

        DynamicMessage dynamicMessage = null;
        try {
            dynamicMessage = DynamicMessage.parseFrom(descriptor,inputData);
        } catch (InvalidProtocolBufferException e) {
            log.error("Dynamic message parsing failed: {}",e.getMessage());
        }
        return JsonFormater.toJsonObject(dynamicMessage);
    }

//    public static JsonObject protobufToJson(String protoPath,String fullMethod,byte[] inputData) throws Exception {
//
//        ProtoDetail protoDetail = new ProtoDetail(protoPath,fullMethod);
//
//        Path path = ProtoCache.getBinary(protoDetail);
//
//        Descriptors.Descriptor methodDescriptor = ProtoBufDecoder.getDescriptor(protoDetail,path.toAbsolutePath().toString());
//
//        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(methodDescriptor,
//                inputData);
//
//        return JsonFormater.toJsonObject(dynamicMessage);
//    }

//    public static String protobufToJsonString(String protoPath,String fullMethod,byte[] inputData) throws Exception {
//
//        ProtoDetail protoDetail = new ProtoDetail(protoPath,fullMethod);
//
//        Path path = ProtoCache.getBinary(protoDetail);
//
//        Descriptors.Descriptor methodDescriptor = ProtoBufDecoder.getDescriptor(protoDetail,path.toAbsolutePath().toString());
//
//        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(methodDescriptor,inputData);
//
//        return JsonFormater.toJson(dynamicMessage);
//    }
}
